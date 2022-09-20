/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
import { blake3 } from '@napi-rs/blake-hash'
import LeastRecentlyUsed from 'blru'
import { HighLevelProducer as Producer, KafkaClient } from 'kafka-node'
import { Assert } from '../assert'
import { Config } from '../fileStores/config'
import { Logger } from '../logger'
import { parseUrl } from '../network'
import { Target } from '../primitives/target'
import { RpcSocketClient } from '../rpc/clients'
import { RpcTcpClient } from '../rpc/clients/tcpClient'
import { RpcTlsClient } from '../rpc/clients/tlsClient'
import { SerializedBlockTemplate } from '../serde/BlockTemplateSerde'
import { BigIntUtils } from '../utils/bigint'
import { ErrorUtils } from '../utils/error'
import { FileUtils } from '../utils/file'
import { SetIntervalToken, SetTimeoutToken } from '../utils/types'
import { MiningPoolShares } from './poolShares'
import { MiningStatusMessage } from './stratum/messages'
import { StratumServer } from './stratum/stratumServer'
import { StratumServerClient } from './stratum/stratumServerClient'
import { mineableHeaderString } from './utils'
import { WebhookNotifier } from './webhooks'

const RECALCULATE_TARGET_TIMEOUT = 30000
const KAFKA_SHARE_BATCH = 500

export type ShareRecord = {
  coin_type: string
  pool_id: string
  user_address: string | null
  isEmailUser: boolean
  worker_id: string
  height: number | null
  block_hash: string | null
  difficulty: string
  status: string
  timestamp: number
}

export class MiningPool {
  readonly stratum: StratumServer
  readonly rpc: RpcSocketClient
  readonly logger: Logger
  readonly shares: MiningPoolShares
  readonly config: Config
  readonly kafka: Producer | undefined
  readonly rpcProxy: RpcTcpClient[]
  readonly webhooks: WebhookNotifier[]

  private started: boolean
  private stopPromise: Promise<void> | null = null
  private stopResolve: (() => void) | null = null

  private connectWarned: boolean
  private connectTimeout: SetTimeoutToken | null

  name: string

  nextMiningRequestId: number
  miningRequestBlocks: LeastRecentlyUsed<number, SerializedBlockTemplate>
  recentSubmissions: Map<number, string[]>

  difficulty: bigint
  target: Buffer

  currentHeadTimestamp: number | null
  currentHeadDifficulty: bigint | null
  kafkaShares: ShareRecord[]

  recalculateTargetInterval: SetIntervalToken | null

  private notifyStatusInterval: SetIntervalToken | null

  private constructor(options: {
    rpc: RpcSocketClient
    shares: MiningPoolShares
    config: Config
    logger: Logger
    webhooks?: WebhookNotifier[]
    host?: string
    port?: number
    kafkaHosts: string[]
    banning?: boolean
    rpcProxy?: string[]
  }) {
    this.rpc = options.rpc
    this.logger = options.logger
    this.webhooks = options.webhooks ?? []
    this.stratum = new StratumServer({
      pool: this,
      config: options.config,
      logger: this.logger,
      host: options.host,
      port: options.port,
      banning: options.banning,
    })
    this.config = options.config
    this.shares = options.shares
    this.nextMiningRequestId = 0
    this.miningRequestBlocks = new LeastRecentlyUsed(30)
    this.recentSubmissions = new Map()
    this.currentHeadTimestamp = null
    this.currentHeadDifficulty = null

    this.name = this.config.get('poolName')

    this.difficulty = BigInt(this.config.get('poolDifficulty'))
    const basePoolTarget = Target.fromDifficulty(this.difficulty).asBigInt()
    this.target = BigIntUtils.toBytesBE(basePoolTarget, 32)

    this.connectTimeout = null
    this.connectWarned = false
    this.started = false

    this.recalculateTargetInterval = null
    this.notifyStatusInterval = null
    this.kafkaShares = []

    if (options.kafkaHosts.length !== 0) {
      this.logger.info('Kafka Init')
      this.kafka = new Producer(
        new KafkaClient({ kafkaHost: `${options.kafkaHosts.join(',')}` }) ?? null,
      )
      this.kafka.on('ready', () => {
        this.logger.info('Kafka is ready, please send message')
      })
      this.kafka.on('error', (err) => {
        this.logger.info(`Kafka Client ${options.kafkaHosts.join(',')} error: ${err}`)
      })
    } else {
      this.logger.info('No Kafka')
      this.kafka = undefined
    }

    this.rpcProxy = []
    if (options.rpcProxy) {
      options.rpcProxy.map((node) => {
        const parsed = parseUrl(node)
        if (parsed.hostname && parsed.port) {
          if (this.config.get('enableRpcTls')) {
            this.logger.info(`MiningRpcTlsClient mounted ${parsed.hostname}:${parsed.port}`)
            this.rpcProxy.push(new RpcTlsClient(parsed.hostname, parsed.port))
          } else {
            this.logger.info(`MiningRpcTcpClient mounted ${parsed.hostname}:${parsed.port}`)
            this.rpcProxy.push(new RpcTcpClient(parsed.hostname, parsed.port))
          }
        }
      })
      this.logger.info(
        `MiningRpcNode mounted on pool: ${JSON.stringify(
          this.rpcProxy.map((a) => `${a.host}:${a.port}`),
        )}`,
      )
    } else {
      this.logger.info('No miningRpcNode mounted on pool')
    }
  }

  static async init(options: {
    rpc: RpcSocketClient
    config: Config
    logger: Logger
    webhooks?: WebhookNotifier[]
    enablePayouts?: boolean
    host?: string
    port?: number
    balancePercentPayoutFlag?: number
    banning?: boolean
    kafkaHosts: string[]
    rpcProxy: string[]
  }): Promise<MiningPool> {
    const shares = await MiningPoolShares.init({
      rpc: options.rpc,
      config: options.config,
      logger: options.logger,
      webhooks: options.webhooks,
      enablePayouts: options.enablePayouts,
      balancePercentPayoutFlag: options.balancePercentPayoutFlag,
    })

    return new MiningPool({
      rpc: options.rpc,
      logger: options.logger,
      config: options.config,
      webhooks: options.webhooks,
      host: options.host,
      port: options.port,
      shares,
      banning: options.banning,
      kafkaHosts: options.kafkaHosts,
      rpcProxy: options.rpcProxy,
    })
  }

  async start(): Promise<void> {
    if (this.started) {
      return
    }

    this.stopPromise = new Promise((r) => (this.stopResolve = r))
    this.started = true
    await this.shares.start()

    this.logger.info(
      `Starting stratum server v${String(this.stratum.version)} on ${this.stratum.host}:${
        this.stratum.port
      }`,
    )
    this.stratum.start()

    this.logger.info('Connecting to node...')
    this.rpcProxy.map((rpc) => rpc.onClose.on(this.onDisconnectRpcProxy))

    const statusInterval = this.config.get('poolStatusNotificationInterval')
    if (statusInterval > 0) {
      this.notifyStatusInterval = setInterval(
        () => void this.notifyStatus(),
        statusInterval * 1000,
      )
    }

    void this.startConnectingRpc()
  }

  async stop(): Promise<void> {
    if (!this.started) {
      return
    }

    this.logger.debug('Stopping pool, goodbye')

    this.started = false
    this.rpcProxy.map((rpc) => rpc.close())
    this.stratum.stop()

    await this.shares.stop()

    if (this.stopResolve) {
      this.stopResolve()
    }

    if (this.connectTimeout) {
      clearTimeout(this.connectTimeout)
    }

    if (this.recalculateTargetInterval) {
      clearInterval(this.recalculateTargetInterval)
    }

    if (this.notifyStatusInterval) {
      clearInterval(this.notifyStatusInterval)
    }
  }

  async waitForStop(): Promise<void> {
    await this.stopPromise
  }

  getTarget(): string {
    return this.target.toString('hex')
  }

  sendKafka(
    client: StratumServerClient,
    status: string,
    block_hash: string | null,
    height: number | null,
  ): void {
    if (!this.kafka) {
      return
    }
    const difficulty = this.config.get('poolDifficulty').toString()
    const shareRecord = {
      coin_type: 'IRON',
      pool_id: '6block-ironfish',
      user_address: client.publicAddress,
      isEmailUser: client.useEmail,
      worker_id: client.name ? client.name : `${client.id}`,
      height: height,
      block_hash: block_hash,
      difficulty: difficulty, // share difficulty
      status: status, // one of 'STALE', 'VALID', 'INVALID", 'SCORED'
      timestamp: new Date().getTime(),
    }
    if (shareRecord.height === null) {
      this.logger.info(`[Pool] Share null height: ${JSON.stringify(shareRecord)}`)
    }
    this.kafkaShares.push(shareRecord)

    if (this.kafkaShares.length >= KAFKA_SHARE_BATCH) {
      const shareKafka = [
        {
          topic: 'ironshare',
          messages: JSON.stringify(this.kafkaShares),
        },
      ]
      this.kafkaShares = []
      this.kafka.send(shareKafka, (error, data) => {
        this.logger.debug(`${JSON.stringify(data)} submitted successfully! `)
        if (error) {
          this.logger.info(`Send kafka message error: ${error}`)
        }
      })
    }
  }

  async submitWork(
    client: StratumServerClient,
    miningRequestId: number,
    randomness: string,
  ): Promise<void> {
    Assert.isNotNull(client.publicAddress)
    Assert.isNotNull(client.graffiti)

    const originalBlockTemplate = this.miningRequestBlocks.get(miningRequestId)

    if (!originalBlockTemplate) {
      this.logger.warn(
        `Client ${client.id} work for invalid mining request: ${miningRequestId}`,
      )
      this.sendKafka(client, 'INVALID', null, null)
      return
    }

    const blockTemplate = Object.assign({}, originalBlockTemplate)
    blockTemplate.header = Object.assign({}, originalBlockTemplate.header)

    const height = originalBlockTemplate.header.sequence

    const isDuplicate = this.isDuplicateSubmission(client.id, randomness)

    if (isDuplicate) {
      this.logger.warn(
        `Client ${client.id} submitted a duplicate mining request: ${miningRequestId}, ${randomness}`,
      )
      this.sendKafka(client, 'INVALID', null, height)
      return
    }

    blockTemplate.header.graffiti = client.graffiti.toString('hex')
    blockTemplate.header.randomness = randomness

    let headerBytes
    try {
      headerBytes = mineableHeaderString(blockTemplate.header)
    } catch (error) {
      this.stratum.peers.punish(client, `${client.id} sent malformed work.`)
      return
    }

    const hashedHeader = blake3(headerBytes)

    if (hashedHeader.compare(this.target) !== 1) {
      if (miningRequestId !== this.nextMiningRequestId - 1) {
        this.logger.debug(
          `Client ${client.id} submitted work for stale mining request: ${miningRequestId}`,
        )
        this.sendKafka(client, 'STALE', hashedHeader.toString('hex'), height)
        return
      }

      this.addWorkSubmission(client.id, randomness)

      if (hashedHeader.compare(Buffer.from(blockTemplate.header.target, 'hex')) !== 1) {
        this.logger.debug('Valid block, submitting to node')

        let result = undefined
        let submitResp = 0
        try {
          await Promise.all(
            this.rpcProxy.map(async (rpc) => {
              if (rpc.isConnected) {
                const submitResponse = await rpc.submitBlock(blockTemplate)
                if (submitResponse.content.added) {
                  this.logger.info(
                    `Block submitted to rpc node ${rpc.host}:${rpc.port} successfully!`,
                  )
                  submitResp += 1
                } else {
                  result = submitResponse.content.reason
                }
              }
            }),
          )
        } catch (e) {
          this.logger.warn(`Exception when submitBlockTemplate, ${JSON.stringify(e)}`)
        }

        if (submitResp > 0) {
          const hashRate = 0
          const hashedHeaderHex = hashedHeader.toString('hex')

          this.logger.info(
            `Block ${hashedHeaderHex} submitted successfully! ${FileUtils.formatHashRate(
              hashRate,
            )}/s`,
          )
          this.sendKafka(client, 'SCORED', hashedHeader.toString('hex'), height)
          this.webhooks.map((w) =>
            w.poolSubmittedBlock(hashedHeaderHex, hashRate, this.stratum.clients.size),
          )
        } else {
          this.logger.info(
            `Block was rejected: ${result ? result : 'by at least one of mining rpc nodes'}`,
          )
          this.sendKafka(client, 'VALID', hashedHeader.toString('hex'), height)
        }
      } else {
        this.sendKafka(client, 'VALID', hashedHeader.toString('hex'), height)
      }
      this.logger.debug('Valid pool share submitted')
    } else {
      this.addWorkSubmission(client.id, randomness)

      this.sendKafka(client, 'INVALID', hashedHeader.toString('hex'), height)
    }
  }

  private async startConnectingRpc(): Promise<void> {
    let connectedProxy = 0
    await Promise.all(
      this.rpcProxy.map(async (rpc) => {
        if (await rpc.tryConnect()) {
          connectedProxy += 1
        } else {
          this.connectTimeout = setTimeout(
            () => void this.startConnectingMiningRpc(`${rpc.host}:${rpc.port}`),
            5000,
          )
        }
      }),
    )

    if (!this.started) {
      return
    }

    if (connectedProxy === 0) {
      if (!this.connectWarned) {
        this.logger.warn(
          `Failed to connect to node on ${String(this.rpc.connection.mode)}, retrying...`,
        )
        this.connectWarned = true
      }

      this.connectTimeout = setTimeout(() => void this.startConnectingRpc(), 5000)
      return
    }

    if (connectedProxy) {
      this.webhooks.map((w) => w.poolConnectedRpc(connectedProxy))
      this.logger.info(`Successfully connected to ${connectedProxy} mining rpc nodes`)
    }

    this.connectWarned = false
    this.logger.info('Successfully connected to node')
    this.logger.info('Listening to node for new blocks')

    void this.processNewBlocksProxy().catch(async (e: unknown) => {
      this.logger.error('Fatal error occured while processing blocks from node:')
      this.logger.error(ErrorUtils.renderError(e, true))
      await this.stop()
    })
  }

  private async startConnectingMiningRpc(connect: string): Promise<void> {
    await Promise.all(
      this.rpcProxy.map(async (rpc) => {
        if (`${rpc.host}:${rpc.port}` === connect) {
          if (await rpc.tryConnect()) {
            this.logger.info(`Successfully connected to mining rpc node ${connect}`)
            this.webhooks.map((w) => w.poolConnectedToSingleRpc(`${connect}`))
          } else {
            this.connectTimeout = setTimeout(
              () => void this.startConnectingMiningRpc(connect),
              5000,
            )
          }
        }
      }),
    )
  }

  private onDisconnectRpcProxy = (rpc: string): void => {
    this.logger.info(`Disconnected from mining rpc node ${rpc} unexpectedly. Reconnecting.`)
    this.webhooks.map((w) => w.poolDisconnectedRpc(rpc))
    void this.startConnectingMiningRpc(rpc)
  }

  private async processNewBlocks() {
    for await (const payload of this.rpc.blockTemplateStream().contentStream()) {
      Assert.isNotUndefined(payload.previousBlockInfo)
      this.restartCalculateTargetInterval()

      const currentHeadTarget = new Target(Buffer.from(payload.previousBlockInfo.target, 'hex'))
      this.currentHeadDifficulty = currentHeadTarget.toDifficulty()
      this.currentHeadTimestamp = payload.previousBlockInfo.timestamp

      this.distributeNewBlock(payload)
    }
  }

  private async processNewBlocksProxy() {
    await Promise.all(
      this.rpcProxy.map(async (rpc) => {
        if (rpc.isConnected) {
          try {
            for await (const payload of rpc.blockTemplateStream().contentStream(true)) {
              Assert.isNotUndefined(payload.previousBlockInfo)
              this.processTemplate(payload, `${rpc.host}:${rpc.port}`)
            }
          } catch (e) {
            this.logger.warn(
              `Exception when getBlockTemplate on ${rpc.host}:${rpc.port}, ${JSON.stringify(
                e,
              )}`,
            )
          }
        }
      }),
    )
  }

  private processTemplate(payload: SerializedBlockTemplate, from: string) {
    const currentBlockTemplate = this.miningRequestBlocks.get(this.nextMiningRequestId - 1)
    let currentTaskSequence = undefined
    let currentTaskPreviousHash = undefined
    if (currentBlockTemplate) {
      currentTaskSequence = currentBlockTemplate.header.sequence
      currentTaskPreviousHash = currentBlockTemplate.header.previousBlockHash
    }
    if (currentTaskSequence && payload.header.sequence < currentTaskSequence) {
      this.logger.warn(
        `Receive stale blockTemplate for sequence ${payload.header.sequence} from ${from}`,
      )
      return
    }
    if (
      currentTaskPreviousHash &&
      currentTaskPreviousHash === payload.header.previousBlockHash
    ) {
      this.logger.info(
        `Receive duplicated blockTemplate for sequence ${payload.header.sequence} from ${from}`,
      )
      return
    }
    Assert.isNotUndefined(payload.previousBlockInfo)

    this.logger.info(
      `Receive new blockTemplate for sequence ${payload.header.sequence} from ${from}`,
    )
    this.restartCalculateTargetInterval()

    const currentHeadTarget = new Target(Buffer.from(payload.previousBlockInfo.target, 'hex'))
    this.currentHeadDifficulty = currentHeadTarget.toDifficulty()
    this.currentHeadTimestamp = payload.previousBlockInfo.timestamp

    this.distributeNewBlock(payload)
  }

  private recalculateTarget() {
    this.logger.debug('recalculating target')

    Assert.isNotNull(this.currentHeadTimestamp)
    Assert.isNotNull(this.currentHeadDifficulty)

    const currentBlock = this.miningRequestBlocks.get(this.nextMiningRequestId - 1)
    const latestBlock = JSON.parse(JSON.stringify(currentBlock)) as typeof currentBlock

    Assert.isNotNull(latestBlock)

    const newTime = new Date()
    const newTarget = Target.fromDifficulty(
      Target.calculateDifficulty(
        newTime,
        new Date(this.currentHeadTimestamp),
        this.currentHeadDifficulty,
      ),
    )

    // Target might be the same if there is a slight timing issue or if the block is at max target.
    // In this case, it is detrimental to send out new work as it will needlessly reset miner's search
    // space, resulting in duplicated work.
    const existingTarget = BigIntUtils.fromBytes(Buffer.from(latestBlock.header.target, 'hex'))
    if (newTarget.asBigInt() === existingTarget) {
      this.logger.debug(
        `New target ${newTarget.asBigInt()} is the same as the existing target, no need to send out new work.`,
      )
      return
    }

    latestBlock.header.target = BigIntUtils.toBytesBE(newTarget.asBigInt(), 32).toString('hex')
    latestBlock.header.timestamp = newTime.getTime()
    this.distributeNewBlock(latestBlock)

    this.logger.debug('target recalculated', { prevHash: latestBlock.header.previousBlockHash })
  }

  private distributeNewBlock(newBlock: SerializedBlockTemplate) {
    Assert.isNotNull(this.currentHeadTimestamp)
    Assert.isNotNull(this.currentHeadDifficulty)

    const miningRequestId = this.nextMiningRequestId++
    this.miningRequestBlocks.set(miningRequestId, newBlock)
    this.recentSubmissions.clear()

    this.stratum.newWork(miningRequestId, newBlock)
  }

  private restartCalculateTargetInterval() {
    if (this.recalculateTargetInterval) {
      clearInterval(this.recalculateTargetInterval)
    }

    this.recalculateTargetInterval = setInterval(() => {
      this.recalculateTarget()
    }, RECALCULATE_TARGET_TIMEOUT)
  }

  private isDuplicateSubmission(clientId: number, randomness: string): boolean {
    const submissions = this.recentSubmissions.get(clientId)
    if (submissions == null) {
      return false
    }
    return submissions.includes(randomness)
  }

  private addWorkSubmission(clientId: number, randomness: string): void {
    const submissions = this.recentSubmissions.get(clientId)
    if (submissions == null) {
      this.recentSubmissions.set(clientId, [randomness])
    } else {
      submissions.push(randomness)
      this.recentSubmissions.set(clientId, submissions)
    }
  }

  async estimateHashRate(publicAddress?: string): Promise<number> {
    // BigInt can't contain decimals, so multiply then divide to give decimal precision
    const shareRate = await this.shares.shareRate(publicAddress)
    const decimalPrecision = 1000000
    return (
      Number(BigInt(Math.floor(shareRate * decimalPrecision)) * this.difficulty) /
      decimalPrecision
    )
  }

  async notifyStatus(): Promise<void> {
    const status = await this.getStatus()
    this.logger.debug(`Mining pool status: ${JSON.stringify(status)}`)
    this.webhooks.map((w) => w.poolStatus(status))
  }

  async getStatus(publicAddress?: string): Promise<MiningStatusMessage> {
    let addressMinerCount = 0

    const status: MiningStatusMessage = {
      name: this.name,
      hashRate: 0,
      miners: this.stratum.subscribed,
      sharesPending: 0,
      bans: this.stratum.peers.banCount,
      clients: this.stratum.clients.size,
    }

    if (publicAddress) {
      const [addressHashRate, addressSharesPending] = await Promise.all([
        this.estimateHashRate(publicAddress),
        this.shares.sharesPendingPayout(publicAddress),
      ])

      const addressConnectedMiners: string[] = []

      for (const client of this.stratum.clients.values()) {
        if (client.subscribed && client.publicAddress === publicAddress) {
          addressMinerCount++
          addressConnectedMiners.push(client.name || `Miner ${client.id}`)
        }
      }

      status.addressStatus = {
        publicAddress: publicAddress,
        hashRate: addressHashRate,
        miners: addressMinerCount,
        connectedMiners: addressConnectedMiners,
        sharesPending: addressSharesPending,
      }
    }

    return status
  }
}
