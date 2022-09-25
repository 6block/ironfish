/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
import {
  DEFAULT_POOL_HOST,
  DEFAULT_POOL_PORT,
  DEFAULT_POOL_TLS_HOST,
  DEFAULT_POOL_TLS_PORT,
  Discord,
  Lark,
  MiningPool,
  Monitor,
  parseUrl,
  StringUtils,
  TlsUtils,
  WebhookNotifier,
} from '@ironfish/sdk'
import { Flags } from '@oclif/core'
import dns from 'dns'
import { IronfishCommand } from '../../../command'
import { RemoteFlags } from '../../../flags'

export class StartPool extends IronfishCommand {
  static description = `Start a mining pool that connects to a node`

  static flags = {
    ...RemoteFlags,
    discord: Flags.string({
      char: 'd',
      description: 'A discord webhook URL to send critical information to',
    }),
    lark: Flags.string({
      char: 'l',
      description: 'A lark webhook URL to send critical information to',
    }),
    monitor: Flags.string({
      char: 'm',
      description: 'a monitor webhook URL to send critical information to',
    }),
    miningRpc: Flags.string({
      char: 'r',
      description: 'comma-separated addresses of mining rpc nodes to get blockTemplate from',
      multiple: true,
      required: true,
    }),
    kafkaHosts: Flags.string({
      char: 'k',
      description:
        'a host:port Kafka server address to connect to: 172.18.1.1:9092,172.18.1.2:9092 ',
    }),
    host: Flags.string({
      char: 'h',
      description: `A host:port listen for stratum connections: ${DEFAULT_POOL_HOST}:${String(
        DEFAULT_POOL_PORT,
      )}`,
    }),
    tlsHost: Flags.string({
      description: `A host:port listen for stratum connections over tls: ${DEFAULT_POOL_TLS_HOST}:${String(
        DEFAULT_POOL_TLS_PORT,
      )}`,
    }),
    payouts: Flags.boolean({
      default: true,
      allowNo: true,
      description: 'Whether the pool should payout or not. Useful for solo miners',
    }),
    balancePercentPayout: Flags.integer({
      description: 'Whether the pool should payout or not. Useful for solo miners',
    }),
    banning: Flags.boolean({
      description: 'Whether the pool should ban peers for errors or bad behavior',
      allowNo: true,
    }),
    enableTls: Flags.boolean({
      description: 'Whether the pool should listen for connections over tls',
      allowNo: true,
    }),
  }

  pool: MiningPool | null = null

  async start(): Promise<void> {
    const { flags } = await this.parse(StartPool)
    const { miningRpc } = flags

    const poolName = this.sdk.config.get('poolName')
    const nameByteLen = StringUtils.getByteLength(poolName)
    if (nameByteLen > 18) {
      this.warn(`The provided name ${poolName} has a byte length of ${nameByteLen}`)
      this.warn(
        'It is recommended to keep the pool name below 18 bytes in length to avoid possible work duplication issues',
      )
    }

    const rpc = this.sdk.client

    this.log(`Starting pool with name ${poolName}`)

    const webhooks: WebhookNotifier[] = []

    const discordWebhook = flags.discord ?? this.sdk.config.get('poolDiscordWebhook')
    if (discordWebhook) {
      webhooks.push(
        new Discord({
          webhook: discordWebhook,
          logger: this.logger,
          explorerBlocksUrl: this.sdk.config.get('explorerBlocksUrl'),
          explorerTransactionsUrl: this.sdk.config.get('explorerTransactionsUrl'),
        }),
      )

      this.log(`Discord enabled: ${discordWebhook}`)
    }

    const larkWebhook = flags.lark ?? this.sdk.config.get('poolLarkWebhook')
    if (larkWebhook) {
      webhooks.push(
        new Lark({
          webhook: larkWebhook,
          logger: this.logger,
          explorerBlocksUrl: this.sdk.config.get('explorerBlocksUrl'),
          explorerTransactionsUrl: this.sdk.config.get('explorerTransactionsUrl'),
        }),
      )

      this.log(`Lark enabled: ${larkWebhook}`)
    }

    const monitorWebhook = flags.monitor ?? this.sdk.config.get('poolMonitorWebhook')
    if (monitorWebhook) {
      webhooks.push(
        new Monitor({
          webhook: monitorWebhook,
          logger: this.logger,
          explorerBlocksUrl: this.sdk.config.get('explorerBlocksUrl'),
          explorerTransactionsUrl: this.sdk.config.get('explorerTransactionsUrl'),
        }),
      )

      this.log(`Monitor enabled: ${monitorWebhook}`)
    }

    let rpcProxy: string[] = []
    if (miningRpc !== undefined) {
      rpcProxy = miningRpc
        .flatMap((n) => n.split(','))
        .filter(Boolean)
        .map((n) => n.trim())
    }
    this.log(`MiningRpc detected: ${rpcProxy.join(',')}`)

    let host = undefined
    let port = undefined

    if (flags.host) {
      const parsed = parseUrl(flags.host)

      if (parsed.hostname) {
        const resolved = await dns.promises.lookup(parsed.hostname)
        host = resolved.address
      }

      if (parsed.port) {
        port = parsed.port
      }
    }

    const kafkahosts: string[] = []

    if (flags.kafkaHosts) {
      const kafkaHostsArray = flags.kafkaHosts.split(',')
      for (const kafkaHost of kafkaHostsArray) {
        let khost = undefined
        let kport = undefined
        const parsed = parseUrl(kafkaHost)
        if (parsed.hostname) {
          const resolved = await dns.promises.lookup(parsed.hostname)
          khost = resolved.address
        }
        if (parsed.port) {
          kport = parsed.port
        }
        if (khost && kport) {
          kafkahosts.push(`${khost}:${kport}`)
          this.log(`Connect to Kafka server : ${khost}:${kport}`)
        } else {
          this.warn(`Fail to parse Kafka server address, your input :${flags.kafkaHosts}`)
        }
      }
    }

    let tlsHost = undefined
    let tlsPort = undefined

    if (flags.tlsHost) {
      const parsed = parseUrl(flags.tlsHost)

      if (parsed.hostname) {
        const resolved = await dns.promises.lookup(parsed.hostname)
        tlsHost = resolved.address
      }

      if (parsed.port) {
        tlsPort = parsed.port
      }
    }

    let tlsOptions = undefined
    if (flags.enableTls) {
      const fileSystem = this.sdk.fileSystem
      const nodeKeyPath = this.sdk.config.get('tlsKeyPath')
      const nodeCertPath = this.sdk.config.get('tlsCertPath')
      tlsOptions = await TlsUtils.getTlsOptions(fileSystem, nodeKeyPath, nodeCertPath)
    }

    this.pool = await MiningPool.init({
      config: this.sdk.config,
      logger: this.logger,
      rpc,
      enablePayouts: flags.payouts,
      webhooks: webhooks,
      host: host,
      port: port,
      tlsHost: tlsHost,
      tlsPort: tlsPort,
      tlsOptions: tlsOptions,
      balancePercentPayoutFlag: flags.balancePercentPayout,
      banning: flags.banning,
      kafkaHosts: kafkahosts,
      rpcProxy: rpcProxy,
      enableTls: flags.enableTls,
    })

    await this.pool.start()
    await this.pool.waitForStop()
  }

  async closeFromSignal(): Promise<void> {
    await this.pool?.stop()
  }
}
