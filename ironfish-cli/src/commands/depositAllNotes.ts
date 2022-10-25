/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

import {
  Assert,
  BigIntUtils,
  CurrencyUtils,
  MINIMUM_IRON_AMOUNT,
  RpcClient,
  WebApi,
} from '@ironfish/sdk'
import { CliUx, Flags } from '@oclif/core'
import { IronfishCommand } from '../command'
import { RemoteFlags } from '../flags'
import { ProgressBar } from '../types'

const REGISTER_URL = 'https://testnet.ironfish.network/signup'
export default class DepositAllNotes extends IronfishCommand {
  static description = 'Deposit all spendable notes for testnet points'

  client: RpcClient | null = null
  api: WebApi | null = new WebApi()

  static flags = {
    ...RemoteFlags,
    fee: Flags.string({
      char: 'f',
      description: `The fee amount in ORE, minimum of 1. 1 ORE is equal to ${MINIMUM_IRON_AMOUNT} IRON`,
    }),
    expirationSequenceDelta: Flags.integer({
      char: 'e',
      description: 'Max number of blocks for the transaction to wait before expiring',
    }),
    account: Flags.string({
      char: 'a',
      parse: (input) => Promise.resolve(input.trim()),
      description: 'The account to send money from',
    }),
    confirm: Flags.boolean({
      default: false,
      description: 'Confirm without asking',
    }),
  }

  async start(): Promise<void> {
    const { flags } = await this.parse(DepositAllNotes)

    this.client = await this.sdk.connectRpc(false, true)
    this.api = new WebApi()

    let fee = null

    if (flags.fee) {
      const [parsedFee] = BigIntUtils.tryParse(flags.fee)

      if (parsedFee != null) {
        fee = parsedFee
      }
    }

    if (fee == null) {
      try {
        // fees p25 of last 100 blocks
        const feeString = (await this.client.getFees({ numOfBlocks: 100 })).content.p25
        fee = CurrencyUtils.decode(feeString)
      } catch {
        fee = 1n
      }
    }

    const expirationSequenceDelta = flags.expirationSequenceDelta

    const accountName =
      flags.account || (await this.client.getDefaultAccount()).content.account?.name

    if (!accountName) {
      this.log(
        'Error fetching account name. Please use --account or make sure your default account is set properly.',
      )
      this.exit(1)
    }
    Assert.isNotUndefined(accountName)

    const bankDepositAddress =
      '1199c69c1b05e5c8d2e87fab15bf2338fdefed2a2fa6ab2604062dd15fb1bf430b021e41071e39e0b46e58'
    const { minDepositSize, maxDepositSize } = await this.api.getMinAndMaxDepositSize()
    const minDepositOre = CurrencyUtils.decodeIron(minDepositSize)
    const maxDepositOre = CurrencyUtils.decodeIron(maxDepositSize)

    if (!bankDepositAddress) {
      this.log('Error fetching deposit address. Please try again later.')
      this.exit(1)
    }

    const graffiti = (await this.client.getConfig({ name: 'blockGraffiti' })).content
      .blockGraffiti

    if (!graffiti) {
      this.log(
        `No graffiti found. Register at ${REGISTER_URL} then run \`ironfish testnet\` to configure your graffiti.\n 
        Sometimes you got this error for network reasons. If so you should update your network env properly or you can disable graffiti verify(advanced!).`,
      )
      this.exit(1)
    }
    Assert.isNotUndefined(graffiti)
    Assert.isNotNull(this.client)
    Assert.isNotNull(this.api)

    const balanceResp = await this.client.getAccountBalance({ account: accountName })
    const confirmedBalance = BigInt(balanceResp.content.confirmed)

    if (confirmedBalance < fee + minDepositOre) {
      this.log(
        `Insufficient balance: ${CurrencyUtils.renderIron(
          confirmedBalance,
        )} IRON.  Fee (${CurrencyUtils.renderIron(
          fee,
        )} IRON) + minimum deposit (${minDepositSize} IRON) = total required (${CurrencyUtils.renderIron(
          fee + minDepositOre,
        )} IRON)`,
      )
      this.exit(1)
    }
    const sendableOre = confirmedBalance - fee
    const oreToSend = BigIntUtils.min(
      (sendableOre / minDepositOre) * minDepositOre,
      maxDepositOre,
    )

    const newBalance = confirmedBalance - oreToSend - fee

    const displayConfirmedBalance = CurrencyUtils.renderIron(confirmedBalance, true)
    const displayAmount = CurrencyUtils.renderIron(oreToSend, true)
    const displayFee = CurrencyUtils.renderIron(fee, true)
    const displayNewBalance = CurrencyUtils.renderIron(newBalance, true)

    if (!flags.confirm) {
      this.log(`
  Your balance is ${displayConfirmedBalance}.

  You are about to send multiple transactions, each of them send amount of ${displayAmount} plus a transaction fee of ${displayFee} to the Iron Fish deposit account.
  Number of transactions depends on your spendable notes.
  The memo will contain the graffiti "${graffiti}".
  
  * This action is NOT reversible *
        `)

      const confirm = await CliUx.ux.confirm('Do you confirm (Y/N)?')
      if (!confirm) {
        this.log('Transaction aborted.')
        this.exit(0)
      }
    }

    // Run the progress bar for about 2 minutes
    // Chances are that the transaction will finish faster (error or faster computer)
    const bar = CliUx.ux.progress({
      barCompleteChar: '\u2588',
      barIncompleteChar: '\u2591',
      format: 'Creating the transaction: [{bar}] {percentage}% | ETA: {eta}s',
    }) as ProgressBar

    bar.start()

    let value = 0
    const timer = setInterval(() => {
      value++
      bar.update(value)
      if (value >= bar.getTotal()) {
        bar.stop()
      }
    }, 1000)

    const stopProgressBar = () => {
      clearInterval(timer)
      bar.update(100)
      bar.stop()
    }

    try {
      const result = await this.client.sendTransaction({
        fromAccountName: accountName,
        receives: [
          {
            publicAddress: bankDepositAddress,
            amount: CurrencyUtils.encode(oreToSend),
            memo: graffiti,
          },
        ],
        fee: CurrencyUtils.encode(fee),
        expirationSequenceDelta: expirationSequenceDelta,
        ifSpendAllNotes: true,
      })

      stopProgressBar()

      const transaction = result.content
      this.log(`
  This is the first one of transactions just sent to deposit account. For all transactions just sent, check log of your local ironfish node.

  Old Balance: ${displayConfirmedBalance}

  Depositing ${displayAmount} from ${transaction.fromAccountName}
  Transaction Hash: ${transaction.hash}
  Transaction fee: ${displayFee}

  New Balance: ${displayNewBalance}
  
  Find the transaction on https://explorer.ironfish.network/transaction/${transaction.hash
        } (it can take a few minutes before the transaction appears in the Explorer)`)
    } catch (error: unknown) {
      stopProgressBar()
      this.log(`An error occurred while sending the transaction.`)
      if (error instanceof Error) {
        this.error(error.message)
      }
      this.exit(2)
    }
  }
}