/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
import * as yup from 'yup'
import { IronfishNode } from '../../..'
import { GENESIS_BLOCK_SEQUENCE } from '../../../consensus'
import { BlockHeader } from '../../../primitives'
import { ValidationError } from '../../adapters'
import { ApiNamespace, router } from '../router'

export type BlockInfoResponse = {
  block: {
    height: number
    difficulty: string
    block_hash: string
    reward: number
    timestamp: number
  }
}

export type GetBlocksInfoRequest = {
  height: number
  number: number
}

export type GetBlocksInfoResponse = {
  blocks: Array<BlockInfoResponse>
}

export const GetBlocksInfoRequestSchema: yup.ObjectSchema<GetBlocksInfoRequest> = yup
  .object()
  .shape({
    height: yup.number(),
    number: yup.number(),
  })
  .defined()

export const GetBlocksInfoResponseSchema: yup.ObjectSchema<GetBlocksInfoResponse> = yup
  .object({
    blocks: yup
      .array(
        yup
          .object({
            block: yup
              .object({
                height: yup.number().defined(),
                difficulty: yup.string().defined(),
                block_hash: yup.string().defined(),
                reward: yup.number().defined(),
                timestamp: yup.number().defined(),
              })
              .defined(),
          })
          .defined(),
      )
      .defined(),
  })
  .defined()

router.register<typeof GetBlocksInfoRequestSchema, GetBlocksInfoResponse>(
  `${ApiNamespace.node}/getBlocksInfo`,
  GetBlocksInfoRequestSchema,
  async (request, node): Promise<void> => {
    const result: BlockInfoResponse[] = []
    let i: number
    for (i = 0; i < request.data.number; i++) {
      const blockinfo = await getBlockInfo(i + request.data.height, node)
      result.push(blockinfo)
    }
    request.end({ blocks: result })
  },
)

async function getBlockInfo(sequence: number, node: IronfishNode): Promise<BlockInfoResponse> {
  let header: BlockHeader | null = null
  let error = ''

  // Use negative numbers to start from the head of the chain
  if (sequence && sequence < 0) {
    sequence = Math.max(node.chain.head.sequence + sequence + 1, GENESIS_BLOCK_SEQUENCE)
  }

  if (sequence && !header) {
    header = await node.chain.getHeaderAtSequence(sequence)
    error = `No block found with sequence ${sequence}`
  }

  if (!header) {
    throw new ValidationError(error)
  }

  const block = await node.chain.getBlock(header)
  if (!block) {
    throw new ValidationError(`No block with header ${header.hash.toString('hex')}`)
  }

  let reward = 0

  for (const tx of block.transactions) {
    const fee = tx.fee()
    reward += Math.abs(Number(fee))
  }

  return {
    block: {
      height: Number(header.sequence),
      difficulty: header.target.toDifficulty().toString(),
      block_hash: header.hash.toString('hex'),
      reward: Number(reward),
      timestamp: header.timestamp.valueOf(),
    },
  }
}
