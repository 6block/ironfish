/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
import * as yup from 'yup'
import { GENESIS_BLOCK_SEQUENCE } from '../../../consensus'
import { BlockHeader } from '../../../primitives'
import { ValidationError } from '../../adapters'
import { ApiNamespace, router } from '../router'

export type GetNodeBlockInfoRequest = {
  search?: string
  hash?: string
  height?: number
}

export type GetNodeBlockInfoResponse = {
  block: {
    height: number
    difficulty: string
    block_hash: string
    reward: number
    timestamp: number
  }
}

export const GetNodeBlockInfoRequestSchema: yup.ObjectSchema<GetNodeBlockInfoRequest> = yup
  .object()
  .shape({
    search: yup.string(),
    hash: yup.string(),
    height: yup.number(),
  })
  .defined()

export const GetNodeBlockInfoResponseSchema: yup.ObjectSchema<GetNodeBlockInfoResponse> = yup
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
  .defined()

router.register<typeof GetNodeBlockInfoRequestSchema, GetNodeBlockInfoResponse>(
  `${ApiNamespace.node}/getBlockInfo`,
  GetNodeBlockInfoRequestSchema,
  async (request, node): Promise<void> => {
    let header: BlockHeader | null = null
    let error = ''

    if (request.data.search) {
      const search = request.data.search.trim()
      const num = Number(search)

      if (Number.isInteger(num)) {
        request.data.height = num
      } else {
        request.data.hash = search
      }
    }

    // Use negative numbers to start from the head of the chain
    if (request.data.height && request.data.height < 0) {
      request.data.height = Math.max(
        node.chain.head.sequence + request.data.height + 1,
        GENESIS_BLOCK_SEQUENCE,
      )
    }

    if (request.data.hash) {
      const hash = Buffer.from(request.data.hash, 'hex')
      header = await node.chain.getHeader(hash)
      error = `No block found with hash ${request.data.hash}`
    }

    if (request.data.height && !header) {
      header = await node.chain.getHeaderAtSequence(request.data.height)
      error = `No block found with sequence ${request.data.height}`
    }

    if (!header) {
      throw new ValidationError(error)
    }

    const block = await node.chain.getBlock(header)
    if (!block) {
      throw new ValidationError(`No block with header ${header.hash.toString('hex')}`)
    }

    let reward: number = 0

    for (const tx of block.transactions) {
      const fee = tx.fee()
      reward += Math.abs(Number(fee))
    }

    request.end({
      block: {
        height: Number(header.sequence),
        difficulty: header.target.toDifficulty().toString(),
        block_hash: header.hash.toString('hex'),
        reward: Number(reward),
        timestamp: header.timestamp.valueOf(),
      }
    })
  },
)
