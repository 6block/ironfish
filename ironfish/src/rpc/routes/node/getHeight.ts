/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
import * as yup from 'yup'
import { IronfishNode } from '../../../node'
import { MathUtils, PromiseUtils } from '../../../utils'
import { ValidationError } from '../../adapters'
import { ApiNamespace, router } from '../router'

export type GetHeightRequest =
  | undefined
  | {
      stream?: boolean
    }

export type GetHeightResponse = {
  block: {
    height: number
    difficulty: string
    block_hash: string
    reward: number
    timestamp: number
  }
}

export const GetHeightRequestSchema: yup.ObjectSchema<GetHeightRequest> = yup
  .object({
    stream: yup.boolean().optional(),
  })
  .optional()
  .default({})

export const GetHeightResponseSchema: yup.ObjectSchema<GetHeightResponse> = yup
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

router.register<typeof GetHeightRequestSchema, GetHeightResponse>(
  `${ApiNamespace.node}/getHeight`,
  GetHeightRequestSchema,
  async (request, node): Promise<void> => {
    const status = await getHeight(node)

    request.end(status)
    return
  },
)

async function getHeight(node: IronfishNode): Promise<GetHeightResponse> {
  const header = node.chain.head
  const block = await node.chain.getBlock(header)
  if (!block) {
    throw new ValidationError(`No block with header ${header.hash.toString('hex')}`)
  }

  let reward = 0

  for (const tx of block.transactions) {
    const fee = tx.fee()
    reward += Math.abs(Number(fee))
  }

  const height: GetHeightResponse = {
    block: {
      height: Number(header.sequence),
      difficulty: header.target.toDifficulty().toString(),
      block_hash: header.hash.toString('hex'),
      reward: Number(reward),
      timestamp: header.timestamp.valueOf(),
    },
  }

  return height
}
