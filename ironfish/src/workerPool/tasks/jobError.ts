/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
import bufio from 'bufio'
import { ErrorUtils } from '../../utils'
import { WorkerMessage, WorkerMessageType } from './workerMessage'

export class SerializableJobError extends WorkerMessage {
  errorType = 'JobError'
  code: string | undefined
  stack: string | undefined
  message = ''

  constructor(jobId?: number, error?: unknown) {
    super(WorkerMessageType.JobError, jobId)

    if (error) {
      this.errorType =
        typeof error === 'object' ? error?.constructor.name ?? typeof error : 'unknown'

      this.code = undefined
      this.stack = undefined
      this.message = ErrorUtils.renderError(error)

      if (error instanceof Error) {
        this.code = error.name
        this.stack = error.stack

        if (ErrorUtils.isNodeError(error)) {
          this.code = error.code
        }
      }
    }
  }

  serialize(): Buffer {
    const bw = bufio.write()
    bw.writeVarString(this.errorType, 'utf8')
    bw.writeVarString(this.message, 'utf8')
    if (this.code) {
      bw.writeVarString(this.code, 'utf8')
    }
    if (this.stack) {
      bw.writeVarString(this.stack, 'utf8')
    }

    return bw.render()
  }

  // We return JobError so the error can be propagated to a calling Promise's reject method
  static deserialize(jobId: number, buffer: Buffer): JobError {
    const br = bufio.read(buffer, true)

    const errorType = br.readVarString('utf8')
    const message = br.readVarString('utf8')

    let stack = undefined
    let code = undefined

    try {
      code = br.readVarString('utf8')
    } catch {
      code = undefined
    }

    try {
      stack = br.readVarString('utf8')
    } catch {
      stack = undefined
    }

    const err = new SerializableJobError(jobId)
    err.errorType = errorType
    err.message = message
    err.code = code
    err.stack = stack

    return new JobError(err)
  }

  getSize(): number {
    const errorTypeSize = bufio.sizeVarString(this.errorType, 'utf8')
    const messageSize = bufio.sizeVarString(this.message, 'utf8')
    const codeSize = this.code ? bufio.sizeVarString(this.code, 'utf8') : 0
    const stackSize = this.stack ? bufio.sizeVarString(this.stack, 'utf8') : 0
    return errorTypeSize + messageSize + codeSize + stackSize
  }
}

export class JobError extends Error {
  type = 'JobError'
  code: string | undefined = undefined

  constructor(serializableJobError?: SerializableJobError) {
    super()

    if (serializableJobError) {
      this.code = serializableJobError.code
      this.stack = serializableJobError.stack
      this.message = serializableJobError.message
      this.type = serializableJobError.errorType
    }
  }
}

export class JobAbortedError extends JobError {}