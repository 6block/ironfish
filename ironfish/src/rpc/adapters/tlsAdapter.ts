/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
import net from 'net'
import tls from 'tls'
import { v4 as uuid } from 'uuid'
import { FileSystem } from '../../fileSystems'
import { createRootLogger, Logger } from '../../logger'
import { IronfishNode } from '../../node'
import { TlsUtils } from '../../utils'
import { ApiNamespace } from '../routes'
import { RpcSocketAdapter } from './socketAdapter/socketAdapter'

export class RpcTlsAdapter extends RpcSocketAdapter {
  readonly fileSystem: FileSystem
  readonly nodeKeyPath: string
  readonly nodeCertPath: string
  node: IronfishNode

  constructor(
    host: string,
    port: number,
    fileSystem: FileSystem,
    nodeKeyPath: string,
    nodeCertPath: string,
    node: IronfishNode,
    logger: Logger = createRootLogger(),
    namespaces: ApiNamespace[],
  ) {
    super(host, port, logger, namespaces)
    this.fileSystem = fileSystem
    this.nodeKeyPath = nodeKeyPath
    this.nodeCertPath = nodeCertPath
    this.node = node
    this.enableAuthentication = false
  }

  protected async createServer(): Promise<net.Server> {
    const rpcAuthToken = this.node.internal.get('rpcAuthToken')

    if (!rpcAuthToken || rpcAuthToken === '') {
      this.logger.debug(
        `Missing RPC Auth token in internal.json config. Automatically generating auth token.`,
      )
      this.node.internal.set('rpcAuthToken', uuid())
      await this.node.internal.save()
    }

    const options = await TlsUtils.getTlsOptions(
      this.fileSystem,
      this.nodeKeyPath,
      this.nodeCertPath,
    )
    return tls.createServer(options, (socket) => this.onClientConnection(socket))
  }
}
