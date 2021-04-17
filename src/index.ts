import EventEmitter from 'events'

import type {
  Hyperdrive as HyperdriveP,
  Hypercore,
  FD,
  MountOptions,
  MountInfo,
  MountMap,
  Watcher,
  Stat,
  ReadStreamOptions,
  ReadDirOptions,
  EncodableOptions,
  TagMap,
  ExtensionHandlers,
  Extension,
  Peer
} from 'hyper-typings/promises'
import type { Hyperdrive as HyperdriveCB } from 'hyper-typings/callbacks'

// TODO: Use the actual path once the module is published somewhere
import Group, { ID, Request, Response } from '@consento/group/src'

import multiHyperdrive from 'multi-hyperdrive'
import hyperdrivePromise from '@geut/hyperdrive-promise'

export interface ConsentoMultiHyperdriveOptions {
  getHyperdrive: (url: string | Buffer, shouldLookup: boolean) => Promise<HyperdriveCB>
  getHypercore: (url: string | Buffer, shouldLookup: boolean) => Promise<Hypercore>
}

export class ConsentoMultiHyperdrive extends EventEmitter implements HyperdriveP {
  private _ready: Promise<void> | null
  private readonly _key: string | Buffer | null
  private readonly _group: Group | null = null
  private _drive: HyperdriveP | null = null

  private readonly getHypercore: (url: string | Buffer, shouldLookup: boolean) => Promise<Hypercore>
  private readonly getHyperdrive: (url: string | Buffer, shouldLookup: boolean) => Promise<HyperdriveCB>

  constructor (key: string | Buffer, { getHyperdrive, getHypercore, ...options }: ConsentoMultiHyperdriveOptions) {
    super()

    this.getHyperdrive = getHyperdrive
    this.getHypercore = getHypercore

    this._key = key
    this._ready = this._init()
  }

  private async _init (): Promise<void> {
    // TODO Generate group if it doesn't exist
    const header = await this.group.getMetadataFor(this.group.id)
    const contentFeed = header?.contentFeed
    if (contentFeed === undefined) {
      throw new Error('Invalid Group Metadata')
    }
    const multi = multiHyperdrive(await this.getHyperdrive(contentFeed, true))
    this._drive = hyperdrivePromise(multi)

    await this.drive.ready()

    this._ready = null
  }

  async ready (): Promise<void> {
    await this._ready
  }

  private async _sync (): Promise<void> {
    await this._ready
    await this.group.sync()
    // Get all members and their removed/added status
    // Get their feed metadatas (hyperdrive keys)
  }

  get group (): Group {
    if (this._group === null) throw new Error('Not Ready')
    return this._group
  }

  get drive (): HyperdriveP {
    if (this._drive === null) throw new Error('Not Ready')
    return this._drive
  }

  get version (): number {
    return this.drive.version
  }

  get writable (): boolean {
    return this.drive.writable
  }

  get key (): Buffer {
    return Buffer.from(this.group.id, 'hex')
  }

  get discoveryKey (): Buffer {
    // TODO: Derive from group id
    return this.drive.discoveryKey
  }

  get peers (): Peer[] {
    if (this._drive == null) return []
    return this.drive.peers
  }

  async createIdentity (): Promise<ID> {
    await this.ready()
    // These URLs will be used to generate hypercores
    const driveURL = `${this.group.id}-drive`
    const groupURL = `${this.group.id}-group`
    const drive = await this.getHyperdrive(driveURL, false)
    const metadata = { contentFeed: drive.key }
    await this.group.createOwnFeed(groupURL, metadata)

    return this.group.feed.id
  }

  getActiveRequests (): Request[] {
    if (this._group === null) return []
    return this.group.getActiveRequests()
  }

  async denyRequest (req: Request): Promise<Response> {
    await this._ready
    return await this.group.denyRequest(req)
  }

  async acceptRequest (req: Request): Promise<Response> {
    await this._ready
    return await this.group.acceptRequest(req)
  }

  async requestRemove (who: ID): Promise<Request> {
    await this._ready
    return await this.group.requestRemove(who)
  }

  async requestAdd (who: ID): Promise<Request> {
    await this._ready
    return await this.group.requestAdd(who)
  }

  registerExtension<M=Buffer>(name: string, handlers: ExtensionHandlers<M>): Extension<M> {
    return this.drive.registerExtension<M>(name, handlers)
  }

  checkout (version: number): HyperdriveP {
  // TODO: Account for async
    return this.drive.checkout(version)
  }

  async createTag (name: string, version?: number): Promise<void> {
    await this._ready
    return await this.drive.createTag(name, version)
  }

  async getTaggedVersion (name: string): Promise<number> {
    await this._ready
    return await this.drive.getTaggedVersion(name)
  }

  async deleteTag (name: string): Promise<void> {
    await this._ready
    return await this.drive.deleteTag(name)
  }

  async getAllTags (): Promise<TagMap> {
    await this._ready
    return await this.drive.getAllTags()
  }

  async download (path?: string): Promise<void> {
    await this._ready
    return await this.drive.download(path)
  }

  createReadStream (name: string, options?: ReadStreamOptions): NodeJS.ReadableStream {
  // TODO: Account for async loading
    return this.drive.createReadStream(name, options)
  }

  async readFile<E=Buffer>(name: string, options?: EncodableOptions): Promise<E> {
    await this._ready
    return await this.drive.readFile<E>(name, options)
  }

  createWriteStream (name: string): NodeJS.WritableStream {
  // TODO: Account for async loading of the stream
    return this.drive.createWriteStream(name)
  }

  async writeFile (name: string, data: Buffer | string, options?: EncodableOptions): Promise<void> {
    await this._ready
    return await this.drive.writeFile(name, data, options)
  }

  async unlink (name: string): Promise<void> {
    await this._ready
    return await this.drive.unlink(name)
  }

  async mkdir (name: string): Promise<void> {
    await this._ready
    return await this.drive.mkdir(name)
  }

  async symlink (target: string, linkname: string): Promise<void> {
    await this._ready
    return await this.drive.symlink(target, linkname)
  }

  async rmdir (name: string): Promise<void> {
    await this._ready
    return await this.drive.rmdir(name)
  }

  async readdir (name: string, options?: ReadDirOptions): Promise<string[] | Stat[]> {
    await this._ready
    return await this.drive.readdir(name, options)
  }

  async stat (name: string): Promise<Stat> {
    await this._ready
    return await this.drive.stat(name)
  }

  async lstat (name: string): Promise<Stat> {
    await this._ready
    return await this.drive.lstat(name)
  }

  async info (name: string): Promise<MountInfo> {
    await this._ready
    return await this.drive.info(name)
  }

  async access (name: string): Promise<void> {
    await this._ready
    return await this.drive.access(name)
  }

  async open (name: string, flags: string): Promise<FD> {
    await this._ready
    return await this.drive.open(name, flags)
  }

  async read (fd: FD, buf: Buffer, offset: number, len: number, position: number): Promise<void> {
    await this._ready
    return await this.drive.read(fd, buf, offset, len, position)
  }

  async write (fd: FD, buf: Buffer, offset: number, leng: number, position: number): Promise<void> {
    await this._ready
    return await this.drive.write(fd, buf, offset, leng, position)
  }

  watch (name: string, onchage: () => void): Watcher {
  // TODO: Account for async
    return this.drive.watch(name, onchage)
  }

  async mount (name: string, key: Buffer, opts?: MountOptions): Promise<void> {
    await this._ready
    return await this.drive.mount(name, key, opts)
  }

  async unmount (name: string): Promise<void> {
    await this._ready
    return await this.drive.unmount(name)
  }

  createMountStream (options?: MountOptions): NodeJS.ReadableStream {
  // TODO: Account for async
    return this.drive.createMountStream(options)
  }

  async getAllMounts (options?: MountOptions): Promise<MountMap> {
    await this._ready
    return await this.getAllMounts(options)
  }

  async close (fd?: FD): Promise<void> {
    await this._ready
    if (fd !== undefined) {
      return await this.drive.close(fd)
    }

    await Promise.all([
      this.drive.close(),
      await this.group.close()
    ])
  }

  async destroyStorage (): Promise<void> {
    await this._ready
    // TODO
    await this.drive.destroyStorage()
  }
}
