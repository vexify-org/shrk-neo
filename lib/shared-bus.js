'use strict'

/**
 * shrk-neo — SharedBus: a lock-free message queue in shared memory.
 *
 * Fixed per-slot regions: every slot owns `capacity` bytes, so space is
 * reclaimed the moment a consumer claims a message — no monotonic cursor.
 * Producers claim an EMPTY slot, publish a payload, and wake waiters;
 * consumers atomically claim a READY slot, read it, and release it.
 *
 * Arbitrary JS objects travel via v8.serialize(); Buffer / typed-array
 * values travel raw with a zero-copy option (no serialization).
 *
 * powered by vexify
 */

const v8 = require('node:v8')
const { Buffer } = require('node:buffer')

const MAGIC = 0x53425553 // 'SBUS'

// slot states
const S_EMPTY = 0
const S_FILLING = 1
const S_READY = 2
const S_CLAIMED = 3

// value types
const T_OBJECT = 0 // v8.serialize() payload
const T_BUFFER = 1 // raw byte slice (zero-copy capable)

// header layout (int32 view)
const IDX_MAGIC = 0
const IDX_SLOTS = 1
const IDX_COUNTER = 2 // bumped on every send (used by receive/waiters)
const HDR_FIELDS = 3

// per-slot layout (int32 view)
const MSG_STATE = 0
const MSG_LEN = 1
const MSG_TYPE = 2
const PER_MSG = 3

const WAIT_CHUNK = 2_000_000_000
const MIN_CAPACITY = 64

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function msgBase (i) {
  return HDR_FIELDS + i * PER_MSG
}

class SharedBus {
  /**
   * Create a new shared message queue. Pass `bus.sab` to workers and have
   * them call `SharedBus.attach(sab)`.
   *
   * @param {object} [options]
   * @param {number} [options.size=1*1024*1024] total bytes of the region.
   * @param {number} [options.slots=64] number of message slots. The per-slot
   *   capacity is `(size - header) / slots`; a message bigger than that
   *   throws.
   */
  constructor (options = {}) {
    const size = options.size ?? 1024 * 1024
    const slots = options.slots ?? 64

    if (!Number.isInteger(size) || size < 4096) {
      throw new RangeError(`shrk-neo: 'size' must be an integer >= 4096 (got ${size})`)
    }
    if (!Number.isInteger(slots) || slots < 1 || slots > 100000) {
      throw new RangeError(`shrk-neo: 'slots' must be an integer in [1, 100000] (got ${slots})`)
    }

    this.slots = slots
    this.sab = new SharedArrayBuffer(size)
    this._init(true)
  }

  /** Attach to a bus region created by another process. */
  static attach (sab) {
    if (!(sab instanceof SharedArrayBuffer)) {
      throw new TypeError('shrk-neo: attach() expects a SharedArrayBuffer')
    }
    const inst = Object.create(SharedBus.prototype)
    inst.sab = sab
    inst._init(false)
    return inst
  }

  _init (fresh) {
    const magic = new Int32Array(this.sab)[IDX_MAGIC]
    if (!fresh && magic !== MAGIC) {
      throw new Error('shrk-neo: not a shrk-neo bus buffer (wrong magic)')
    }
    if (fresh || !magic) {
      this.slots = this.slots || 64
      const tmp = new Int32Array(this.sab)
      tmp[IDX_MAGIC] = MAGIC
      tmp[IDX_SLOTS] = this.slots
    } else {
      this.slots = this.slots || new Int32Array(this.sab)[IDX_SLOTS]
    }

    const hdrLen = HDR_FIELDS + PER_MSG * this.slots
    if (hdrLen * 4 > this.sab.byteLength) {
      throw new Error('shrk-neo: shared buffer too small for its slot table')
    }
    this.ints = new Int32Array(this.sab, 0, hdrLen)
    this.u8 = new Uint8Array(this.sab)
    this.dataStart = hdrLen * 4
    this.capacity = Math.floor((this.sab.byteLength - this.dataStart) / this.slots)
    if (this.capacity < MIN_CAPACITY) {
      throw new Error(
        `shrk-neo: slot capacity is ${this.capacity} B (min ${MIN_CAPACITY} B); ` +
        'use a larger size or fewer slots'
      )
    }
  }

  /**
   * Publish a message. Any value is accepted: objects are v8-encoded;
   * Buffer / typed-array / ArrayBuffer values are stored raw.
   * Throws if all slots are busy (bounded queue) or the payload is bigger
   * than a slot.
   */
  send (value) {
    let payload
    let type
    if (Buffer.isBuffer(value)) {
      payload = value
      type = T_BUFFER
    } else if (value instanceof Uint8Array) {
      payload = new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
      type = T_BUFFER
    } else if (value instanceof ArrayBuffer) {
      payload = new Uint8Array(value)
      type = T_BUFFER
    } else {
      payload = v8.serialize(value)
      type = T_OBJECT
    }

    if (payload.length > this.capacity) {
      throw new RangeError(
        `shrk-neo: message too large (${payload.length} B > slot capacity ${this.capacity} B); ` +
        'create the bus with a larger size or fewer slots'
      )
    }

    for (let i = 0; i < this.slots; i++) {
      const base = msgBase(i)
      if (Atomics.compareExchange(this.ints, base + MSG_STATE, S_EMPTY, S_FILLING) === S_EMPTY) {
        const start = this.dataStart + i * this.capacity
        this.u8.set(payload, start)
        // Release ordering: the payload write happens-before the seq-cst
        // state store below, so a consumer that sees S_READY sees the bytes.
        this.ints[base + MSG_LEN] = payload.length
        this.ints[base + MSG_TYPE] = type
        Atomics.store(this.ints, base + MSG_STATE, S_READY)

        Atomics.add(this.ints, IDX_COUNTER, 1)
        Atomics.notify(this.ints, IDX_COUNTER, Number.MAX_SAFE_INTEGER)
        return this
      }
    }
    throw new Error(
      `shrk-neo: bus full (slots=${this.slots}); wait for a consumer or create more slots`
    )
  }

  /** Non-blocking receive. Returns `undefined` if the bus is empty. */
  tryReceive (options = {}) {
    const base = this._claim()
    if (base === -1) return undefined
    return this._consume(base, options)
  }

  /**
   * Blocking receive. Waits on the change counter (Atomics.wait — no busy
   * loop). Blocks the calling thread's event loop; prefer it inside a
   * worker, or use `receiveAsync` on the main thread.
   */
  receive (timeoutMs = Infinity, options = {}) {
    const deadline = timeoutMs === Infinity ? Infinity : Date.now() + timeoutMs
    for (;;) {
      const base = this._claim()
      if (base !== -1) return this._consume(base, options)
      if (deadline !== Infinity && Date.now() >= deadline) return undefined

      let chunk = WAIT_CHUNK
      if (deadline !== Infinity) chunk = Math.min(WAIT_CHUNK, Math.max(1, deadline - Date.now()))
      Atomics.wait(this.ints, IDX_COUNTER, Atomics.load(this.ints, IDX_COUNTER), chunk)
    }
  }

  /** Non-blocking receive for the main thread (Atomics.waitAsync). */
  async receiveAsync (timeoutMs = Infinity, options = {}) {
    if (typeof Atomics.waitAsync !== 'function') {
      throw new Error('shrk-neo: receiveAsync() requires Atomics.waitAsync (Node.js >= 16.17)')
    }
    const deadline = timeoutMs === Infinity ? Infinity : Date.now() + timeoutMs
    for (;;) {
      const base = this._claim()
      if (base !== -1) return this._consume(base, options)
      if (deadline !== Infinity && Date.now() >= deadline) return undefined

      const res = Atomics.waitAsync(this.ints, IDX_COUNTER, Atomics.load(this.ints, IDX_COUNTER))
      if (res.async) {
        const timeout =
          timeoutMs === Infinity
            ? new Promise(() => {})
            : sleep(Math.max(0, deadline - Date.now()))
        await Promise.race([res.value, timeout])
      }
    }
  }

  /** Number of messages currently queued or in flight. */
  get pending () {
    let n = 0
    for (let i = 0; i < this.slots; i++) {
      const s = Atomics.load(this.ints, msgBase(i) + MSG_STATE)
      if (s === S_READY || s === S_CLAIMED) n++
    }
    return n
  }

  get empty () {
    return this.pending === 0
  }

  /** Drop every queued message. */
  clear () {
    for (let i = 0; i < this.slots; i++) {
      const base = msgBase(i)
      this.ints[base + MSG_LEN] = 0
      this.ints[base + MSG_TYPE] = 0
      Atomics.store(this.ints, base + MSG_STATE, S_EMPTY)
    }
    Atomics.add(this.ints, IDX_COUNTER, 1)
    Atomics.notify(this.ints, IDX_COUNTER, Number.MAX_SAFE_INTEGER)
  }

  /** Atomically claim a READY slot for consumption. */
  _claim () {
    for (let i = 0; i < this.slots; i++) {
      const base = msgBase(i)
      if (Atomics.compareExchange(this.ints, base + MSG_STATE, S_READY, S_CLAIMED) === S_READY) {
        return base
      }
    }
    return -1
  }

  /** Read the claimed slot, release it, return the decoded value. */
  _consume (base, options) {
    const i = (base - HDR_FIELDS) / PER_MSG
    const vl = this.ints[base + MSG_LEN]
    const type = this.ints[base + MSG_TYPE]
    const start = this.dataStart + i * this.capacity

    // Snapshot bytes while we still own the slot, then release it.
    if (type === T_BUFFER) {
      if (options.copy) {
        const snapshot = Buffer.from(this.u8.subarray(start, start + vl))
        this._release(base)
        return snapshot
      }
      const view = new Uint8Array(this.sab, start, vl)
      this._release(base)
      // Zero-copy view straight into the shared region. Transient: a later
      // send() may reuse this slot's bytes — snapshot or process immediately.
      return Buffer.from(view.buffer, view.byteOffset, view.byteLength)
    }
    const snapshot = Buffer.from(this.u8.subarray(start, start + vl))
    this._release(base)
    return v8.deserialize(snapshot)
  }

  _release (base) {
    this.ints[base + MSG_LEN] = 0
    this.ints[base + MSG_TYPE] = 0
    Atomics.store(this.ints, base + MSG_STATE, S_EMPTY)
  }
}

module.exports = { SharedBus }
