'use strict'

/**
 * shrk-neo — shared-memory object passing for Node.js worker processes.
 *
 * One process allocates a SharedArrayBuffer, every other process attaches to
 * the same buffer. Data is read/written directly on that shared region:
 * no JSON, no IPC, no sockets. Buffer / typed-array values travel fully
 * zero-copy (no serialization at all); arbitrary JS objects are encoded with
 * V8's native serializer (v8.serialize), which is far faster than JSON and
 * handles Buffer, Map, Set, Date, typed arrays and circular references.
 *
 * powered by vexify
 */

const v8 = require('node:v8')
const { Buffer } = require('node:buffer')

const MAGIC = 0x5348524b // 'SHRK'

// slot states
const S_EMPTY = 0
const S_WRITING = 1
const S_READY = 2

// value types
const T_OBJECT = 0 // payload was produced by v8.serialize()
const T_BUFFER = 1 // payload is a raw byte slice (zero-copy, no serialization)

// header layout (int32 view)
const IDX_MAGIC = 0
const IDX_SLOTS = 1
const IDX_COUNTER = 2 // bumped on every mutation (used by wait/waitAsync)
const IDX_CURSOR = 3 // bump-allocator high-water mark (byte offset from dataStart)
const HDR_FIELDS = 4

// per-slot layout (int32 view)
const SLOT_STATE = 0
const SLOT_KEY_OFF = 1
const SLOT_KEY_LEN = 2
const SLOT_VAL_OFF = 3
const SLOT_VAL_LEN = 4
const SLOT_TYPE = 5
const PER_SLOT = 6

// Atomics.wait only accepts timeouts < 2^31 ms; use a large chunk.
const WAIT_CHUNK = 2_000_000_000

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function slotBase (i) {
  return HDR_FIELDS + i * PER_SLOT
}

class SharedMemory {
  /**
   * Create a new shared memory region. Call this once in the process that
   * spawns the workers, then pass `mem.sab` to each worker (e.g. via
   * `workerData`) and have them call `SharedMemory.attach(sab)`.
   *
   * @param {object} [options]
   * @param {number} [options.size=4*1024*1024] total size in bytes of the
   *   SharedArrayBuffer (header + data region).
   * @param {number} [options.slots=64] number of key slots (max live keys).
   */
  constructor (options = {}) {
    const size = options.size ?? 4 * 1024 * 1024
    const slots = options.slots ?? 64

    if (!Number.isInteger(size) || size < 4096) {
      throw new RangeError(
        `shrk-neo: 'size' must be an integer >= 4096 (got ${size})`
      )
    }
    if (!Number.isInteger(slots) || slots < 1 || slots > 100000) {
      throw new RangeError(
        `shrk-neo: 'slots' must be an integer in [1, 100000] (got ${slots})`
      )
    }

    this.slots = slots
    this.sab = new SharedArrayBuffer(size)
    this._init(true)
  }

  /**
   * Attach to a SharedArrayBuffer that was created by another process
   * (worker thread). No allocation happens — every read/write goes straight
   * to the shared region.
   */
  static attach (sab) {
    if (!(sab instanceof SharedArrayBuffer)) {
      throw new TypeError(
        'shrk-neo: attach() expects a SharedArrayBuffer'
      )
    }
    const inst = Object.create(SharedMemory.prototype)
    inst.sab = sab
    inst._init(false)
    return inst
  }

  _init (fresh) {
    const magic = new Int32Array(this.sab)[IDX_MAGIC]
    if (!fresh && magic !== MAGIC) {
      throw new Error(
        'shrk-neo: not a shrk-neo shared memory buffer (wrong magic)'
      )
    }
    if (fresh || !magic) {
      // The creator may attach() a SAB it just created; treat that as init too.
      this.slots = this.slots || 64
      const tmp = new Int32Array(this.sab)
      tmp[IDX_MAGIC] = MAGIC
      tmp[IDX_SLOTS] = this.slots
    } else {
      this.slots = this.slots || new Int32Array(this.sab)[IDX_SLOTS]
    }

    const hdrLen = HDR_FIELDS + PER_SLOT * this.slots
    if (hdrLen * 4 > this.sab.byteLength) {
      throw new Error('shrk-neo: shared buffer too small for its slot table')
    }
    this.ints = new Int32Array(this.sab, 0, hdrLen)
    this.u8 = new Uint8Array(this.sab)
    this.dataStart = hdrLen * 4
  }

  _keyBuf (key) {
    if (typeof key === 'string') return Buffer.from(key, 'utf8')
    if (Buffer.isBuffer(key)) return key
    return Buffer.from(String(key), 'utf8')
  }

  /** Scan the slot table for a ready slot whose key bytes match. */
  _find (keyBuf) {
    for (let i = 0; i < this.slots; i++) {
      const base = slotBase(i)
      if (Atomics.load(this.ints, base + SLOT_STATE) !== S_READY) continue
      const kl = this.ints[base + SLOT_KEY_LEN]
      if (kl !== keyBuf.length) continue
      const start = this.dataStart + this.ints[base + SLOT_KEY_OFF]
      let eq = true
      for (let j = 0; j < kl; j++) {
        if (this.u8[start + j] !== keyBuf[j]) {
          eq = false
          break
        }
      }
      if (eq) return base
    }
    return -1
  }

  /** Claim a free slot (compare-exchange EMPTY -> WRITING). */
  _allocSlot () {
    for (let i = 0; i < this.slots; i++) {
      const base = slotBase(i)
      if (
        Atomics.compareExchange(this.ints, base + SLOT_STATE, S_EMPTY, S_WRITING) ===
        S_EMPTY
      ) {
        return base
      }
    }
    throw new Error(
      `shrk-neo: no free slots (slots=${this.slots}); delete() keys or grow the pool`
    )
  }

  _publish (base, keyBuf, payload, type) {
    const need = keyBuf.length + payload.length
    // Atomic bump allocator: concurrent writers never overlap.
    const cursor = Atomics.add(this.ints, IDX_CURSOR, need)
    if (cursor + need > this.sab.byteLength - this.dataStart) {
      Atomics.add(this.ints, IDX_CURSOR, -need) // roll back
      throw new Error(
        `shrk-neo: shared memory exhausted (needs ${need} more bytes); ` +
        'clear() it or create a larger region'
      )
    }

    const start = this.dataStart + cursor
    this.u8.set(keyBuf, start)
    this.u8.set(payload, start + keyBuf.length)

    // Release ordering: these plain stores happen-before the seq-cst store
    // below, so a reader that observes S_READY also observes the fields.
    this.ints[base + SLOT_KEY_OFF] = cursor
    this.ints[base + SLOT_KEY_LEN] = keyBuf.length
    this.ints[base + SLOT_VAL_OFF] = cursor + keyBuf.length
    this.ints[base + SLOT_VAL_LEN] = payload.length
    this.ints[base + SLOT_TYPE] = type
    Atomics.store(this.ints, base + SLOT_STATE, S_READY)

    this._bumpCounter()
  }

  _bumpCounter () {
    Atomics.add(this.ints, IDX_COUNTER, 1)
    Atomics.notify(this.ints, IDX_COUNTER, Number.MAX_SAFE_INTEGER)
  }

  /**
   * Write `value` under `key`.
   * - Buffer / typed-array / ArrayBuffer values are stored raw (T_BUFFER):
   *   readers get them back zero-copy, with no serialization.
   * - Any other value is encoded with v8.serialize() (fast native, no JSON).
   *
   * Replacing a key writes the new value into a fresh slot first, then frees
   * the old one, so readers never observe a half-written value.
   */
  set (key, value) {
    const keyBuf = this._keyBuf(key)

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

    const base = this._allocSlot()
    try {
      this._publish(base, keyBuf, payload, type)
    } catch (err) {
      Atomics.store(this.ints, base + SLOT_STATE, S_EMPTY)
      throw err
    }

    // Free any previous value for the same key.
    const old = this._find(keyBuf)
    if (old !== -1 && old !== base) {
      this._freeSlot(old)
    }
    return this
  }

  /**
   * Read `key` straight from the shared region.
   * - Buffer values: returns a Buffer that views the SharedArrayBuffer
   *   directly (zero-copy, no serialization). Pass `{ copy: true }` for an
   *   independent snapshot.
   * - Other values: v8.deserialize() of the shared bytes.
   * Returns `undefined` if the key does not exist.
   */
  get (key, options = {}) {
    const keyBuf = this._keyBuf(key)
    for (let attempt = 0; attempt < 4; attempt++) {
      const base = this._find(keyBuf)
      if (base === -1) return undefined
      if (Atomics.load(this.ints, base + SLOT_STATE) !== S_READY) return undefined

      const vl = this.ints[base + SLOT_VAL_LEN]
      const vo = this.ints[base + SLOT_VAL_OFF]
      const type = this.ints[base + SLOT_TYPE]
      // Guard against a concurrent delete/realloc tearing the fields above.
      if (Atomics.load(this.ints, base + SLOT_STATE) !== S_READY) continue

      const start = this.dataStart + vo
      if (type === T_BUFFER) {
        const view = new Uint8Array(this.sab, start, vl)
        if (options.copy) return Buffer.from(view)
        return Buffer.from(view.buffer, view.byteOffset, view.byteLength)
      }
      const snapshot = Buffer.from(this.u8.subarray(start, start + vl))
      return v8.deserialize(snapshot)
    }
    return undefined
  }

  has (key) {
    return this._find(this._keyBuf(key)) !== -1
  }

  /** List all currently stored keys. */
  keys () {
    const out = []
    for (let i = 0; i < this.slots; i++) {
      const base = slotBase(i)
      if (Atomics.load(this.ints, base + SLOT_STATE) !== S_READY) continue
      const ko = this.ints[base + SLOT_KEY_OFF]
      const kl = this.ints[base + SLOT_KEY_LEN]
      out.push(
        Buffer.from(this.u8.subarray(this.dataStart + ko, this.dataStart + ko + kl)).toString('utf8')
      )
    }
    return out
  }

  /** Remove `key`. Returns true if it existed. */
  delete (key) {
    const base = this._find(this._keyBuf(key))
    if (base === -1) return false
    this._freeSlot(base)
    return true
  }

  /** Free the slot's key + value bytes (storage space is reclaimed on clear). */
  _freeSlot (base) {
    this.ints[base + SLOT_KEY_OFF] = 0
    this.ints[base + SLOT_KEY_LEN] = 0
    this.ints[base + SLOT_VAL_OFF] = 0
    this.ints[base + SLOT_VAL_LEN] = 0
    this.ints[base + SLOT_TYPE] = 0
    Atomics.store(this.ints, base + SLOT_STATE, S_EMPTY)
    this._bumpCounter()
  }

  /** Remove every key and reset the allocator (reclaims all space). */
  clear () {
    for (let i = 0; i < this.slots; i++) {
      const base = slotBase(i)
      if (Atomics.load(this.ints, base + SLOT_STATE) === S_EMPTY) continue
      this.ints[base + SLOT_KEY_OFF] = 0
      this.ints[base + SLOT_KEY_LEN] = 0
      this.ints[base + SLOT_VAL_OFF] = 0
      this.ints[base + SLOT_VAL_LEN] = 0
      this.ints[base + SLOT_TYPE] = 0
      Atomics.store(this.ints, base + SLOT_STATE, S_EMPTY)
    }
    Atomics.store(this.ints, IDX_CURSOR, 0)
    this._bumpCounter()
  }

  /**
   * Block until `key` has a value, then return it (like get()).
   * Efficient: sleeps on the change counter via Atomics.wait — no busy loop.
   * BLOCKS the calling thread's event loop; prefer using it inside a worker
   * (or use `waitAsync` on the main thread).
   *
   * @param {string} key
   * @param {number} [timeoutMs=Infinity] 0 means "check once".
   */
  wait (key, timeoutMs = Infinity) {
    const keyBuf = this._keyBuf(key)
    const deadline = timeoutMs === Infinity ? Infinity : Date.now() + timeoutMs
    let last = Atomics.load(this.ints, IDX_COUNTER)
    for (;;) {
      if (this._find(keyBuf) !== -1) return this.get(key)
      if (deadline !== Infinity && Date.now() >= deadline) return undefined

      let chunk = WAIT_CHUNK
      if (deadline !== Infinity) {
        chunk = Math.min(WAIT_CHUNK, Math.max(1, deadline - Date.now()))
      }
      Atomics.wait(this.ints, IDX_COUNTER, last, chunk)
      last = Atomics.load(this.ints, IDX_COUNTER)
    }
  }

  /**
   * Non-blocking version of wait() for the main thread. Returns a Promise
   * that resolves with the value once `key` is set (or `undefined` on
   * timeout). Requires Atomics.waitAsync (Node.js >= 16.17).
   */
  async waitAsync (key, timeoutMs = Infinity) {
    if (typeof Atomics.waitAsync !== 'function') {
      throw new Error('shrk-neo: waitAsync() requires Atomics.waitAsync (Node.js >= 16.17)')
    }
    const keyBuf = this._keyBuf(key)
    const deadline = timeoutMs === Infinity ? Infinity : Date.now() + timeoutMs
    let last = Atomics.load(this.ints, IDX_COUNTER)
    for (;;) {
      if (this._find(keyBuf) !== -1) return this.get(key)
      if (deadline !== Infinity && Date.now() >= deadline) return undefined

      const res = Atomics.waitAsync(this.ints, IDX_COUNTER, last)
      if (res.async) {
        const timeout =
          timeoutMs === Infinity
            ? new Promise(() => {})
            : sleep(Math.max(0, deadline - Date.now()))
        await Promise.race([res.value, timeout])
      }
      last = Atomics.load(this.ints, IDX_COUNTER)
    }
  }
}

module.exports = { SharedMemory }
