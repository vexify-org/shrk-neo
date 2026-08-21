'use strict'

/**
 * shrk-neo — SharedCounter: an atomic 32-bit counter shared across workers.
 *
 * powered by vexify
 */

const MAGIC = 0x53435452 // 'SCTR'
const IDX_MAGIC = 0
const IDX_VALUE = 1

class SharedCounter {
  /**
   * @param {object} [options]
   * @param {number} [options.initial=0] starting value (non-negative integer).
   */
  constructor (options = {}) {
    const initial = options.initial ?? 0
    if (!Number.isInteger(initial) || initial < 0) {
      throw new RangeError(`shrk-neo: 'initial' must be a non-negative integer (got ${initial})`)
    }
    this.sab = new SharedArrayBuffer(64)
    this._init(true)
    Atomics.store(this.ints, IDX_VALUE, initial)
  }

  /** Attach to a counter region created by another process. */
  static attach (sab) {
    if (!(sab instanceof SharedArrayBuffer)) {
      throw new TypeError('shrk-neo: attach() expects a SharedArrayBuffer')
    }
    const inst = Object.create(SharedCounter.prototype)
    inst.sab = sab
    inst._init(false)
    return inst
  }

  _init (fresh) {
    this.ints = new Int32Array(this.sab)
    if (fresh) {
      this.ints[IDX_MAGIC] = MAGIC
    } else if (this.ints[IDX_MAGIC] !== MAGIC) {
      throw new Error('shrk-neo: not a shrk-neo counter buffer (wrong magic)')
    }
  }

  /** Current value (atomic load). */
  get value () {
    return Atomics.load(this.ints, IDX_VALUE)
  }

  /** Increment by `delta` (default 1) and return the new value. */
  inc (delta = 1) {
    if (!Number.isInteger(delta)) {
      throw new TypeError(`shrk-neo: 'delta' must be an integer (got ${delta})`)
    }
    return Atomics.add(this.ints, IDX_VALUE, delta) + delta
  }

  /** Decrement by `delta` (default 1) and return the new value. */
  dec (delta = 1) {
    return this.inc(-delta)
  }

  /** Atomically store a new value (default 0). */
  reset (value = 0) {
    Atomics.store(this.ints, IDX_VALUE, value)
    return this
  }
}

module.exports = { SharedCounter }
