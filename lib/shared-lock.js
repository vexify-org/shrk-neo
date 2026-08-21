'use strict'

/**
 * shrk-neo — SharedLock: a cross-worker mutex built on Atomics.
 *
 * Not owner-checked: any process that can see the lock can unlock() it.
 * lock() blocks the calling thread (fine inside workers); tryLock() never
 * blocks.
 *
 * powered by vexify
 */

const { threadId } = require('node:worker_threads')

const MAGIC = 0x534c4f4b // 'SLCK'
const IDX_MAGIC = 0
const IDX_STATE = 1
const IDX_OWNER = 2

const UNLOCKED = 0
const LOCKED = 1

const WAIT_CHUNK = 2_000_000_000

function ownerId () {
  // Best-effort diagnostics id (pid + worker thread id). Not used for
  // authorization — unlock() is not owner-checked.
  return process.pid * 65536 + threadId
}

class SharedLock {
  constructor () {
    this.sab = new SharedArrayBuffer(64)
    this._init(true)
  }

  /** Attach to a lock region created by another process. */
  static attach (sab) {
    if (!(sab instanceof SharedArrayBuffer)) {
      throw new TypeError('shrk-neo: attach() expects a SharedArrayBuffer')
    }
    const inst = Object.create(SharedLock.prototype)
    inst.sab = sab
    inst._init(false)
    return inst
  }

  _init (fresh) {
    this.ints = new Int32Array(this.sab)
    if (fresh) {
      this.ints[IDX_MAGIC] = MAGIC
      this.ints[IDX_STATE] = UNLOCKED
      this.ints[IDX_OWNER] = 0
    } else if (this.ints[IDX_MAGIC] !== MAGIC) {
      throw new Error('shrk-neo: not a shrk-neo lock buffer (wrong magic)')
    }
  }

  /** Acquire the lock, blocking until it is free. */
  lock () {
    const id = ownerId()
    for (;;) {
      if (Atomics.compareExchange(this.ints, IDX_STATE, UNLOCKED, LOCKED) === UNLOCKED) {
        Atomics.store(this.ints, IDX_OWNER, id)
        return
      }
      // Sleep while the state is LOCKED; the unlock() notify wakes us.
      Atomics.wait(this.ints, IDX_STATE, LOCKED, WAIT_CHUNK)
    }
  }

  /** Try to acquire the lock without blocking. Returns true on success. */
  tryLock () {
    if (Atomics.compareExchange(this.ints, IDX_STATE, UNLOCKED, LOCKED) === UNLOCKED) {
      Atomics.store(this.ints, IDX_OWNER, ownerId())
      return true
    }
    return false
  }

  /** Release the lock and wake one waiter. */
  unlock () {
    Atomics.store(this.ints, IDX_OWNER, 0)
    Atomics.store(this.ints, IDX_STATE, UNLOCKED)
    Atomics.notify(this.ints, IDX_STATE, 1)
  }

  /** Run `fn` while holding the lock, always releasing it. */
  withLock (fn) {
    this.lock()
    try {
      return fn()
    } finally {
      this.unlock()
    }
  }

  /** True if any process currently holds the lock. */
  get locked () {
    return Atomics.load(this.ints, IDX_STATE) === LOCKED
  }
}

module.exports = { SharedLock }
