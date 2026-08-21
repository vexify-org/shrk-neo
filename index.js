'use strict'

/**
 * shrk-neo
 *
 * Shared-memory primitives for Node.js worker processes — no JSON, no IPC,
 * no sockets. Data is read/written straight from SharedArrayBuffer regions:
 * Buffer / typed-array values travel zero-copy (no serialization at all),
 * arbitrary objects use V8's native serializer.
 *
 * - SharedMemory — key/value object store
 * - SharedBus    — lock-free message queue (mailbox)
 * - SharedCounter — atomic 32-bit counter
 * - SharedLock   — cross-worker mutex
 *
 * powered by vexify — Apache-2.0
 */

const { SharedMemory } = require('./lib/shared-memory')
const { SharedBus } = require('./lib/shared-bus')
const { SharedCounter } = require('./lib/shared-counter')
const { SharedLock } = require('./lib/shared-lock')

module.exports = { SharedMemory, SharedBus, SharedCounter, SharedLock }
