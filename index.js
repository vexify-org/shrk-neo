'use strict'

/**
 * shrk-neo
 *
 * Pass JS objects between Node.js worker processes straight from a
 * SharedArrayBuffer — no JSON, no IPC, no sockets. Buffer / typed-array
 * values travel zero-copy with no serialization at all; arbitrary objects
 * use V8's native serializer.
 *
 * powered by vexify — Apache-2.0
 */

const { SharedMemory } = require('./lib/shared-memory')

module.exports = { SharedMemory }
