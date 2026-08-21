'use strict'

/**
 * shrk-neo example — worker process.
 */

const { workerData } = require('node:worker_threads')
const { SharedMemory } = require('../index.js')

const mem = SharedMemory.attach(workerData.sab)

// Read straight from shared memory — no IPC, no serialization for buffers.
const config = mem.get('config')
const image = mem.get('image') // zero-copy Buffer viewing the SAB

console.log(`worker -> config.retries = ${config.retries}, labels.env = ${config.labels.get('env')}`)
console.log(`worker -> image bytes   = ${image.length}, direct SAB view = ${image.buffer === mem.sab}`)

// Write a result back into the shared region.
mem.set('result', { ok: true, checksum: image.length, by: 'worker' })
