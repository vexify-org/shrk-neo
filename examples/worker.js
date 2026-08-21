'use strict'

/**
 * shrk-neo example — worker process.
 */

const { workerData } = require('node:worker_threads')
const { SharedMemory, SharedBus, SharedCounter, SharedLock } = require('../index.js')

const mem = SharedMemory.attach(workerData.sab)
const bus = SharedBus.attach(workerData.busSab)
const counter = SharedCounter.attach(workerData.counterSab)
const lock = SharedLock.attach(workerData.lockSab)

// --- SharedMemory: wait for the config/image, then read straight from
// shared memory (zero-copy buffer for the image).
const config = mem.wait('config', 5000)
const image = mem.wait('image', 5000) // zero-copy Buffer viewing the SAB
console.log(`worker -> config.retries = ${config.retries}, labels.env = ${config.labels.get('env')}`)
console.log(`worker -> image bytes = ${image.length}, direct SAB view = ${image.buffer === mem.sab}`)

// --- SharedLock + SharedCounter: exclusive critical section.
lock.lock()
counter.inc(1)
lock.unlock()

// --- SharedBus: receive a job.
const job = bus.receive(5000)
console.log(`worker -> got job ${job.job}`)

// --- SharedLock + SharedMemory: append the result into the shared store.
lock.lock()
const results = mem.get('results') || []
results.push({ done: job.job, checksum: image.length })
mem.set('results', results)
lock.unlock()
