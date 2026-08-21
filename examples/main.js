'use strict'

/**
 * shrk-neo example — master process.
 * Run: node examples/main.js
 */

const path = require('node:path')
const { Worker } = require('node:worker_threads')
const { SharedMemory, SharedBus, SharedCounter, SharedLock } = require('../index.js')

const mem = new SharedMemory({ size: 16 * 1024 * 1024, slots: 256 })
const bus = new SharedBus({ slots: 16 })
const counter = new SharedCounter({ initial: 0 })
const lock = new SharedLock()

const workerData = {
  sab: mem.sab,
  busSab: bus.sab,
  counterSab: counter.sab,
  lockSab: lock.sab
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

async function main () {
  // Spawn a couple of workers sharing the same memory regions.
  new Worker(path.join(__dirname, 'worker.js'), { workerData })
  new Worker(path.join(__dirname, 'worker.js'), { workerData })

  // Let the workers boot and block on mem.wait() / bus.receive().
  await sleep(300)

  // --- SharedMemory: publish a config object and a big zero-copy blob.
  mem.set('config', {
    retries: 3,
    backoff: [100, 200, 500],
    labels: new Map([['env', 'prod'], ['region', 'cn']]),
    createdAt: new Date()
  })
  mem.set('image', Buffer.alloc(8 * 1024 * 1024, 7))

  // --- SharedBus: hand each worker a job.
  bus.send({ job: 'render', frames: [1, 2, 3] })
  bus.send({ job: 'resize', width: 1920, height: 1080 })

  // --- SharedMemory: wait for the workers to publish their results.
  const results = await mem.waitAsync('results', 5000)

  console.log('main -> results:', results)
  console.log('main -> counter value:', counter.value, '(expect 2 — one inc per worker)')
  console.log('main -> shared image length:', mem.get('image').length, '| stats:', mem.stats())

  process.exit(0)
}

main()
