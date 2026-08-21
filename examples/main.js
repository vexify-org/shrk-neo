'use strict'

/**
 * shrk-neo example — master process.
 * Run: node examples/main.js
 */

const path = require('node:path')
const { Worker } = require('node:worker_threads')
const { SharedMemory } = require('../index.js')

const mem = new SharedMemory({ size: 16 * 1024 * 1024, slots: 256 })

async function main () {
  // Spawn a couple of workers sharing the same memory region.
  new Worker(path.join(__dirname, 'worker.js'), { workerData: { sab: mem.sab } })
  new Worker(path.join(__dirname, 'worker.js'), { workerData: { sab: mem.sab } })

  // Publish a config object and a big zero-copy blob.
  mem.set('config', {
    retries: 3,
    backoff: [100, 200, 500],
    labels: new Map([['env', 'prod'], ['region', 'cn']]),
    createdAt: new Date()
  })
  mem.set('image', Buffer.alloc(8 * 1024 * 1024, 7))

  // Wait (non-blocking) for one of the workers to write a result back.
  const result = await mem.waitAsync('result', 5000)
  console.log('main -> result from worker:', result)
  console.log('main -> shared image length:', mem.get('image').length)

  process.exit(0)
}

main()
