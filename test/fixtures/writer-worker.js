'use strict'

const { workerData, parentPort } = require('node:worker_threads')
const { SharedMemory } = require('../../index.js')

const mem = SharedMemory.attach(workerData.sab)

mem.set('from-worker', { hi: 'from worker', arr: [1, 2, 3] })
mem.set('wbuf', Buffer.from('buffer-from-worker'))

parentPort.postMessage('done')
