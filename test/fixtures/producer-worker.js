'use strict'

const { workerData, parentPort } = require('node:worker_threads')
const { SharedMemory } = require('../../index.js')

const mem = SharedMemory.attach(workerData.sab)
mem.set('late', { from: 'producer', n: 7 })
parentPort.postMessage('produced')
