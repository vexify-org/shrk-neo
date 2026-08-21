'use strict'

const { workerData, parentPort } = require('node:worker_threads')
const { SharedBus } = require('../../index.js')

const bus = SharedBus.attach(workerData.busSab)
bus.send({ from: 'worker', n: 5 })
parentPort.postMessage('done')
