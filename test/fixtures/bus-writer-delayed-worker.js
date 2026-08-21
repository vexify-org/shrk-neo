'use strict'

const { workerData, parentPort } = require('node:worker_threads')
const { SharedBus } = require('../../index.js')

const bus = SharedBus.attach(workerData.busSab)
setTimeout(() => {
  bus.send({ from: 'delayed', n: 9 })
  parentPort.postMessage('sent')
}, 100)
