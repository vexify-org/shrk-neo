'use strict'

const { workerData, parentPort } = require('node:worker_threads')
const { SharedLock, SharedCounter } = require('../../index.js')

const lock = SharedLock.attach(workerData.lockSab)
const counter = SharedCounter.attach(workerData.counterSab)

for (let i = 0; i < workerData.n; i++) {
  lock.lock()
  counter.inc(1)
  lock.unlock()
}
parentPort.postMessage('done')
