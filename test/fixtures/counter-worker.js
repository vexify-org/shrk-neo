'use strict'

const { workerData, parentPort } = require('node:worker_threads')
const { SharedCounter } = require('../../index.js')

const counter = SharedCounter.attach(workerData.counterSab)
counter.inc(10)
parentPort.postMessage('done')
