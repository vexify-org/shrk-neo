'use strict'

const { workerData, parentPort } = require('node:worker_threads')
const { SharedBus } = require('../../index.js')

const bus = SharedBus.attach(workerData.busSab)
const v = bus.receive(150)
parentPort.postMessage(v === undefined ? 'timeout' : v)
