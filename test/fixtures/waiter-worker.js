'use strict'

const { workerData, parentPort } = require('node:worker_threads')
const { SharedMemory } = require('../../index.js')

const mem = SharedMemory.attach(workerData.sab)
const value = mem.wait('late', 5000)
parentPort.postMessage(value)
