'use strict'

const { workerData, parentPort } = require('node:worker_threads')
const { SharedMemory } = require('../../index.js')

const mem = SharedMemory.attach(workerData.sab)

const msg = mem.get('msg')
const blob = mem.get('blob')
const directRead = blob.buffer === mem.sab ? 'shared' : 'copied'

parentPort.postMessage({ msg, blobStr: blob.toString(), directRead })
