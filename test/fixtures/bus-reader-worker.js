'use strict'

const { workerData, parentPort } = require('node:worker_threads')
const { SharedBus } = require('../../index.js')

const bus = SharedBus.attach(workerData.busSab)

const out = []
for (let i = 0; i < workerData.count; i++) {
  const v = bus.receive(2000)
  if (Buffer.isBuffer(v)) {
    // Report the bytes plus proof it was a direct SharedArrayBuffer view.
    out.push({ data: v.toString('utf8'), direct: v.buffer === bus.sab })
  } else {
    out.push(v)
  }
}
parentPort.postMessage(out)
