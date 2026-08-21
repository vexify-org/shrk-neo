'use strict'

const { test } = require('node:test')
const assert = require('node:assert')
const path = require('node:path')
const { Worker } = require('node:worker_threads')

const { SharedMemory } = require('../index.js')

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function runWorker (file, sab, extra) {
  return new Promise((resolve, reject) => {
    const w = new Worker(path.join(__dirname, 'fixtures', file), {
      workerData: { sab, ...extra }
    })
    w.once('message', resolve)
    w.once('error', reject)
  })
}

test('set/get round-trips objects, maps, dates and circular refs', () => {
  const mem = new SharedMemory({ size: 1 << 20 })

  const circular = { name: 'loop' }
  circular.self = circular

  mem.set('obj', { a: 1, b: 'two', nested: { c: [true, null] } })
  mem.set('map', new Map([['k', 'v'], [1, 2]]))
  mem.set('date', new Date('2026-08-21T00:00:00Z'))
  mem.set('circular', circular)

  assert.deepStrictEqual(mem.get('obj'), { a: 1, b: 'two', nested: { c: [true, null] } })
  const map = mem.get('map')
  assert.ok(map instanceof Map)
  assert.strictEqual(map.get('k'), 'v')
  assert.strictEqual(map.get(1), 2)
  assert.deepStrictEqual(mem.get('date'), new Date('2026-08-21T00:00:00Z'))
  assert.strictEqual(mem.get('circular').self.name, 'loop')
})

test('zero-copy buffers: get() returns a live view of the SAB, copy option snapshots', () => {
  const mem = new SharedMemory({ size: 1 << 20 })
  const original = Buffer.from([1, 2, 3, 4, 5])

  mem.set('blob', original)

  // caller's buffer is not aliased
  original[0] = 99
  const live = mem.get('blob')
  assert.strictEqual(live[0], 1)

  // zero-copy: returned Buffer views the shared region directly
  assert.strictEqual(live.buffer, mem.sab)
  assert.deepStrictEqual([...live], [1, 2, 3, 4, 5])

  // typed array values are also zero-copy
  mem.set('ta', new Uint8Array([9, 8, 7]))
  assert.strictEqual(mem.get('ta').buffer, mem.sab)

  // copy:true gives an independent snapshot
  const copy = mem.get('blob', { copy: true })
  assert.strictEqual(copy.buffer, copy.buffer)
  assert.notStrictEqual(copy.buffer, mem.sab)
  assert.deepStrictEqual([...copy], [1, 2, 3, 4, 5])
})

test('has / keys / delete / clear', () => {
  const mem = new SharedMemory({ size: 1 << 20 })
  mem.set('x', 1)
  mem.set('y', 2)
  assert.ok(mem.has('x'))
  assert.deepStrictEqual(mem.keys().sort(), ['x', 'y'])
  assert.strictEqual(mem.delete('x'), true)
  assert.strictEqual(mem.delete('x'), false)
  assert.ok(!mem.has('x'))
  assert.deepStrictEqual(mem.keys(), ['y'])

  mem.clear()
  assert.deepStrictEqual(mem.keys(), [])
  assert.strictEqual(mem.get('y'), undefined)
})

test('overwriting a key publishes new value, old slot freed', () => {
  const mem = new SharedMemory({ size: 1 << 20 })
  mem.set('k', 'first')
  mem.set('k', 'second')
  assert.strictEqual(mem.get('k'), 'second')
  assert.deepStrictEqual(mem.keys(), ['k'])
})

test('attach() rejects non-shrk-neo buffers and non-SABs', () => {
  assert.throws(() => SharedMemory.attach(new ArrayBuffer(1024)))
  assert.throws(() => SharedMemory.attach(new SharedArrayBuffer(1024)))
})

test('slots run out → error; clear() recovers', () => {
  const mem = new SharedMemory({ size: 1 << 20, slots: 2 })
  mem.set('a', 1)
  mem.set('b', 2)
  assert.throws(() => mem.set('c', 3), /no free slots/)
  mem.delete('a')
  mem.set('c', 3)
  assert.strictEqual(mem.get('c'), 3)
})

test('worker reads objects written by main directly from shared memory (no IPC)', async () => {
  const mem = new SharedMemory({ size: 1 << 20 })
  mem.set('msg', { hello: 'from main', n: 42, list: [1, 2, 3] })
  mem.set('blob', Buffer.from('zero-copy bytes'))

  const result = await runWorker('reader-worker.js', mem.sab)

  assert.deepStrictEqual(result.msg, { hello: 'from main', n: 42, list: [1, 2, 3] })
  assert.strictEqual(result.blobStr, 'zero-copy bytes')
  assert.strictEqual(result.directRead, 'shared', 'worker must see the SAB-backed view')
})

test('worker writes objects, main reads them back', async () => {
  const mem = new SharedMemory({ size: 1 << 20 })
  await runWorker('writer-worker.js', mem.sab)
  assert.deepStrictEqual(mem.get('from-worker'), { hi: 'from worker', arr: [1, 2, 3] })
  assert.strictEqual(mem.get('wbuf').toString(), 'buffer-from-worker')
})

test('wait() blocks in a worker until the value appears', async () => {
  const mem = new SharedMemory({ size: 1 << 20 })
  const done = runWorker('waiter-worker.js', mem.sab)
  await sleep(75) // ensure the worker is already waiting
  mem.set('late', { arrives: true })
  assert.deepStrictEqual(await done, { arrives: true })
})

test('wait() times out when the value never appears', async () => {
  const mem = new SharedMemory({ size: 1 << 20 })
  const done = runWorker('waiter-timeout-worker.js', mem.sab)
  assert.strictEqual(await done, 'timeout')
})

test('waitAsync() resolves on the main thread once the value appears', async () => {
  const mem = new SharedMemory({ size: 1 << 20 })
  const pending = mem.waitAsync('async-key')
  await sleep(50)
  mem.set('async-key', 1234)
  assert.strictEqual(await pending, 1234)
})

test('waitAsync() returns undefined on timeout', async () => {
  const mem = new SharedMemory({ size: 1 << 20 })
  assert.strictEqual(await mem.waitAsync('never', 60), undefined)
})

test('worker->worker: one worker produces, another consumes via wait()', async () => {
  const mem = new SharedMemory({ size: 1 << 20 })
  const consumer = runWorker('waiter-worker.js', mem.sab) // waits on 'late'
  await sleep(50)
  await runWorker('producer-worker.js', mem.sab) // sets 'late'
  assert.deepStrictEqual(await consumer, { from: 'producer', n: 7 })
})

test('many set/get round-trips stay consistent', () => {
  const mem = new SharedMemory({ size: 4 * 1024 * 1024, slots: 600 })
  for (let i = 0; i < 500; i++) {
    mem.set(`key-${i}`, { i, payload: `value-${i}` })
  }
  for (let i = 0; i < 500; i++) {
    const v = mem.get(`key-${i}`)
    assert.strictEqual(v.i, i)
    assert.strictEqual(v.payload, `value-${i}`)
  }
  mem.clear()
  assert.strictEqual(mem.keys().length, 0)
})
