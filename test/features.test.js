'use strict'

const { test } = require('node:test')
const assert = require('node:assert')
const path = require('node:path')
const { Worker } = require('node:worker_threads')

const { SharedMemory, SharedBus, SharedCounter, SharedLock } = require('../index.js')

function runWorker (file, workerData) {
  return new Promise((resolve, reject) => {
    const w = new Worker(path.join(__dirname, 'fixtures', file), { workerData })
    w.once('message', resolve)
    w.once('error', reject)
  })
}

test('SharedMemory.stats() reports live keys and capacity', () => {
  const mem = new SharedMemory({ size: 1 << 20, slots: 8 })
  mem.set('a', 1)
  mem.set('b', 2)
  const s = mem.stats()
  assert.strictEqual(s.slots, 8)
  assert.strictEqual(s.liveKeys, 2)
  assert.strictEqual(s.freeSlots, 6)
  assert.ok(s.bytesUsed >= 0)
  assert.ok(s.bytesCapacity > 0)
  mem.clear()
  assert.strictEqual(mem.stats().liveKeys, 0)
  assert.strictEqual(mem.stats().bytesUsed, 0)
})

test('SharedCounter: inc/dec/reset, plus a worker bumping it', async () => {
  const counter = new SharedCounter({ initial: 3 })
  assert.strictEqual(counter.value, 3)
  assert.strictEqual(counter.inc(), 4)
  assert.strictEqual(counter.inc(2), 6)
  assert.strictEqual(counter.dec(1), 5)
  counter.reset(0)
  assert.strictEqual(counter.value, 0)

  await runWorker('counter-worker.js', { counterSab: counter.sab })
  assert.strictEqual(counter.value, 10)
})

test('SharedLock: mutual exclusion across two workers', async () => {
  const lock = new SharedLock()
  const counter = new SharedCounter()
  const N = 200

  await Promise.all([
    runWorker('lock-worker.js', { lockSab: lock.sab, counterSab: counter.sab, n: N }),
    runWorker('lock-worker.js', { lockSab: lock.sab, counterSab: counter.sab, n: N })
  ])

  // Every increment ran inside the lock, so no increment was lost.
  assert.strictEqual(counter.value, 2 * N)
  assert.strictEqual(lock.locked, false)
})

test('SharedLock: withLock and tryLock behave', () => {
  const lock = new SharedLock()
  assert.strictEqual(lock.locked, false)
  assert.ok(lock.tryLock())
  assert.strictEqual(lock.locked, true)
  assert.strictEqual(lock.tryLock(), false)
  lock.unlock()
  assert.strictEqual(lock.locked, false)
  const out = lock.withLock(() => 42)
  assert.strictEqual(out, 42)
  assert.strictEqual(lock.locked, false)
})

test('SharedBus: main sends, worker receives objects + zero-copy buffer', async () => {
  const bus = new SharedBus({ slots: 16 })
  bus.send({ a: 1, nested: { b: [1, 2] } })
  bus.send(Buffer.from('raw-bytes'))
  bus.send(new Map([['k', 'v']]))

  const out = await runWorker('bus-reader-worker.js', { busSab: bus.sab, count: 3 })

  assert.deepStrictEqual(out[0], { a: 1, nested: { b: [1, 2] } })
  assert.deepStrictEqual(out[1], { data: 'raw-bytes', direct: true })
  assert.ok(out[2] instanceof Map)
  assert.strictEqual(out[2].get('k'), 'v')
  assert.strictEqual(bus.pending, 0)
  assert.ok(bus.empty)
})

test('SharedBus: worker sends, main receives via tryReceive', async () => {
  const bus = new SharedBus({ slots: 16 })
  assert.strictEqual(bus.tryReceive(), undefined)

  await runWorker('bus-writer-worker.js', { busSab: bus.sab })

  const msg = bus.tryReceive()
  assert.deepStrictEqual(msg, { from: 'worker', n: 5 })
  assert.ok(bus.empty)
})

test('SharedBus: receiveAsync waits for a delayed producer', async () => {
  const bus = new SharedBus({ slots: 16 })
  const pending = runWorker('bus-writer-delayed-worker.js', { busSab: bus.sab })

  const msg = await bus.receiveAsync(3000)
  assert.deepStrictEqual(msg, { from: 'delayed', n: 9 })
  await pending
})

test('SharedBus: receive times out on an empty bus', async () => {
  const bus = new SharedBus({ slots: 16 })
  const started = Date.now()
  const v = await runWorker('bus-timeout-worker.js', { busSab: bus.sab })
  assert.strictEqual(v, 'timeout')
  assert.ok(Date.now() - started >= 120, 'must actually block for the timeout')
})

test('SharedBus: oversized messages throw, small ones pass', () => {
  const bus = new SharedBus({ size: 4096, slots: 4 })
  assert.ok(bus.capacity >= 64)
  assert.throws(() => bus.send(Buffer.alloc(bus.capacity + 1)), /message too large/)
  bus.send({ small: true })
  assert.deepStrictEqual(bus.tryReceive(), { small: true })
})

test('SharedBus: consumers grab distinct messages (no double delivery)', async () => {
  const bus = new SharedBus({ slots: 32 })
  for (let i = 0; i < 20; i++) bus.send({ i })

  // Two workers racing to consume from the same bus.
  const [a, b] = await Promise.all([
    runWorker('bus-reader-worker.js', { busSab: bus.sab, count: 10 }),
    runWorker('bus-reader-worker.js', { busSab: bus.sab, count: 10 })
  ])
  const seen = [...a, ...b].map((m) => m.i).sort((x, y) => x - y)
  assert.deepStrictEqual(seen, Array.from({ length: 20 }, (_, i) => i))
  assert.ok(bus.empty)
})
