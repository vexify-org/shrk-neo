# shrk-neo

> Pass JS objects between Node.js worker processes straight from a
> `SharedArrayBuffer` — **no JSON, no IPC, no sockets.**
> Buffer / typed-array values travel **zero-copy, with no serialization at all**;
> arbitrary JS objects use V8's native serializer.
>
> powered by [vexify](https://github.com/vexify) · Apache-2.0

`shrk-neo` gives Node.js "multi-process" (worker thread) programs a real
shared heap. One process creates the region, every other process attaches to
the very same memory. Writers and readers touch the memory directly with
`Atomics`-guarded lock-free operations — there is no pipe, no socket, no
`postMessage` involved in moving the data.

It ships four primitives:

| primitive | purpose |
| --- | --- |
| `SharedMemory` | key/value object store (`set`/`get`/`wait`) |
| `SharedBus` | lock-free message queue / mailbox (`send`/`receive`) |
| `SharedCounter` | atomic 32-bit counter (`inc`/`dec`/`reset`) |
| `SharedLock` | cross-worker mutex (`lock`/`unlock`/`withLock`) |

## Why "no serialization"?

- **Buffer / typed-array / ArrayBuffer values** are copied once into the
  shared region and read back as live views of that memory. Zero copies, zero
  encoding — a true zero-copy, no-serialization path.
- **Arbitrary JS objects** cannot physically live outside a V8 heap, so they
  are encoded with `v8.serialize()` — the fastest native encoding Node has.
  It beats `JSON.stringify` by a wide margin and natively handles `Buffer`,
  `Map`, `Set`, `Date`, typed arrays, and circular references.

## Install

```sh
npm install shrk-neo
```

## Quick start

```js
// master.js — create the shared region, spawn workers
const { Worker } = require('node:worker_threads')
const { SharedMemory } = require('shrk-neo')

const mem = new SharedMemory({ size: 16 * 1024 * 1024, slots: 256 })

new Worker('./worker.js', { workerData: { sab: mem.sab } })

// Write objects from the main process…
mem.set('config', { retries: 3, backoff: [100, 200, 500], labels: new Map([['env', 'prod']]) })
mem.set('image', Buffer.alloc(1024 * 1024, 7)) // zero-copy blob
```

```js
// worker.js — attach, read straight from shared memory
const { workerData } = require('node:worker_threads')
const { SharedMemory } = require('shrk-neo')

const mem = SharedMemory.attach(workerData.sab)

const cfg = mem.get('config') // -> { retries: 3, backoff: [100,200,500], labels: Map{env->prod} }
const img = mem.get('image')  // -> Buffer that views the shared memory (zero-copy)

// …and write back the other way
mem.set('result', { ok: true, checksum: img.length })
```

That's it. The data never crosses an IPC channel — `worker.js` reads the
object bytes directly out of the region the master created.

## API

### `new SharedMemory(options?)`

Creates a new shared region. Only the process that spawns the workers calls
this. Exposes `.sab` (the `SharedArrayBuffer`) to hand to workers.

| option | default | description |
| --- | --- | --- |
| `size` | `4 * 1024 * 1024` | total size in bytes of the region (header + data) |
| `slots` | `64` | number of key slots — the max number of live keys |

### `SharedMemory.attach(sab)`

Attaches to a region created by another process. No allocation — reads/writes
go directly to the shared memory. Returns a new `SharedMemory` instance.

### `set(key, value) → this`

Writes `value` under `key`. Replacing a key never leaves readers seeing a
half-written value (new slot is published first, old one freed after).

### `get(key, options?) → value | undefined`

Reads `key` directly from the shared region.

- Buffer values return a `Buffer` that **views the SharedArrayBuffer**
  (zero-copy, no serialization). Pass `{ copy: true }` for an independent
  snapshot.
- Other values are returned via `v8.deserialize()`.

### `has(key) → boolean`

True if the key currently exists.

### `keys() → string[]`

All currently stored keys.

### `delete(key) → boolean`

Removes the key; returns whether it existed.

### `clear()`

Removes every key and resets the allocator (reclaims all space).

### `wait(key, timeoutMs?) → value | undefined`

Blocks until `key` has a value, then returns it (as `get`). Sleeps on the
change counter with `Atomics.wait` — no busy loop. Blocks the calling
thread's event loop, so prefer calling it inside a worker.

### `waitAsync(key, timeoutMs?) → Promise<value | undefined>`

Non-blocking `wait` for the main thread, built on `Atomics.waitAsync`
(Node.js ≥ 16.17).

### `stats() → { slots, liveKeys, freeSlots, bytesUsed, bytesCapacity }`

Usage stats for the region. `bytesUsed` is the allocator's high-water mark
since the last `clear()`.

## `SharedBus` — message queue

```js
const bus = new SharedBus({ size: 4 * 1024 * 1024, slots: 128 })
// main → workers
bus.send({ job: 'render', frames: [1, 2, 3] })
bus.send(Buffer.alloc(64 * 1024)) // raw bytes, zero-copy on receive
// …and from a worker:
bus.send({ result: 'ok' })
```

```js
// worker.js
const bus = SharedBus.attach(workerData.busSab)
const job = bus.receive(5000)      // blocking receive (use inside a worker)
const next = bus.tryReceive()      // never blocks, undefined if empty
const also = await bus.receiveAsync(5000) // non-blocking, for the main thread
```

### `new SharedBus(options?)`

| option | default | description |
| --- | --- | --- |
| `size` | `1 * 1024 * 1024` | total bytes of the region |
| `slots` | `64` | number of message slots |

Each slot owns a fixed `capacity = (size - header) / slots` bytes, so space is
reclaimed the instant a consumer claims a message. A payload bigger than
`capacity` throws (`message too large`).

### `send(value) → this`

Publishes a message. Objects are v8-encoded; Buffer / typed-array /
ArrayBuffer values are stored raw. Throws when every slot is busy (bounded
queue).

### `tryReceive(options?) → value | undefined`

Non-blocking receive; `undefined` when empty.

### `receive(timeoutMs?, options?) → value | undefined`

Blocking receive (Atomics.wait, no busy loop). Blocks the calling thread's
event loop — use it in workers, `receiveAsync` on the main thread.

### `receiveAsync(timeoutMs?, options?) → Promise<value | undefined>`

Main-thread-friendly blocking receive via `Atomics.waitAsync`.

### `pending` / `empty` / `clear()`

`pending` counts queued + in-flight messages; `empty` is `pending === 0`;
`clear()` drops everything.

Like `SharedMemory.get`, Buffer values are returned as zero-copy views by
default (`{ copy: true }` for a snapshot). Because a slot is freed as soon as
it is consumed, a zero-copy message's bytes may be reused by a later `send` —
process it promptly or use `copy: true`.

## `SharedCounter` — atomic counter

```js
const counter = new SharedCounter({ initial: 0 })       // main
// in a worker: SharedCounter.attach(workerData.counterSab)

counter.inc()    // +1, returns new value
counter.inc(5)   // +5, returns new value
counter.dec(2)   // -2, returns new value
counter.value    // current value (atomic load)
counter.reset(0) // store a new value
```

Useful for progress meters, per-worker credits, or aggregate statistics
across processes. `inc`/`dec` are `Atomics.add` — safe from many workers at
once.

## `SharedLock` — mutex

```js
const lock = new SharedLock()               // main
// in a worker: SharedLock.attach(workerData.lockSab)

lock.lock()
try {
  // critical section, exclusive across all workers
} finally {
  lock.unlock()
}

lock.withLock(() => { /* same, with auto-release */ })
lock.tryLock()   // true/false, never blocks
lock.locked      // is anyone holding it?
```

`lock()` blocks the calling thread via `Atomics.wait` (use inside workers).
It is *not* owner-checked — anyone with access can `unlock()`. Pair it with a
`SharedCounter` for classic producer/consumer critical sections.

## Semantics & caveats

- **Scope** — `SharedArrayBuffer` can only be shared between worker threads
  of the same process, which is Node's native "multi-process" model. Data does
  not cross OS-process boundaries (that would require IPC, which this package
  deliberately avoids).
- **Concurrency** — the store is lock-free for reads. The safe pattern is:
  writers own their keys, readers read. Concurrent `set`/`delete` racing with
  `get` on the *same* key may return the previous value; coordinate such
  writes yourself (e.g. one producer per key).
- **Memory reuse** — deleted values free their *slot* immediately; their
  bytes are reclaimed by the next `clear()`. If the region fills up `set`
  throws `shared memory exhausted` — call `clear()`, delete keys, or create a
  larger region.
- **Zero-copy buffers** are live views of shared memory. If a writer later
  overwrites the same key, a long-held zero-copy buffer may see its content
  change; use `{ copy: true }` or snapshot the bytes when that matters.

## Examples

```sh
node examples/main.js
```

## License

[Apache-2.0](./LICENSE) · powered by [vexify](https://github.com/vexify)
