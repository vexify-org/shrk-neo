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
