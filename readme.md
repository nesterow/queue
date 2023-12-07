# Queue

A fault-tolerant PostgreSQL backed message queue for Deno.
- No dependencies
- Scales horisontaly
- Scales vertically

**Motivation:**

I needed a queue that only uses postgres as a backend and works with Deno, so I made it.

<br/>

**Performance:**

It performs as good as PostgreSQL and highly depends on its condition.
This solution is production-tested within a system that sends around 1k RPS to the queue. With 6 workers (processes) the peak memory usage was 1.5Gb. That's all I can say about performance for now - more testing is required.

## Settings

Required environment variables:

```bash
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:54322/queues
```

## Basic usage

The queue handles messages in recieving order as soon as possible.

```typescript
import { Queue } from './mod.ts'
import type { TimeoutError } from './mod.ts' 

const que = new Queue({
  name: "que-uniq-name"
})

await que.listen(async (msg, onError) => {
  const some = new Some()
  onError((e) => {
    // we can abort
    if (e instanceof TimeoutError) {
      some.abort()
    }
  });
  // handle msg
  await some.send(
    msg.type,
    msg.uuid,
    msg.payload
  )
});

await queue.enqueue({
  type: "email",
  payload: {
    to: "to@test.moc",
    from: "from@test.moc",
    body: "Message"
  }
});

```

### Queue methods

The Queue methods are self-descriptive. 
The constructor options requires providing the `name`

```typescript
import { Queue } from './mod.ts'

const que = new Queue({
  name: "name",
})

que.listen(callback: ListenCallback)
que.enqueue(..msgs: { type: string, payload: JsonValue, timeout_ms?: number })
que.pause(state: boolean)
que.close()
```

### Constructor options
```typescript
type QueueOptions = {
  name: string;
  max_attempts?: number;
  batch_size?: number;
  throttle?: number;
  storage?: Storage;
};
```

*name* - Required. Unique queue name.

*max_attempts* - Maximum attempts made after failing

*batch_size* - Number of the items one instance takes to process at a tick, the default is `2`. You can use it in pair with `throttle` to control RPS.

*throttle* - Default is 0. This option controls queque tick timeout. Normally used on a master queue when you need to control RPS.

*storage* - A custom storage implementation.


## Vertical scaling

When running on single machine sometimes it makes sence to increase number of workers to speed up the queue processing.
This solution provides an `Executor` class which is responsible for worker sync and managemet. 
The workers spawned through the `Executor` will sync `pause` and `close` states.

**Environment**

Following environment variables control the executor limits:

```bash
EXECUTOR_WORKER_NUMBER=2 # spawn workers
EXECUTOR_WORKER_LIMIT=4  # max workers
```

**Example**

```typescript
const exec = new Executor({
  url: new URL("./worker.ts", import.meta.url),
  name: "queue",
});
// exec.increase()
```

## Horizontal scaling

The queue workers can work on any machines as long as utilizing one database. The only thing to keep in mind is that the instances are not going to syncronize `pause` and `close` states.

## Execution order
The queue handles messages in recieving order, however the *execution order* is only guarantied when utilizing a single instance. 
If you need the  *execution order* in tact when scaling the workers - it is possible, relate to the known patterns and techniques.

