# Que

A fault-tolerant PostgreSQL backed scheduler and message queue for Deno.

- No dependencies
- Cron pattern support + expire & once
- Scales horisontaly
- Scales vertically

**Motivation:**

I needed a queue that only uses postgres as a backend and works with Deno so I
made one.

## Settings

Required environment variables:

```bash
DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:54322/queues
```

## Scheduler basic usage

The scheduler executes functions for the task `type` on cron schedule.

```typescript
import { scheduler } from "https://deno.land/x/que/mod.ts";

await scheduler.start();

scheduler.register('fetch', async (ctx, onError) => {
  onError((e) => {
    console.error(e)
  })
  await fetch(ctx.url)
})

// fetch some resource every second
const fetchEverySec = {
  type: "fetch",
  pattern: "* * * * * *",
  description: "fetch url",
  context: { url: "http://google.com" },
};

await scheduler.schedule(fetchEverySec);
// the next line doesn't have effect, 
// because the task is the same
await scheduler.schedule(fetchEverySec);

```

Execute once at midnight, and deactivate the task:

```typescript
const executeOnce = {
  type: "fetch",
  pattern: "0 0 * * *",
  description: "fetch url",
  context: { url: "http://google.com" },
  once: true
};
await scheduler.schedule(executeOnce);
```

Execute every second for one hour:

```typescript
const expireInHour = {
  type: "fetch",
  pattern: "* * * * * *",
  description: "fetch url",
  context: { url: "http://google.com" },
  expire_at: new Date(new Date().getTime() + 1000 * 60 * 60)
};
await scheduler.schedule(expireInHour);
```

Unschedule the tasks:

```typescript
await scheduler.unschedule(fetchEverySec);
await scheduler.unschedule(executeOnce);
await scheduler.unschedule(expireInHour);
```

## Queue basic usage

The queue handles messages in recieving order as soon as possible.

```typescript
import { Queue } from "https://deno.land/x/que/mod.ts";
import type { TimeoutError } from "https://deno.land/x/que/mod.ts";

const que = new Queue({
  name: "que-uniq-name",
});

await que.listen(async (msg, onError) => {
  const some = new Some();
  onError((e) => {
    // we can abort
    if (e instanceof TimeoutError) {
      some.abort();
    }
  });
  // handle msg
  await some.send(
    msg.type,
    msg.uuid,
    msg.payload,
  );
});

await queue.enqueue({
  type: "email",
  payload: {
    to: "to@test.moc",
    from: "from@test.moc",
    body: "Message",
  },
});
```

### Queue methods

The Queue methods are self-descriptive. The constructor options requires
providing the `name`

```typescript
import { Queue } from 'https://deno.land/x/que/mod.ts'

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

_name_ - Required. Unique queue name.

_max_attempts_ - Maximum attempts made after failing

_batch_size_ - Number of the items one instance takes to process at a tick, the
default is `2`. You can use it in pair with `throttle` to control RPS.

_throttle_ - Default is 0. This option controls queque tick timeout. Normally
used on a master queue when you need to control RPS.

_storage_ - A custom storage implementation.

## Vertical scaling

When running on single machine sometimes it makes sence to increase number of
workers to speed up the queue processing. This solution provides an `Executor`
class which is responsible for worker sync and managemet. The workers spawned
through the `Executor` will sync `pause` and `close` states.

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

The queue workers can work on any machines as long as utilizing one database.
The only thing to keep in mind is that the instances are not going to syncronize
`pause` and `close` states.

## Execution order

The queue handles messages in recieving order, however the _execution order_ is
only guarantied when utilizing a single instance. If you need the _execution
order_ in tact when scaling the workers - it is possible, relate to the known
patterns and techniques.
