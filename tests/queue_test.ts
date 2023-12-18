// deno-lint-ignore-file require-await no-explicit-any
import { assert } from "https://deno.land/std@0.114.0/_util/assert.ts";
import { Queue, TimeoutError } from "../mod.ts";

const { test } = Deno;

test({
  name: "Queue - enqueue / listen",
  fn: async () => {
    const queue = new Queue({ name: "test_queue" });
    const items = [];
    for (let i = 0; i < 10; i++) {
      items.push({
        type: "test",
        payload: ({ number: i }),
        timeout_ms: 100,
      });
    }
    const results = [];
    await queue.listen(async (data) => {
      results.push(data);
      return data.payload;
    });
    await queue.enqueue(...items);
    await new Promise((resolve) => setTimeout(resolve, 2000));
    await queue.close();
    await queue.storage._drop_tables();
    assert(results.length === 10);
  },
  sanitizeOps: false,
  sanitizeResources: false,
});

test({
  name: "Queue - enqueue / timeout / retry",
  fn: async () => {
    const queue = new Queue({ name: "test_queue", max_attempts: 2 });
    const items = [];
    for (let i = 0; i < 2; i++) {
      items.push({
        type: "test",
        payload: { number: i },
        timeout_ms: 100,
      });
    }
    const results: any[] = [];
    await queue.listen(async (data, onError) => {
      let abort = false;
      onError((e) => {
        assert(e instanceof TimeoutError);
        abort = true;
      });
      await new Promise((resolve) => setTimeout(resolve, 300));
      if (abort) return { error: "timeout" };
      results.push(data);
      return data.payload;
    });
    const ids = await queue.enqueue(...items);
    for (const id of ids) {
      const status = await queue.statusOf(id);
      assert(status.pending)
    }
    await new Promise((resolve) => setTimeout(resolve, 17500));
    for (const id of ids) {
      const status = await queue.statusOf(id);
      assert(status.fail)
    }
    await queue.close();
    await queue.storage._drop_tables();
    assert(results.length === 0);
  },
  sanitizeOps: false,
  sanitizeResources: false,
});

test({
  name: "Queue - enqueue / error / retry",
  fn: async () => {
    const queue = new Queue({ name: "test_queue", max_attempts: 2 });
    const items = [];
    for (let i = 0; i < 2; i++) {
      items.push({
        type: "test",
        payload: JSON.stringify({ number: i }),
        timeout_ms: 0,
      });
    }
    await queue.listen(async (_data, onError) => {
      let abort = false;
      onError((e) => {
        assert(e.message === "test");
        abort = true;
      });
      await new Promise((resolve) => setTimeout(resolve, 300));
      assert(abort === false);
      throw new Error("test");
    });
    await queue.enqueue(...items);
    await new Promise((resolve) => setTimeout(resolve, 17500));
    await queue.close();
    await queue.storage._drop_tables();
  },
  sanitizeOps: false,
  sanitizeResources: false,
});
