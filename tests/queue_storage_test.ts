import { load } from "https://deno.land/std@0.208.0/dotenv/mod.ts";
import { delay } from "https://deno.land/std@0.114.0/async/delay.ts";
import { assert } from "https://deno.land/std@0.114.0/_util/assert.ts";
import QueueStorage, { QueueMsg } from "../storage.ts";

await load({ export: true });
const { test } = Deno;

const storage = new QueueStorage("test_storage");
await storage._drop_tables();
await storage.initialize();

const createMessages = (count: number): QueueMsg[] => {
  const items: QueueMsg[] = [];
  for (let i = 0; i < count; i++) {
    items.push({
      type: "test",
      payload: JSON.stringify({ number: i }),
      timeout_ms: 100,
    });
  }
  return items;
};

test({
  name: "QueueStorage - pg skip locked / 3 workers",
  fn: async () => {
    const items = createMessages(10);
    await storage.push(...items);

    const messages = [];
    for (let i = 1; i < 3; i++) {
      const w = new Worker(
        new URL("./queue_storage_worker.ts", import.meta.url).href,
        { name: `worker-${i}`, type: "module" },
      );
      w.onmessage = (e) => {
        messages.push(e.data);
      };
    }
    await delay(1000);
    const data = await storage.select(10);
    assert(data.length === 0);
    assert(messages.length === 10);
    await delay(500);
    await storage._drop_tables();
  },
  sanitizeOps: false,
  sanitizeResources: false,
});
