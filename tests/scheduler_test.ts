// deno-lint-ignore-file no-explicit-any
import { assert } from "https://deno.land/std@0.114.0/_util/assert.ts";
import { load } from "https://deno.land/std@0.208.0/dotenv/mod.ts";
import { delay } from "https://deno.land/std@0.114.0/async/delay.ts";
import { scheduler } from "../mod.ts";

await load({ export: true });
const { test } = Deno;

test({
  name: "scheduler",
  fn: async (t) => {
    const instance = await scheduler.setup();
    await scheduler.start(0);

    await t.step("tasks / cron 1s - delay 5s", async () => {
      const results = [];
      scheduler.register("test", async (ctx) => {
        results.push(ctx);
        await delay(1000);
      });
      const task = {
        type: "test",
        pattern: "* * * * * *",
        description: "test task",
        context: { contextData: "test" },
      };
      await scheduler.schedule(task);
      await delay(5000);
      await scheduler.unschedule(task);
      scheduler.unregister("test");
      assert(results.length === 5);
    });

    await t.step("tasks / once / cron 1s - delay 5s", async () => {
      const results = [];
      scheduler.register("test", async (ctx) => {
        results.push(ctx);
        await delay(1000);
      });
      const task = {
        type: "test",
        pattern: "* * * * * *",
        description: "test task",
        context: { contextData: "test" },
        once: true,
      };
      await scheduler.schedule(task);
      await delay(5000);
      await scheduler.unschedule(task);
      scheduler.unregister("test");
      assert(results.length === 1);
    });

    await t.step("tasks / expire in 2s  / cron 1s - delay 5s", async () => {
      const results: any = [];
      scheduler.register("test", async (ctx) => {
        results.push(ctx);
        await delay(1000);
      });
      const task = {
        type: "test",
        pattern: "* * * * * *",
        description: "test task",
        context: { contextData: "test" },
        expires_at: new Date(Date.now() + 1990),
      };
      await scheduler.schedule(task);
      await delay(5000);
      await scheduler.unschedule(task);
      scheduler.unregister("test");
      assert(results.length === 2);
    });

    scheduler.stop();
    await instance.storage.drop_table();
  },
  sanitizeOps: false,
  sanitizeResources: false,
});
