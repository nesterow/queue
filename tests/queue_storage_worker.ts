import { load } from "https://deno.land/std@0.208.0/dotenv/mod.ts";
import { delay } from "https://deno.land/std@0.114.0/async/delay.ts";
import QueueStorage from "../storage.ts";

await load({ export: true });

const storage = new QueueStorage("test_storage");
await storage.initialize();
await delay(700); // let the other workers start

console.log("Worker started", self.name);
let data = await storage.select(1);
while (data.length > 0) {
  console.log(
    self.name,
    "received =",
    data.map((d) => (d.payload as Record<string, string>).number).join(", "),
  );
  // @ts-ignore: no index signature
  self.postMessage(data);
  data = await storage.select(1);
}
