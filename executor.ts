const { env } = Deno;

export type ExecutorOptions = {
  url: URL;
  name: string;
};

export class Executor {
  name: string;
  url: URL;
  workers: Worker[] = [];
  limit: number;
  qty: number;

  constructor({ name, url }: ExecutorOptions) {
    this.name = name;
    this.url = url;
    this.qty = env.get("EXECUTOR_WORKER_NUMBER")
      ? parseInt(env.get("EXECUTOR_WORKER_NUMBER")!)
      : 2;
    this.limit = env.get("EXECUTOR_WORKER_LIMIT")
      ? parseInt("EXECUTOR_WORKER_LIMIT")
      : 4;

    for (let i = 0; i < this.qty; i++) {
      this.increase();
    }
  }

  increase() {
    if (this.workers.length >= this.limit) {
      console.warn("Executor worker limit reached", this.limit);
      return;
    }
    const worker = new Worker(this.url.href, {
      type: "module",
      name: this.name + "-" + this.workers.length,
    });
    worker.addEventListener("message", (msg) => {
      this.workers.map((w) => {
        w.postMessage(msg.data);
      });
    });
    console.log("Spawned worker " + this.name + " " + this.workers.length);
    this.workers.push(worker);
  }

  decrease() {
    const w = this.workers.pop();
    if (w) {
      w.terminate();
    }
  }

  terminate() {
    for (const worker of this.workers) {
      worker.terminate();
    }
  }

  // deno-lint-ignore no-explicit-any
  async postMessage(msg: any, options?: StructuredSerializeOptions) {
    const worker = this.workers.shift();
    if (worker) {
      const res = await worker.postMessage(msg, options);
      this.workers.push(worker);
      return res;
    }
    throw new Error("No workers available");
  }

  listen(handler: (msg: unknown) => Promise<void>) {
    for (const worker of this.workers) {
      worker.onmessage = async (msg) => {
        await handler(msg.data);
      };
    }
  }
}
