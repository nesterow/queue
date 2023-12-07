import Storage from "./storage.ts";
import type { QueueItem, QueueMsg } from "./storage.ts";

export type ListenCallback = (
  data: QueueItem,
  errorHook: (callback: (e: Error) => void) => void,
) => Promise<QueueItem["result"]>;

export class TimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TimeoutError";
  }
}

export type QueueOptions = {
  name: string;
  max_attempts?: number;
  batch_size?: number;
  throttle?: number;
  storage?: Storage;
};

export default class Queue {
  #storage: Storage;
  #limit: number;
  #timeout?: number;
  #timeouts: Record<string, number> = {};
  #callback?: ListenCallback;
  #closed = false;
  #paused = false;
  #last_retry = new Date().getTime();
  #max_attempts = 5;
  #throttle = 0;
  constructor(
    { name, storage, max_attempts, batch_size, throttle }: QueueOptions,
  ) {
    this.#storage = storage ?? new Storage(name);
    this.#limit = batch_size ?? 2;
    this.#throttle = throttle ?? 0;
    this.#max_attempts = max_attempts ?? 5;
    self.addEventListener("message", (msg) => {
      this.#sync(msg as MessageEvent);
    });
  }

  async listen(callback: ListenCallback) {
    if (this.#callback) {
      throw new Error("Queue is already listening");
    }
    await this.#storage.initialize();
    this.#callback = callback;
    this.#loop();
  }

  async enqueue(...message: QueueMsg[]) {
    await this.#storage.initialize();
    return this.#storage.push(...message.map((m) => ({
      ...m,
      payload: m.payload instanceof Object
        ? JSON.stringify(m.payload)
        : m.payload,
    })));
  }

  close(): void {
    this.#closed = true;
    clearTimeout(this.#timeout);
    // TODO: check if worker
    if (self.name) {
      // @ts-ignore: no index signature
      self.postMessage({ 
        type: "close", 
        name: self.name 
      });
    }
  }

  pause(state = true): void {
    if (state) {
      this.#paused = state;
      clearTimeout(this.#timeout);
    } else {
      this.#paused = state;
      this.#loop();
    }
    if (self.name) {
      // @ts-ignore: no index signature
      self.postMessage({ 
        type: "pause", 
        name: 
        self.name, 
        payload: state 
      });
    }
  }

  async #loop() {
    await this.#storage.initialize();
    if (this.#closed) return;
    const data = await this.#storage.select(this.#limit);
    if (new Date().getTime() - this.#last_retry >= 5000) {
      this.#last_retry = new Date().getTime();
      this.#storage.retry(this.#max_attempts);
    }
    for (const item of data) {
      let errorhandler: (e: Error) => void = () => {};
      const errorhook = (callback: (e: Error) => void) => {
        errorhandler = callback;
      };
      try {
        let result: QueueItem["result"];
        if (item.timeout_ms) {
          result = await Promise.race([
            this.#callback!(item, errorhook),
            this.#timeout_task(item),
          ]).finally(() => {
            clearTimeout(this.#timeouts[item.uuid]);
            delete this.#timeouts[item.uuid];
          }) as QueueItem["result"];
        } else {
          result = await this.#callback!(item, errorhook);
        }
        await this.#storage.commit({ ...item, result });
      } catch (error) {
        const failed = await this.#storage.fail(item, error);
        if (failed.error_attempts >= this.#max_attempts) {
          console.error(
            `Task ${item.uuid} failed after ${failed.error_attempts} attempts`,
          );
        }
        errorhandler(error);
      }
    }
    if (this.#paused) return;
    this.#timeout = setTimeout(() => this.#loop(), this.#throttle);
  }

  async #timeout_task(task: QueueItem) {
    const { timeout_ms } = task;
    await new Promise((_, reject) => {
      this.#timeouts[task.uuid] = setTimeout(() => {
        delete this.#timeouts[task.uuid];
        reject(
          new TimeoutError(`Task ${task.uuid} timed out after ${timeout_ms}ms`),
        );
      }, timeout_ms);
    });
  }

  #sync(msg: MessageEvent) {
    if (self.name === msg.data.name) return;
    if (msg.data.type === "close") {
      this.close();
    }
    if (msg.data.type === "pause") {
      this.pause(msg.data.payload);
    }
  }

  get storage() {
    return this.#storage;
  }
}
