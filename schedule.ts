import { createHash } from "https://deno.land/std@0.91.0/hash/mod.ts";
import * as postgres from "https://deno.land/x/postgres@v0.14.0/mod.ts";
import { parseCronExpression } from "npm:cron-schedule@4.0.0";

const registry: Record<
  string,
  (
    ctx: Record<string, unknown>,
    onError?: (handler: (e: Error) => void) => void,
  ) => Promise<void>
> = {};

interface TaskObject {
  name?: string;
  type: string;
  context: Record<string, unknown> | string;
  pattern: string;
  description: string;
  next_run: Date;
  once?: boolean;
  expires_at?: Date | string;
  is_active?: boolean;
}

class Task {
  constructor(
    public name: string,
    public type: string,
    public pattern: string,
    public description?: string,
    public expires_at?: Date,
    public once = false,
    public next_run: Date = new Date(),
    public is_running = false,
    public is_active = true,
    public context: Record<string, unknown> = {},
  ) {}

  isReady(): boolean {
    return new Date(this.next_run) <= new Date();
  }

  async exec(): Promise<Date | false> {
    if (!this.isReady()) {
      return false;
    }
    let onError: (e: Error) => void = () => {};
    const errorhook = (cb: typeof onError) => {
      onError = cb;
    };
    try {
      await registry[this.type]?.(this.context, errorhook);
    } catch (e) {
      onError(e);
    }
    const cron = parseCronExpression(this.pattern);
    this.next_run = cron.getNextDate();
    return this.next_run;
  }
}

class Storage {
  static pool: postgres.Pool | undefined;
  static get client(): Promise<postgres.PoolClient> {
    if (!this.pool) {
      this.setup();
    }
    return this.pool!.connect();
  }
  static setup(db_url = Deno.env.get("DATABASE_URL")) {
    this.pool = new postgres.Pool(db_url, 5, true);
  }

  static md5(task: Partial<TaskObject>): string {
    const hash = createHash("md5");
    hash.update(
      JSON.stringify(task.context ?? "{}") + task.pattern + task.type +
        task.once +
        (task.expires_at),
    );
    return hash.toString();
  }

  static #sanitize(task: Partial<TaskObject>): TaskObject {
    if (!task.type) {
      throw new Error("Task type is required");
    }
    if (!task.pattern) {
      throw new Error("Task pattern is required");
    }
    return {
      name: task.name ?? this.md5(task),
      type: task.type,
      pattern: task.pattern,
      description: task.description ?? "",
      next_run: parseCronExpression(task.pattern).getNextDate(),
      once: task.once ?? false,
      expires_at: task.expires_at,
      is_active: task.is_active ?? true,
      context: JSON.stringify(task.context ?? {}),
    };
  }

  static async set(task: Partial<TaskObject>): Promise<void> {
    const t = this.#sanitize(task);
    const client = await this.client;
    try {
      await client.queryObject`
      INSERT INTO cron_d_tasks (
        name, 
        type, 
        context, 
        pattern, 
        description, 
        next_run, 
        once, 
        expires_at, 
        is_active
      )
      VALUES (
        ${t.name}, 
        ${t.type}, 
        ${t.context}, 
        ${t.pattern}, 
        ${t.description}, 
        ${t.next_run}, 
        ${t.once}, 
        ${t.expires_at}, 
        ${t.is_active}
      )
      ON CONFLICT (name) DO UPDATE SET 
        pattern = ${t.pattern}, 
        description = ${t.description},
        context = ${t.context},
        next_run = ${t.next_run},
        once = ${t.once},
        expires_at = ${t.expires_at},
        is_active = ${t.is_active}
      RETURNING *
      ;
    `;
    } finally {
      client.release();
    }
  }

  static async delete(taks: Partial<TaskObject>): Promise<void> {
    const { name } = this.#sanitize(taks);
    const client = await this.client;
    try {
      await client.queryObject`
      DELETE FROM cron_d_tasks WHERE name = ${name}
    `;
    } finally {
      client.release();
    }
  }

  static async commit(task: Task): Promise<void> {
    const client = await this.client;
    try {
      if (task.once) {
        await client.queryObject`
        UPDATE cron_d_tasks SET 
          is_active = FALSE,
          is_running = FALSE
        WHERE name = ${task.name}
      `;
        return;
      }
      await client.queryObject`
      UPDATE cron_d_tasks SET 
        is_running = FALSE, 
        next_run = ${task.next_run} 
      WHERE name = ${task.name} AND is_running = TRUE
    `;
    } finally {
      client.release();
    }
  }

  static async *select(): AsyncIterable<Task> {
    const client = await this.client;
    try {
      const result = await client.queryObject`
        UPDATE cron_d_tasks SET is_running = TRUE
        WHERE name IN (
          SELECT name FROM cron_d_tasks WHERE is_active = TRUE AND is_running = FALSE 
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
      `;
      for (const row of result.rows as Task[]) {
        yield new Task(
          row.name,
          row.type,
          row.pattern,
          row.description,
          row.expires_at,
          row.once,
          row.next_run,
          row.is_running,
          row.is_active,
          row.context,
        );
      }
    } finally {
      client.release();
    }
  }

  static async create_table(): ReturnType<postgres.PoolClient["queryObject"]> {
    const client = await this.client;
    try {
      return client.queryObject(`
        CREATE TABLE IF NOT EXISTS cron_d_tasks (
          id INT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
          name VARCHAR(255) NOT NULL,
          type VARCHAR(255) NOT NULL DEFAULT 'task',
          context JSONB NOT NULL DEFAULT '{}',
          pattern VARCHAR(255) NOT NULL,
          description TEXT,
          next_run TIMESTAMP NOT NULL,
          once BOOLEAN NOT NULL DEFAULT FALSE,
          expires_at TIMESTAMP,
          is_running BOOLEAN NOT NULL DEFAULT FALSE,
          is_active BOOLEAN NOT NULL DEFAULT TRUE
        );
        CREATE UNIQUE INDEX IF NOT EXISTS cron_d_tasks_name_idx ON cron_d_tasks (name);
      `);
    } finally {
      client.release();
    }
  }

  static async drop_table(): ReturnType<postgres.PoolClient["queryObject"]> {
    const client = await this.client;
    try {
      return client.queryObject(`
        DROP TABLE IF EXISTS cron_d_tasks;
      `);
    } finally {
      client.release();
    }
  }
}

class Cron {
  static instance: Cron;
  #timeout = 0;
  #is_initialized = false;
  #is_stop = false;

  constructor(
    public tick = 300,
  ) {
    if (Cron.instance) {
      return Cron.instance;
    }
    Cron.instance = this;
  }

  async initialize() {
    if (this.#is_initialized) {
      return;
    }
    await Storage.create_table();
    this.#is_initialized = true;
  }

  async listen(): Promise<void> {
    await this.initialize();
    if (this.#is_stop) {
      return;
    }
    for await (const task of Storage.select()) {
      if (this.#is_stop) {
        return;
      }
      if (task.expires_at && task.expires_at < new Date()) {
        await Storage.delete(task).catch(console.error);
        continue;
      }
      const next_run = await task.exec();
      if (next_run) {
        task.next_run = next_run;
      }
      await Storage.commit(task);
    }
    this.#timeout = setTimeout(() => this.listen(), this.tick);
  }

  stop(): void {
    clearTimeout(this.#timeout);
    this.#is_stop = true;
  }

  get storage() {
    return Storage;
  }
}

export const setup = (tick?: number) => {
  if (!Cron.instance) {
    return new Cron(tick);
  }
  Cron.instance.tick = tick ?? Cron.instance.tick;
  return Cron.instance;
};

export async function start(tick?: number): Promise<void> {
  if (!Cron.instance) {
    setup(tick);
  }
  Cron.instance.tick = tick ?? Cron.instance.tick;
  await Cron.instance.listen();
}

export function stop(): void {
  Cron.instance?.stop();
}

export function register(
  type: string,
  fn: (ctx: Record<string, unknown>) => Promise<void>,
): void {
  registry[type] = fn;
}

export function unregister(type: string): void {
  delete registry[type];
}

export async function schedule(task: Partial<TaskObject>): Promise<void> {
  return await Storage.set(task);
}

export async function unschedule(task: Partial<TaskObject>): Promise<void> {
  return await Storage.delete(task);
}
