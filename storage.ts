import * as postgres from "https://deno.land/x/postgres@v0.14.0/mod.ts";
const { Pool } = postgres;

type JSONValue =
  | string
  | number
  | boolean
  | JSONObject
  | JSONObject[];

interface JSONObject {
  [key: string]: JSONValue;
}

export interface QueueItem {
  id?: number;
  uuid: string;
  type: string;
  payload: JSONObject | JSONValue;
  result: JSONObject | JSONValue;
  pending: boolean;
  timeout_ms: number;
  created_at: Date;
  updated_at: Date;
}

export type QueueMsg = Omit<
  QueueItem,
  "id" | "uuid" | "pending" | "result" | "created_at" | "updated_at"
>;

export interface FailQueueItem {
  id: number;
  uuid: string;
  type: string;
  payload: JSONObject;
  error_data: JSONObject;
  error_attempts: number;
  created_at: Date;
  updated_at: Date;
}

export interface QueueStorageStats {
  queued: number;
  done: number;
  fail: number;
  retry: number;
  created_at: Date;
  updated_at: Date;
}

export interface QueueStorage {
  name: string;
  pool: postgres.Pool;
  is_initialized: boolean;
  initialize(): Promise<void>;
  push(...items: QueueItem[]): Promise<void>;
  select(length?: number): Promise<QueueItem[]>;
  commit(...items: QueueItem[]): Promise<void>;
  fail(item: QueueItem, error_data: string): Promise<FailQueueItem>;
  _create_tables(): Promise<void>;
  _drop_tables(): Promise<void>;
  get _pending_table(): string;
  get _fail_table(): string;
  get _done_table(): string;
  get _stats_table(): string;
}

const ivname = (iv: string) => "'" + iv.replace(" ", "_") + "'";

export default class implements QueueStorage {
  is_initialized = false;
  timeseries = [
    "1 minute",
    "5 minutes",
    "30 minutes",
    "1 hour",
    "1 day",
    "1 week",
    "30 days",
    "60 days",
    "180 days",
  ];
  constructor(
    readonly name: string,
    readonly pool: postgres.Pool = new Pool(
      Deno.env.get("DATABASE_URL"),
      3,
      true,
    ),
  ) {}

  async initialize(): Promise<void> {
    if (this.is_initialized) return;
    await this._create_tables();
    this.is_initialized = true;
  }

  async push(...items: QueueMsg[]): Promise<void> {
    this._guard_initialized();
    const connection = await this.pool.connect();
    try {
      const now = new Date();
      await connection.queryObject(`
        INSERT INTO ${this._pending_table} (uuid, type, payload, pending, timeout_ms, created_at, updated_at)
        VALUES ${
        items.map((item) =>
          `('${crypto.randomUUID()}', '${item.type}', '${item.payload}', false, ${item.timeout_ms}, '${now.toISOString()}', '${now.toISOString()}')`
        ).join(", ")
      }
      `);
    } finally {
      connection.release();
    }
  }

  async select(length?: number): Promise<QueueItem[]> {
    this._guard_initialized();
    const connection = await this.pool.connect();
    try {
      const result = await connection.queryObject(`
        UPDATE ${this._pending_table}
        SET pending = true, updated_at = NOW()
        WHERE id IN (
          SELECT id FROM ${this._pending_table}
          WHERE pending = false
          ORDER BY id ASC
          LIMIT ${length ?? 1}
          FOR UPDATE SKIP LOCKED
        )
        RETURNING *
      `);
      return result.rows as QueueItem[];
    } finally {
      connection.release();
    }
  }

  async remove(items: QueueItem[]): Promise<void> {
    this._guard_initialized();
    const connection = await this.pool.connect();
    try {
      await connection.queryObject(`
        DELETE FROM ${this._pending_table}
        WHERE id IN (${items.map((item) => item.id).join(", ")})
      `);
    } finally {
      connection.release();
    }
  }

  async commit(...items: QueueItem[]): Promise<void> {
    this._guard_initialized();
    const connection = await this.pool.connect();
    const it = items.map((item) => {
      return {
        ...item,
        result: JSON.stringify(item.result),
        payload: JSON.stringify(item.payload),
      };
    });
    try {
      const now = new Date();
      await connection.queryObject(`
        INSERT INTO ${this._done_table} (uuid, type, payload, result, created_at)
        VALUES ${
        it.map((item) =>
          `('${item.uuid}', '${item.type}', '${item.payload}', '${item.result}', '${now.toISOString()}')`
        ).join(", ")
      };
        DELETE FROM ${this._pending_table} WHERE id IN (${
        items.map((item) => item.id).join(", ")
      });
        DELETE FROM ${this._fail_table} WHERE uuid IN (${
        items.map((item) => `'${item.uuid}'`).join(", ")
      });
      `);
    } finally {
      connection.release();
    }
  }

  async fail(item: QueueItem, error_data: string): Promise<FailQueueItem> {
    this._guard_initialized();
    const connection = await this.pool.connect();
    const payload = JSON.stringify(item.payload);
    try {
      const now = new Date();
      const result = await connection.queryObject(`
        INSERT INTO ${this._fail_table} (uuid, type, payload, error_data, error_attempts, created_at, updated_at)
        VALUES ('${item.uuid}', '${item.type}', '${payload}', '${error_data}', 1, '${now.toISOString()}', '${now.toISOString()}')
        ON CONFLICT (uuid)
        DO UPDATE SET
          error_attempts = ${this._fail_table}.error_attempts + 1,
          updated_at = NOW()
        RETURNING *
      `);
      const fail_item = result.rows[0] as FailQueueItem;
      return fail_item;
    } finally {
      connection.release();
    }
  }

  async retry(max_attempts = 4, batchSize = 10): Promise<void> {
    this._guard_initialized();
    const connection = await this.pool.connect();
    try {
      // update pending based on fail
      await connection.queryObject(`
        UPDATE ${this._pending_table}
        SET pending = false, updated_at = NOW()
        WHERE uuid IN (
          SELECT uuid FROM ${this._fail_table}
          WHERE error_attempts < ${max_attempts}
          ORDER BY id ASC
          LIMIT ${batchSize}
          FOR UPDATE SKIP LOCKED
        )
      `);
    } finally {
      connection.release();
    }
  }

  async now_stats(): Promise<QueueStorageStats> {
    this._guard_initialized();
    const connection = await this.pool.connect();
    try {
      const result = await connection.queryObject(`
        SELECT
          (SELECT COUNT(*) FROM ${this._done_table} WHERE created_at > NOW() - INTERVAL('1 minute')) as done,
          (SELECT COUNT(*) FROM ${this._fail_table} WHERE created_at > NOW() - INTERVAL('1 minute')) as fail,
          (SELECT COUNT(*) FROM ${this._pending_table} WHERE pending = true) as queued,
          (SELECT SUM(error_attempts) FROM ${this._fail_table} WHERE created_at > NOW() - INTERVAL('1 minute')) as retry,
          NOW() as created_at,
          NOW() as updated_at
      `);
      return result.rows[0] as QueueStorageStats;
    } finally {
      connection.release();
    }
  }

  async stats(inteval: string, period: string): Promise<QueueStorageStats[]> {
    this._guard_initialized();
    const connection = await this.pool.connect();
    try {
      const result = await connection.queryObject(`
        SELECT * FROM ${this._stats_table}_${ivname(inteval)}
        WHERE created_at >= date_bin('${period}', NOW(), TIMESTAMP '2001-01-01')
        ORDER BY created_at ASC
      `);
      return result.rows as QueueStorageStats[];
    } finally {
      connection.release();
    }
  }

  async _create_tables(): Promise<void> {
    const connection = await this.pool.connect();
    try {
      await connection.queryObject(`
        CREATE TABLE IF NOT EXISTS ${this.name}_state (
          id INT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
          is_paused BOOLEAN DEFAULT false,
          created_at TIMESTAMP NOT NULL,
          updated_at TIMESTAMP NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ${this._pending_table} (
          id INT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
          uuid VARCHAR(255),
          type VARCHAR(255) NOT NULL,
          payload JSON NOT NULL,
          pending BOOLEAN NOT NULL,
          timeout_ms INTEGER NOT NULL,
          created_at TIMESTAMP NOT NULL,
          updated_at TIMESTAMP NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ${this._fail_table} (
          id INT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
          uuid VARCHAR(255),
          type VARCHAR(255) NOT NULL,
          payload JSON NOT NULL,
          error_data TEXT,
          error_attempts INTEGER NOT NULL,
          created_at TIMESTAMP NOT NULL,
          updated_at TIMESTAMP NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ${this._done_table} (
          id INT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
          uuid VARCHAR(255),
          type VARCHAR(255) NOT NULL,
          payload JSON NOT NULL,
          result JSON NOT NULL,
          created_at TIMESTAMP NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ${this._stats_table}_tmpl (
          id INT NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
          done INTEGER DEFAULT 0,
          fail INTEGER DEFAULT 0,
          queued INTEGER DEFAULT 0,
          retry INTEGER DEFAULT 0,
          created_at TIMESTAMP NOT NULL,
          updated_at TIMESTAMP NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS ${this._pending_table}_uuid_idx ON ${this._pending_table} (uuid);
        CREATE UNIQUE INDEX IF NOT EXISTS ${this._fail_table}_uuid_idx ON ${this._fail_table} (uuid);

        INSERT INTO ${this.name}_state (created_at, updated_at)
        VALUES (NOW(), NOW());

        DO $$
        DECLARE
          intervals VARCHAR[] := ARRAY[${
        this.timeseries.map((iv) => ivname(iv)).join(",")
      }];
          postfix VARCHAR;
        BEGIN
          FOREACH postfix IN ARRAY intervals LOOP
            EXECUTE format(
              'CREATE TABLE IF NOT EXISTS %s_%s (LIKE %s_tmpl INCLUDING ALL)',
              '${this._stats_table}', 
              postfix, 
              '${this._stats_table}'
            );
            EXECUTE format(
              'CREATE UNIQUE INDEX IF NOT EXISTS %s_%s_created_at_idx ON %s_%s (created_at)', 
              '${this._stats_table}', 
              postfix, 
              '${this._stats_table}', 
              postfix
            );
          END LOOP;
        END;
        $$;
        DROP TABLE IF EXISTS ${this._stats_table}_tmpl;
      `);

      for (const interval of this.timeseries) {
        const name = this._stats_table + "_" +
          ivname(interval).replaceAll("'", "");
        const p_trigger = this._pending_table.replace(this.name, "") + "_" +
          name;
        const f_trigger = this._fail_table.replace(this.name, "") + "_" + name;
        const d_trigger = this._done_table.replace(this.name, "") + "_" + name;
        await connection.queryObject(`
          CREATE OR REPLACE FUNCTION ${name}_update() RETURNS TRIGGER AS $$
          BEGIN
            INSERT INTO ${name} as s (done, fail, queued, retry, created_at, updated_at)
            VALUES (
              (SELECT COUNT(*) FROM ${this._done_table} WHERE created_at >= date_bin('${interval}', NOW(), TIMESTAMP '2001-01-01')),
              (SELECT COUNT(*) FROM ${this._fail_table} WHERE created_at >= date_bin('${interval}', NOW(), TIMESTAMP '2001-01-01')),
              (SELECT COUNT(*) FROM ${this._pending_table}),
              (SELECT SUM(error_attempts) FROM ${this._fail_table} WHERE created_at >= date_bin('${interval}', NOW(), TIMESTAMP '2001-01-01')),
              date_bin('${interval}', NOW(), TIMESTAMP '2001-01-01'),
              NOW()
            )
            ON CONFLICT (created_at)
            DO UPDATE SET
              done = EXCLUDED.done,
              fail = EXCLUDED.fail,
              queued = EXCLUDED.queued + s.queued,
              retry = EXCLUDED.retry,
              updated_at = EXCLUDED.updated_at;
            DELETE FROM ${name} WHERE created_at < NOW() - INTERVAL '6 month';
            RETURN NULL;
          END;
          $$ LANGUAGE plpgsql;

          CREATE OR REPLACE TRIGGER ${p_trigger}_t AFTER INSERT ON ${this._pending_table} EXECUTE FUNCTION ${name}_update();
          CREATE OR REPLACE TRIGGER ${f_trigger}_t AFTER INSERT ON ${this._fail_table} EXECUTE FUNCTION ${name}_update();
          CREATE OR REPLACE TRIGGER ${d_trigger}_t AFTER INSERT ON ${this._done_table} EXECUTE FUNCTION ${name}_update();
        `);
      }
    } catch (e) {
      console.error(e);
    } finally {
      connection.release();
    }
  }

  async _drop_tables(): Promise<void> {
    const connection = await this.pool.connect();
    try {
      const p_trigger = this._pending_table.replace(this.name, "");
      const f_trigger = this._fail_table.replace(this.name, "");
      const d_trigger = this._done_table.replace(this.name, "");
      await connection.queryObject(`
        DO $$
        DECLARE
          intervals VARCHAR[] := ARRAY[${
        this.timeseries.map((iv) => ivname(iv)).join(",")
      }];
          postfix VARCHAR;
        BEGIN
        FOREACH postfix IN ARRAY intervals LOOP
            EXECUTE format('DROP INDEX IF EXISTS %s_%s_created_at_idx', '${this._stats_table}', postfix);
            EXECUTE format('DROP TABLE IF EXISTS %s_%s', '${this._stats_table}', postfix);
            EXECUTE format('DROP TRIGGER IF EXISTS %s_%s_%s_t ON %s', '${p_trigger}', '${this._stats_table}', postfix, '${this._pending_table}');
            EXECUTE format('DROP TRIGGER IF EXISTS %s_%s_%s_t ON %s', '${f_trigger}','${this._stats_table}', postfix, '${this._fail_table}');
            EXECUTE format('DROP TRIGGER IF EXISTS %s_%s_%s_t ON %s', '${d_trigger}', '${this._stats_table}', postfix, '${this._done_table}');
            EXECUTE format('DROP FUNCTION IF EXISTS %s_%s_update()', '${this._stats_table}', postfix);
          END LOOP;
        END; $$;
        DROP TABLE IF EXISTS ${this.name}_state;
        DROP INDEX IF EXISTS ${this._pending_table}_uuid_idx;
        DROP INDEX IF EXISTS ${this._fail_table}_uuid_idx;
        DROP TABLE IF EXISTS ${this._pending_table};
        DROP TABLE IF EXISTS ${this._fail_table};
        DROP TABLE IF EXISTS ${this._done_table};
      `);
    } finally {
      connection.release();
    }
  }

  get _pending_table(): string {
    return `${this.name}_q`;
  }

  get _fail_table(): string {
    return `${this.name}_f`;
  }

  get _done_table(): string {
    return `${this.name}_d`;
  }

  get _stats_table(): string {
    return `${this.name}_s`;
  }

  _guard_initialized(): void {
    if (!this.is_initialized) {
      throw new Error("QueueStorage is not initialized");
    }
  }
}
