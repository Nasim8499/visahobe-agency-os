import { mkdirSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, resolve } from 'node:path';
import { env } from '../env.js';
import { migrations } from './migrations.js';

const require = createRequire(import.meta.url);

export type Dialect = 'sqlite' | 'postgres';
export type SqlParam = string | number | boolean | null | Buffer;
export type Row = Record<string, any>;

export interface Db {
  readonly dialect: Dialect;
  all<T = Row>(sql: string, params?: SqlParam[]): Promise<T[]>;
  get<T = Row>(sql: string, params?: SqlParam[]): Promise<T | undefined>;
  run(sql: string, params?: SqlParam[]): Promise<{ changes: number }>;
  exec(sql: string): Promise<void>;
  tx<T>(fn: () => Promise<T>): Promise<T>;
  close(): Promise<void>;
}

/** `?` placeholders are rewritten to `$n` for postgres. */
function toPgParams(sql: string): string {
  let index = 0;
  let out = '';
  let inString = false;
  for (let i = 0; i < sql.length; i += 1) {
    const ch = sql[i];
    if (ch === "'") inString = !inString;
    if (ch === '?' && !inString) {
      index += 1;
      out += `$${index}`;
    } else {
      out += ch;
    }
  }
  return out;
}

/**
 * The minimal synchronous SQLite surface the server needs. `better-sqlite3` is
 * the preferred driver, but it is a native addon: on hosts where it cannot be
 * compiled or its prebuilt binary cannot be downloaded, Node's own `node:sqlite`
 * module provides the same semantics. The app must never fail to boot because
 * of a missing native build.
 */
interface SqliteStatement {
  all(...params: any[]): any[];
  get(...params: any[]): any;
  run(...params: any[]): { changes: number | bigint };
}

interface SqliteDriver {
  prepare(sql: string): SqliteStatement;
  exec(sql: string): void;
  close(): void;
  pragma(source: string): void;
}

/** node:sqlite accepts null, number, bigint, string and Uint8Array only. */
function toDriverParams(params: any[]): any[] {
  return params.map((value) => {
    if (value === undefined) return null;
    if (typeof value === 'boolean') return value ? 1 : 0;
    return value;
  });
}

/** Rows from node:sqlite are null-prototype objects; normalise them. */
function toPlainRow(row: any): any {
  return row === undefined || row === null ? row : Object.assign({}, row);
}

function openSqliteDriver(file: string): { driver: SqliteDriver; engine: string } {
  try {
    const Database = require('better-sqlite3');
    const db = new Database(file);
    return {
      engine: 'better-sqlite3',
      driver: {
        prepare: (sql: string) => db.prepare(sql),
        exec: (sql: string) => db.exec(sql),
        close: () => db.close(),
        pragma: (source: string) => db.pragma(source),
      },
    };
  } catch (nativeError) {
    let DatabaseSync: any;
    try {
      ({ DatabaseSync } = require('node:sqlite'));
    } catch {
      throw new Error(
        'No SQLite driver available. Install better-sqlite3 (native build) or run on Node 22.5+ ' +
          `for the built-in node:sqlite module. Original error: ${(nativeError as Error).message}`,
      );
    }
    const db = new DatabaseSync(file);
    return {
      engine: 'node:sqlite',
      driver: {
        prepare: (sql: string) => {
          const stmt = db.prepare(sql);
          return {
            all: (...params: any[]) => stmt.all(...toDriverParams(params)).map(toPlainRow),
            get: (...params: any[]) => toPlainRow(stmt.get(...toDriverParams(params))),
            run: (...params: any[]) => {
              const info = stmt.run(...toDriverParams(params));
              return { changes: Number(info?.changes ?? 0) };
            },
          };
        },
        exec: (sql: string) => db.exec(sql),
        close: () => db.close(),
        pragma: (source: string) => db.exec(`PRAGMA ${source}`),
      },
    };
  }
}

class SqliteDb implements Db {
  readonly dialect: Dialect = 'sqlite';
  readonly engine: string;
  private stmt: SqliteDriver;
  private queue: Promise<unknown> = Promise.resolve();

  constructor(private file: string) {
    mkdirSync(dirname(file), { recursive: true });
    const opened = openSqliteDriver(file);
    this.stmt = opened.driver;
    this.engine = opened.engine;
    // WAL needs shared-memory (-shm) support, which some network/FUSE mounts and
    // containers do not provide; fall back to the default journal rather than
    // failing to open the database at all.
    try {
      this.stmt.pragma('journal_mode = WAL');
    } catch {
      try {
        this.stmt.pragma('journal_mode = DELETE');
      } catch {
        /* the driver default journal is fine */
      }
    }
    try {
      this.stmt.pragma('foreign_keys = ON');
      this.stmt.pragma('busy_timeout = 5000');
    } catch {
      /* optional tuning only */
    }
  }

  async all<T = Row>(sql: string, params: SqlParam[] = []): Promise<T[]> {
    return this.stmt.prepare(sql).all(...params) as T[];
  }
  async get<T = Row>(sql: string, params: SqlParam[] = []): Promise<T | undefined> {
    return this.stmt.prepare(sql).get(...params) as T | undefined;
  }
  async run(sql: string, params: SqlParam[] = []): Promise<{ changes: number }> {
    const info = this.stmt.prepare(sql).run(...params);
    return { changes: Number(info.changes ?? 0) };
  }
  async exec(sql: string): Promise<void> {
    this.stmt.exec(sql);
  }
  async tx<T>(fn: () => Promise<T>): Promise<T> {
    // Serialise transactions: better-sqlite3 is synchronous and cannot be
    // interleaved with another transaction on the same connection.
    const run = async (): Promise<T> => {
      this.stmt.exec('BEGIN');
      try {
        const result = await fn();
        this.stmt.exec('COMMIT');
        return result;
      } catch (err) {
        try {
          this.stmt.exec('ROLLBACK');
        } catch {
          /* ignore */
        }
        throw err;
      }
    };
    const next = this.queue.then(run, run);
    this.queue = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  }
  async close(): Promise<void> {
    this.stmt.close();
  }
}

class PostgresDb implements Db {
  readonly dialect: Dialect = 'postgres';
  private client: any;
  private depth = 0;

  constructor(private url: string) {
    const { Pool } = require('pg');
    // Serverless-friendly pool: few connections, no long idle waits. Managed
    // Postgres (Supabase/Neon) needs TLS; a local Postgres does not.
    const needsSsl = /supabase|neon|render|aws|azure|sslmode=require/i.test(url);
    const schema = process.env.DB_SCHEMA?.trim();
    const connectionString = schema
      ? `${url}${url.includes('?') ? '&' : '?'}options=${encodeURIComponent(`-c search_path=${schema}`)}`
      : url;
    this.client = new Pool({
      connectionString,
      max: Number(process.env.DB_POOL_MAX) || 3,
      idleTimeoutMillis: 10_000,
      connectionTimeoutMillis: 15_000,
      ssl: needsSsl ? { rejectUnauthorized: false } : undefined,
    });
    // A dropped idle client must not crash the process.
    this.client.on('error', () => undefined);
  }

  async all<T = Row>(sql: string, params: SqlParam[] = []): Promise<T[]> {
    const res = await this.client.query(toPgParams(sql), params);
    return res.rows as T[];
  }
  async get<T = Row>(sql: string, params: SqlParam[] = []): Promise<T | undefined> {
    const res = await this.client.query(toPgParams(sql), params);
    return res.rows[0] as T | undefined;
  }
  async run(sql: string, params: SqlParam[] = []): Promise<{ changes: number }> {
    const res = await this.client.query(toPgParams(sql), params);
    return { changes: res.rowCount ?? 0 };
  }
  async exec(sql: string): Promise<void> {
    await this.client.query(sql);
  }
  async tx<T>(fn: () => Promise<T>): Promise<T> {
    if (this.depth > 0) return fn();
    const conn = await this.client.connect();
    this.depth += 1;
    try {
      await conn.query('BEGIN');
      const scoped = new PostgresDb(this.url);
      scoped.client = conn;
      const result = await fn();
      await conn.query('COMMIT');
      return result;
    } catch (err) {
      await conn.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      this.depth -= 1;
      conn.release();
    }
  }
  async close(): Promise<void> {
    await this.client.end();
  }
}

let instance: Db | null = null;

export function getDb(): Db {
  if (instance) return instance;
  instance = env.DATABASE_URL?.startsWith('postgres')
    ? new PostgresDb(env.DATABASE_URL)
    : new SqliteDb(resolve(process.cwd(), env.DB_FILE));
  return instance;
}

/** Human readable driver name for health endpoints and boot logs. */
export function dbEngine(): string {
  const db = getDb();
  return db instanceof SqliteDb ? db.engine : 'pg';
}

export async function runMigrations(db: Db = getDb()): Promise<string[]> {
  const applied: string[] = [];
  await db.exec(
    `CREATE TABLE IF NOT EXISTS schema_migrations (
       id TEXT PRIMARY KEY,
       applied_at TEXT NOT NULL
     )`,
  );
  const done = new Set((await db.all<{ id: string }>('SELECT id FROM schema_migrations')).map((r) => r.id));
  for (const migration of migrations) {
    if (done.has(migration.id)) continue;
    for (const stmt of migration.statements) await db.exec(stmt);
    await db.run('INSERT INTO schema_migrations (id, applied_at) VALUES (?, ?)', [
      migration.id,
      new Date().toISOString(),
    ]);
    applied.push(migration.id);
  }
  return applied;
}
