import {Pool, PoolClient} from 'pg';
import {save, saveBatch} from './build';
import {Attribute, Attributes, Manager, Statement, StringMap} from './metadata';

export * from './metadata';
export * from './build';

export class PoolManager implements Manager {
  constructor(public pool: Pool) {
    this.exec = this.exec.bind(this);
    this.execute = this.execute.bind(this);
    this.query = this.query.bind(this);
    this.queryOne = this.queryOne.bind(this);
    this.executeScalar = this.executeScalar.bind(this);
    this.count = this.count.bind(this);
  }
  exec(sql: string, args?: any[]): Promise<number> {
    return exec(this.pool, sql, args);
  }
  execute(statements: Statement[]): Promise<number> {
    return execute(this.pool, statements);
  }
  query<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T[]> {
    return query(this.pool, sql, args, m, bools);
  }
  queryOne<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T> {
    return queryOne(this.pool, sql, args, m, bools);
  }
  executeScalar<T>(sql: string, args?: any[]): Promise<T> {
    return executeScalar<T>(this.pool, sql, args);
  }
  count(sql: string, args?: any[]): Promise<number> {
    return count(this.pool, sql, args);
  }
}
export class PoolClientManager implements Manager {
  constructor(public client: PoolClient) {
    this.exec = this.exec.bind(this);
    this.execute = this.execute.bind(this);
    this.query = this.query.bind(this);
    this.queryOne = this.queryOne.bind(this);
    this.executeScalar = this.executeScalar.bind(this);
    this.count = this.count.bind(this);
  }
  exec(sql: string, args?: any[]): Promise<number> {
    return execWithClient(this.client, sql, args);
  }
  execute(statements: Statement[]): Promise<number> {
    return executeWithClient(this.client, statements);
  }
  query<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T[]> {
    return queryWithClient(this.client, sql, args, m, bools);
  }
  queryOne<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T> {
    return queryOneWithClient(this.client, sql, args, m, bools);
  }
  executeScalar<T>(sql: string, args?: any[]): Promise<T> {
    return executeScalarWithClient<T>(this.client, sql, args);
  }
  count(sql: string, args?: any[]): Promise<number> {
    return countWithClient(this.client, sql, args);
  }
}
export async function execute(pool: Pool, statements: Statement[]): Promise<number> {
  const client = await pool.connect();
  try {
    await client.query('begin');
    const arrPromise = statements.map((item) => client.query(item.query, item.args ? item.args : []));
    let c = 0;
    await Promise.all(arrPromise).then(results => {
      for (const obj of results) {
        c += obj.rowCount;
      }
    });
    await client.query('commit');
    return c;
  } catch (e) {
    await client.query('rollback');
    throw e;
  } finally {
    client.release();
  }
}
function buildError(err: any): any {
  if (err.code === '23505') {
    err.error = 'duplicate';
  }
  return err;
}
export function exec(pool: Pool, sql: string, args?: any[]): Promise<number> {
  const p = toArray(args);
  return new Promise<number>((resolve, reject) => {
    return pool.query(sql, p, (err, results) => {
      if (err) {
        buildError(err);
        return reject(err);
      } else {
        return resolve(results.rowCount);
      }
    });
  });
}
export function query<T>(pool: Pool, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T[]> {
  const p = toArray(args);
  return new Promise<T[]>((resolve, reject) => {
    return pool.query<T>(sql, p, (err, results) => {
      if (err) {
        return reject(err);
      } else {
        return resolve(handleResults(results.rows, m, bools));
      }
    });
  });
}
export function queryOne<T>(pool: Pool, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T> {
  return query<T>(pool, sql, args, m, bools).then(r => {
    return (r && r.length > 0 ? r[0] : null);
  });
}
export function executeScalar<T>(pool: Pool, sql: string, args?: any[]): Promise<T> {
  return queryOne<T>(pool, sql, args).then(r => {
    if (!r) {
      return null;
    } else {
      const keys = Object.keys(r);
      return r[keys[0]];
    }
  });
}
export function count(pool: Pool, sql: string, args?: any[]): Promise<number> {
  return executeScalar<number>(pool, sql, args);
}

export async function executeWithClient(client: PoolClient, statements: Statement[]): Promise<number> {
  try {
    await client.query('begin');
    const arrPromise = statements.map((item) => client.query(item.query, item.args ? item.args : []));
    let c = 0;
    await Promise.all(arrPromise).then(results => {
      for (const obj of results) {
        c += obj.rowCount;
      }
    });
    await client.query('commit');
    return c;
  } catch (e) {
    await client.query('rollback');
    throw e;
  }
}
export function execWithClient(client: PoolClient, sql: string, args?: any[]): Promise<number> {
  const p = toArray(args);
  return new Promise<number>((resolve, reject) => {
    return client.query(sql, p, (err, results) => {
      if (err) {
        buildError(err);
        return reject(err);
      } else {
        return resolve(results.rowCount);
      }
    });
  });
}
export function queryWithClient<T>(client: PoolClient, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T[]> {
  const p = toArray(args);
  return new Promise<T[]>((resolve, reject) => {
    return client.query<T>(sql, p, (err, results) => {
      if (err) {
        return reject(err);
      } else {
        return resolve(handleResults(results.rows, m, bools));
      }
    });
  });
}
export function queryOneWithClient<T>(client: PoolClient, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T> {
  return queryWithClient<T>(client, sql, args, m, bools).then(r => {
    return (r && r.length > 0 ? r[0] : null);
  });
}
export function executeScalarWithClient<T>(client: PoolClient, sql: string, args?: any[]): Promise<T> {
  return queryOneWithClient<T>(client, sql, args).then(r => {
    if (!r) {
      return null;
    } else {
      const keys = Object.keys(r);
      return r[keys[0]];
    }
  });
}
export function countWithClient(client: PoolClient, sql: string, args?: any[]): Promise<number> {
  return executeScalarWithClient<number>(client, sql, args);
}

export function toArray<T>(arr: T[]): T[] {
  if (!arr || arr.length === 0) {
    return [];
  }
  const p: T[] = [];
  const l = arr.length;
  for (let i = 0; i < l; i++) {
    if (arr[i] === undefined) {
      p.push(null);
    } else {
      p.push(arr[i]);
    }
  }
  return p;
}
export function handleResults<T>(r: T[], m?: StringMap, bools?: Attribute[]) {
  if (m) {
    const res = mapArray(r, m);
    if (bools && bools.length > 0) {
      return handleBool(res, bools);
    } else {
      return res;
    }
  } else {
    if (bools && bools.length > 0) {
      return handleBool(r, bools);
    } else {
      return r;
    }
  }
}
export function handleBool<T>(objs: T[], bools: Attribute[]) {
  if (!bools || bools.length === 0 || !objs) {
    return objs;
  }
  for (const obj of objs) {
    for (const field of bools) {
      const value = obj[field.name];
      if (value != null && value !== undefined) {
        const b = field.true;
        if (b == null || b === undefined) {
          // tslint:disable-next-line:triple-equals
          obj[field.name] = ('1' == value || 'T' == value || 'Y' == value);
        } else {
          // tslint:disable-next-line:triple-equals
          obj[field.name] = (value == b ? true : false);
        }
      }
    }
  }
  return objs;
}
export function map<T>(obj: T, m?: StringMap): any {
  if (!m) {
    return obj;
  }
  const mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return obj;
  }
  const obj2: any = {};
  const keys = Object.keys(obj);
  for (const key of keys) {
    let k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    obj2[k0] = obj[key];
  }
  return obj2;
}
export function mapArray<T>(results: T[], m?: StringMap): T[] {
  if (!m) {
    return results;
  }
  const mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return results;
  }
  const objs = [];
  const length = results.length;
  for (let i = 0; i < length; i++) {
    const obj = results[i];
    const obj2: any = {};
    const keys = Object.keys(obj);
    for (const key of keys) {
      let k0 = m[key];
      if (!k0) {
        k0 = key;
      }
      obj2[k0] = (obj as any)[key];
    }
    objs.push(obj2);
  }
  return objs;
}
export function getFields(fields: string[], all?: string[]): string[] {
  if (!fields || fields.length === 0) {
    return undefined;
  }
  const ext: string [] = [];
  if (all) {
    for (const s of fields) {
      if (all.includes(s)) {
        ext.push(s);
      }
    }
    if (ext.length === 0) {
      return undefined;
    } else {
      return ext;
    }
  } else {
    return fields;
  }
}
export function buildFields(fields: string[], all?: string[]): string {
  const s = getFields(fields, all);
  if (!s || s.length === 0) {
    return '*';
  } else {
    return s.join(',');
  }
}
export function getMapField(name: string, mp?: StringMap): string {
  if (!mp) {
    return name;
  }
  const x = mp[name];
  if (!x) {
    return name;
  }
  if (typeof x === 'string') {
    return x;
  }
  return name;
}
export function isEmpty(s: string): boolean {
  return !(s && s.length > 0);
}

export class StringService {
  constructor(protected pool: Pool, public table: string, public column: string) {
    this.load = this.load.bind(this);
    this.save = this.save.bind(this);
  }
  load(key: string, max: number): Promise<string[]> {
    const s = `select ${this.column} from ${this.table} where ${this.column} ilike $1 order by ${this.column} fetch next ${max} rows only`;
    return query(this.pool, s, ['' + key + '%']).then(arr => {
      return arr.map(i => i[this.column] as string);
    });
  }
  save(values: string[]): Promise<number> {
    if (!values || values.length === 0) {
      return Promise.resolve(0);
    }
    const arr: string[] = [];
    for (let i = 1; i <= values.length; i++) {
      arr.push('($' + i + ')');
      i++;
    }
    const s = `insert into ${this.table}(${this.column})values${arr.join(',')} on conflict do nothing`;
    return exec(this.pool, s, values);
  }
}

export function version(attrs: Attributes): Attribute {
  const ks = Object.keys(attrs);
  for (const k of ks) {
    const attr = attrs[k];
    if (attr.version) {
      attr.name = k;
      return attr;
    }
  }
  return undefined;
}
export class PostgreSQLWriter<T> {
  pool?: Pool;
  version?: string;
  exec?: (sql: string, args?: any[]) => Promise<number>;
  map?: (v: T) => T;
  param?: (i: number) => string;
  constructor(pool: Pool|((sql: string, args?: any[]) => Promise<number>), public table: string, public attributes: Attributes, toDB?: (v: T) => T, buildParam?: (i: number) => string) {
    this.write = this.write.bind(this);
    if (typeof pool === 'function') {
      this.exec = pool;
    } else {
      this.pool = pool;
    }
    this.param = buildParam;
    this.map = toDB;
    const x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  write(obj: T): Promise<number> {
    if (!obj) {
      return Promise.resolve(0);
    }
    let obj2 = obj;
    if (this.map) {
      obj2 = this.map(obj);
    }
    const stmt = save(obj2, this.table, this.attributes, this.version, this.param);
    if (stmt) {
      if (this.exec) {
        return this.exec(stmt.query, stmt.args);
      } else {
        return exec(this.pool, stmt.query, stmt.args);
      }
    } else {
      return Promise.resolve(0);
    }
  }
}
export class PostgreSQLBatchWriter<T> {
  pool?: Pool;
  version?: string;
  execute?: (statements: Statement[]) => Promise<number>;
  map?: (v: T) => T;
  param?: (i: number) => string;
  constructor(pool: Pool|((statements: Statement[]) => Promise<number>), public table: string, public attributes: Attributes, toDB?: (v: T) => T, buildParam?: (i: number) => string) {
    this.write = this.write.bind(this);
    if (typeof pool === 'function') {
      this.execute = pool;
    } else {
      this.pool = pool;
    }
    this.param = buildParam;
    this.map = toDB;
    const x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  write(objs: T[]): Promise<number> {
    if (!objs || objs.length === 0) {
      return Promise.resolve(0);
    }
    let list = objs;
    if (this.map) {
      list = [];
      for (const obj of objs) {
        const obj2 = this.map(obj);
        list.push(obj2);
      }
    }
    const stmts = saveBatch(list, this.table, this.attributes, this.version, this.param);
    if (stmts && stmts.length > 0) {
      if (this.execute) {
        return this.execute(stmts);
      } else {
        return execute(this.pool, stmts);
      }
    } else {
      return Promise.resolve(0);
    }
  }
}

export interface AnyMap {
  [key: string]: any;
}
export class PostgreSQLChecker {
  constructor(private pool: Pool, private service?: string, private timeout?: number) {
    if (!this.timeout) {
      this.timeout = 4200;
    }
    if (!this.service) {
      this.service = 'postgre';
    }
    this.check = this.check.bind(this);
    this.name = this.name.bind(this);
    this.build = this.build.bind(this);
  }
  async check(): Promise<AnyMap> {
    const obj = {} as AnyMap;
    await this.pool.connect();
    const promise = new Promise<any>((resolve, reject) => {
      this.pool.query('select now()', (err, result) => {
        if (err) {
          return reject(err);
        } else {
          resolve(obj);
        }
      });
    });
    if (this.timeout > 0) {
      return promiseTimeOut(this.timeout, promise);
    } else {
      return promise;
    }
  }
  name(): string {
    return this.service;
  }
  build(data: AnyMap, err: any): AnyMap {
    if (err) {
      if (!data) {
        data = {} as AnyMap;
      }
      data['error'] = err;
    }
    return data;
  }
}

function promiseTimeOut(timeoutInMilliseconds: number, promise: Promise<any>): Promise<any> {
  return Promise.race([
    promise,
    new Promise((resolve, reject) => {
      setTimeout(() => {
        reject(`Timed out in: ${timeoutInMilliseconds} milliseconds!`);
      }, timeoutInMilliseconds);
    })
  ]);
}
