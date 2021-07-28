import {Pool, PoolClient} from 'pg';
import {Attribute, Manager, Statement, StringMap} from './metadata';

export * from './metadata';
export * from './build';
export * from './batch';

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
    return queryWithclient(this.client, sql, args, m, bools);
  }
  queryOne<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T> {
    return queryOneWithClient(this.client, sql, args, m, bools);
  }
  executeScalar<T>(sql: string, args?: any[]): Promise<T> {
    return executeScalarWithclient<T>(this.client, sql, args);
  }
  count(sql: string, args?: any[]): Promise<number> {
    return countWithclient(this.client, sql, args);
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
export function queryWithclient<T>(client: PoolClient, sql: string, args?: any[], m?: StringMap, bools?: Attribute[]): Promise<T[]> {
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
  return queryWithclient<T>(client, sql, args, m, bools).then(r => {
    return (r && r.length > 0 ? r[0] : null);
  });
}
export function executeScalarWithclient<T>(client: PoolClient, sql: string, args?: any[]): Promise<T> {
  return queryOneWithClient<T>(client, sql, args).then(r => {
    if (!r) {
      return null;
    } else {
      const keys = Object.keys(r);
      return r[keys[0]];
    }
  });
}
export function countWithclient(client: PoolClient, sql: string, args?: any[]): Promise<number> {
  return executeScalarWithclient<number>(client, sql, args);
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
