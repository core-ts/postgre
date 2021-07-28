import {save, saveBatch} from './build';
import {Attribute, Attributes, Statement} from './metadata';

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
export class PostgreWriter<T> {
  version?: string;
  constructor(public table: string, public attributes: Attributes, public exec: (sql: string, args?: any[]) => Promise<number>, public map?: (v: T) => T, public buildParam?: (i: number) => string) {
    this.write = this.write.bind(this);
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
    const stmt = save(obj2, this.table, this.attributes, this.version, this.buildParam);
    if (stmt) {
      return this.exec(stmt.query, stmt.args);
    } else {
      return Promise.resolve(0);
    }
  }
}
export class PostgreBatchWriter<T> {
  version?: string;
  constructor(public table: string, public attributes: Attributes, public execute: (statements: Statement[]) => Promise<number>, public map?: (v: T) => T, public buildParam?: (i: number) => string) {
    this.write = this.write.bind(this);
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
    const stmts = saveBatch(list, this.table, this.attributes, this.version, this.buildParam);
    if (stmts && stmts.length > 0) {
      return this.execute(stmts);
    } else {
      return Promise.resolve(0);
    }
  }
}
