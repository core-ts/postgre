import {Attribute, Attributes, Statement, StringMap} from './metadata';

export function param(i: number): string {
  return '$' + i;
}
export interface Metadata {
  keys: Attribute[];
  bools?: Attribute[];
  map?: StringMap;
  version?: string;
}
export function metadata(attrs: Attributes): Metadata {
  const mp: StringMap = {};
  const ks = Object.keys(attrs);
  const ats: Attribute[] = [];
  const bools: Attribute[] = [];
  let ver: string;
  let isMap = false;
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    if (attr.key) {
      ats.push(attr);
    }
    if (attr.type === 'boolean') {
      bools.push(attr);
    }
    if (attr.version) {
      ver = k;
    }
    const field = (attr.field ? attr.field : k);
    const s = field.toLowerCase();
    if (s !== k) {
      mp[s] = k;
      isMap = true;
    }
  }
  const m: Metadata = {keys: ats, version: ver};
  if (isMap) {
    m.map = mp;
  }
  if (bools.length > 0) {
    m.bools = bools;
  }
  return m;
}
export function save<T>(obj: T, table: string, attrs: Attributes, ver?: string, buildParam?: (i: number) => string, i?: number): Statement {
  if (!i) {
    i = 1;
  }
  if (!buildParam) {
    buildParam = param;
  }
  const ks = Object.keys(attrs);
  const pks: Attribute[] = [];
  const cols: string[] = [];
  const values: string[] = [];
  const args: any[] = [];
  let isVersion = false;
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    if (attr.key) {
      pks.push(attr);
    }
    const v = obj[k];
    if (v != null && v !== undefined && !attr.ignored && !attr.noinsert) {
      const field = (attr.field ? attr.field : k);
      cols.push(field);
      if (k === ver) {
        isVersion = true;
        values.push(`${1}`);
      } else {
        if (v === '') {
          values.push(`''`);
        } else if (typeof v === 'number') {
          values.push(`${v}`);
        } else {
          const p = buildParam(i);
          values.push(p);
          i++;
          if (typeof v === 'boolean') {
            if (v === true) {
              const v2 = (attr.true ? attr.true : '1');
              args.push(v2);
            } else {
              const v2 = (attr.false ? attr.false : '0');
              args.push(v2);
            }
          } else {
            args.push(v);
          }
        }
      }
    }
  }
  if (!isVersion && ver && ver.length > 0) {
    const attr = attrs[ver];
    const field = (attr.field ? attr.field : ver);
    cols.push(field);
    values.push(`${1}`);
  }
  if (pks.length === 0) {
    if (cols.length === 0) {
      return null;
    } else {
      const q = `insert into ${table}(${cols.join(',')})values(${values.join(',')})`;
      return { query: q, args };
    }
  } else {
    const colSet: string[] = [];
    for (const k of ks) {
      const v = obj[k];
      if (v !== undefined) {
        const attr = attrs[k];
        if (attr && !attr.key && !attr.ignored && !attr.noupdate) {
          const field = (attr.field ? attr.field : k);
          let x: string;
          if (v == null) {
            x = 'null';
          } else if (v === '') {
            x = `''`;
          } else if (typeof v === 'number') {
            x = `${v}`;
          } else {
            x = buildParam(i);
            i++;
            if (typeof v === 'boolean') {
              if (v === true) {
                const v2 = (attr.true ? '' + attr.true : '1');
                args.push(v2);
              } else {
                const v2 = (attr.false ? '' + attr.false : '0');
                args.push(v2);
              }
            } else {
              args.push(v);
            }
          }
          colSet.push(`${field}=${x}`);
        }
      }
    }
    const fks: string[] = [];
    for (const pk of pks) {
      const field = (pk.field ? pk.field : pk.name);
      fks.push(field);
    }
    if (colSet.length === 0) {
      const q = `insert into ${table}(${cols.join(',')})values(${values.join(',')}) on conflict(${fks.join(',')}) do nothing`;
      return { query: q, args };
    } else {
      const q = `insert into ${table}(${cols.join(',')})values(${values.join(',')}) on conflict(${fks.join(',')}) do update set ${colSet.join(',')}`;
      return { query: q, args };
    }
  }
}
export function saveBatch<T>(objs: T[], table: string, attrs: Attributes, ver?: string, buildParam?: (i: number) => string): Statement[] {
  if (!buildParam) {
    buildParam = param;
  }
  const sts: Statement[] = [];
  const meta = metadata(attrs);
  const pks = meta.keys;
  if (!pks || pks.length === 0) {
    return null;
  }
  const ks = Object.keys(attrs);
  for (const obj of objs) {
    let i = 1;
    const cols: string[] = [];
    const values: string[] = [];
    const args: any[] = [];
    let isVersion = false;
    for (const k of ks) {
      const attr = attrs[k];
      const v = obj[k];
      if (v != null && v !== undefined && !attr.ignored && !attr.noinsert) {
        const field = (attr.field ? attr.field : k);
        cols.push(field);
        if (k === ver) {
          isVersion = true;
          values.push(`${1}`);
        } else {
          if (v === '') {
            values.push(`''`);
          } else if (typeof v === 'number') {
            values.push(`${v}`);
          } else {
            const p = buildParam(i);
            values.push(p);
            i++;
            if (typeof v === 'boolean') {
              if (v === true) {
                const v2 = (attr.true ? attr.true : '1');
                args.push(v2);
              } else {
                const v2 = (attr.false ? attr.false : '0');
                args.push(v2);
              }
            } else {
              args.push(v);
            }
          }
        }
      }
    }
    if (!isVersion && ver && ver.length > 0) {
      const attr = attrs[ver];
      const field = (attr.field ? attr.field : ver);
      cols.push(field);
      values.push(`${1}`);
    }
    const colSet: string[] = [];
    for (const k of ks) {
      const v = obj[k];
      if (v !== undefined) {
        const attr = attrs[k];
        if (attr && !attr.key && !attr.ignored && k !== ver && !attr.noupdate) {
          const field = (attr.field ? attr.field : k);
          let x: string;
          if (v == null) {
            x = 'null';
          } else if (v === '') {
            x = `''`;
          } else if (typeof v === 'number') {
            x = `${v}`;
          } else {
            x = buildParam(i);
            i++;
            if (typeof v === 'boolean') {
              if (v === true) {
                const v2 = (attr.true ? '' + attr.true : '1');
                args.push(v2);
              } else {
                const v2 = (attr.false ? '' + attr.false : '0');
                args.push(v2);
              }
            } else {
              args.push(v);
            }
          }
          colSet.push(`${field}=${x}`);
        }
      }
    }
    const fks: string[] = [];
    for (const pk of pks) {
      const field = (pk.field ? pk.field : pk.name);
      fks.push(field);
    }
    if (colSet.length === 0) {
      const q = `insert into ${table}(${cols.join(',')})values(${values.join(',')}) on conflict(${fks.join(',')}) do nothing`;
      const smt = { query: q, args };
      sts.push(smt);
    } else {
      const q = `insert into ${table}(${cols.join(',')})values(${values.join(',')}) on conflict(${fks.join(',')}) do update set ${colSet.join(',')}`;
      const smt = { query: q, args };
      sts.push(smt);
    }
  }
  return sts;
}
