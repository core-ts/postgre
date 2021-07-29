"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
  function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
  return new (P || (P = Promise))(function (resolve, reject) {
    function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
    function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
    function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
    step((generator = generator.apply(thisArg, _arguments || [])).next());
  });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
  var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
  return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
  function verb(n) { return function (v) { return step([n, v]); }; }
  function step(op) {
    if (f) throw new TypeError("Generator is already executing.");
    while (_) try {
      if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
      if (y = 0, t) op = [op[0] & 2, t.value];
      switch (op[0]) {
        case 0: case 1: t = op; break;
        case 4: _.label++; return { value: op[1], done: false };
        case 5: _.label++; y = op[1]; op = [0]; continue;
        case 7: op = _.ops.pop(); _.trys.pop(); continue;
        default:
          if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
          if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
          if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
          if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
          if (t[2]) _.ops.pop();
          _.trys.pop(); continue;
      }
      op = body.call(thisArg, _);
    } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
    if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
  }
};
function __export(m) {
  for (var p in m) if (!exports.hasOwnProperty(p)) exports[p] = m[p];
}
Object.defineProperty(exports, "__esModule", { value: true });
var build_1 = require("./build");
__export(require("./build"));
var PoolManager = (function () {
  function PoolManager(pool) {
    this.pool = pool;
    this.exec = this.exec.bind(this);
    this.execute = this.execute.bind(this);
    this.query = this.query.bind(this);
    this.queryOne = this.queryOne.bind(this);
    this.executeScalar = this.executeScalar.bind(this);
    this.count = this.count.bind(this);
  }
  PoolManager.prototype.exec = function (sql, args) {
    return exec(this.pool, sql, args);
  };
  PoolManager.prototype.execute = function (statements) {
    return execute(this.pool, statements);
  };
  PoolManager.prototype.query = function (sql, args, m, bools) {
    return query(this.pool, sql, args, m, bools);
  };
  PoolManager.prototype.queryOne = function (sql, args, m, bools) {
    return queryOne(this.pool, sql, args, m, bools);
  };
  PoolManager.prototype.executeScalar = function (sql, args) {
    return executeScalar(this.pool, sql, args);
  };
  PoolManager.prototype.count = function (sql, args) {
    return count(this.pool, sql, args);
  };
  return PoolManager;
}());
exports.PoolManager = PoolManager;
var PoolClientManager = (function () {
  function PoolClientManager(client) {
    this.client = client;
    this.exec = this.exec.bind(this);
    this.execute = this.execute.bind(this);
    this.query = this.query.bind(this);
    this.queryOne = this.queryOne.bind(this);
    this.executeScalar = this.executeScalar.bind(this);
    this.count = this.count.bind(this);
  }
  PoolClientManager.prototype.exec = function (sql, args) {
    return execWithClient(this.client, sql, args);
  };
  PoolClientManager.prototype.execute = function (statements) {
    return executeWithClient(this.client, statements);
  };
  PoolClientManager.prototype.query = function (sql, args, m, bools) {
    return queryWithClient(this.client, sql, args, m, bools);
  };
  PoolClientManager.prototype.queryOne = function (sql, args, m, bools) {
    return queryOneWithClient(this.client, sql, args, m, bools);
  };
  PoolClientManager.prototype.executeScalar = function (sql, args) {
    return executeScalarWithClient(this.client, sql, args);
  };
  PoolClientManager.prototype.count = function (sql, args) {
    return countWithClient(this.client, sql, args);
  };
  return PoolClientManager;
}());
exports.PoolClientManager = PoolClientManager;
function execute(pool, statements) {
  return __awaiter(this, void 0, void 0, function () {
    var client, arrPromise, c_1, e_1;
    return __generator(this, function (_a) {
      switch (_a.label) {
        case 0: return [4, pool.connect()];
        case 1:
          client = _a.sent();
          _a.label = 2;
        case 2:
          _a.trys.push([2, 6, 8, 9]);
          return [4, client.query('begin')];
        case 3:
          _a.sent();
          arrPromise = statements.map(function (item) { return client.query(item.query, item.args ? item.args : []); });
          c_1 = 0;
          return [4, Promise.all(arrPromise).then(function (results) {
              for (var _i = 0, results_1 = results; _i < results_1.length; _i++) {
                var obj = results_1[_i];
                c_1 += obj.rowCount;
              }
            })];
        case 4:
          _a.sent();
          return [4, client.query('commit')];
        case 5:
          _a.sent();
          return [2, c_1];
        case 6:
          e_1 = _a.sent();
          return [4, client.query('rollback')];
        case 7:
          _a.sent();
          throw e_1;
        case 8:
          client.release();
          return [7];
        case 9: return [2];
      }
    });
  });
}
exports.execute = execute;
function buildError(err) {
  if (err.code === '23505') {
    err.error = 'duplicate';
  }
  return err;
}
function exec(pool, sql, args) {
  var p = toArray(args);
  return new Promise(function (resolve, reject) {
    return pool.query(sql, p, function (err, results) {
      if (err) {
        buildError(err);
        return reject(err);
      }
      else {
        return resolve(results.rowCount);
      }
    });
  });
}
exports.exec = exec;
function query(pool, sql, args, m, bools) {
  var p = toArray(args);
  return new Promise(function (resolve, reject) {
    return pool.query(sql, p, function (err, results) {
      if (err) {
        return reject(err);
      }
      else {
        return resolve(handleResults(results.rows, m, bools));
      }
    });
  });
}
exports.query = query;
function queryOne(pool, sql, args, m, bools) {
  return query(pool, sql, args, m, bools).then(function (r) {
    return (r && r.length > 0 ? r[0] : null);
  });
}
exports.queryOne = queryOne;
function executeScalar(pool, sql, args) {
  return queryOne(pool, sql, args).then(function (r) {
    if (!r) {
      return null;
    }
    else {
      var keys = Object.keys(r);
      return r[keys[0]];
    }
  });
}
exports.executeScalar = executeScalar;
function count(pool, sql, args) {
  return executeScalar(pool, sql, args);
}
exports.count = count;
function executeWithClient(client, statements) {
  return __awaiter(this, void 0, void 0, function () {
    var arrPromise, c_2, e_2;
    return __generator(this, function (_a) {
      switch (_a.label) {
        case 0:
          _a.trys.push([0, 4, , 6]);
          return [4, client.query('begin')];
        case 1:
          _a.sent();
          arrPromise = statements.map(function (item) { return client.query(item.query, item.args ? item.args : []); });
          c_2 = 0;
          return [4, Promise.all(arrPromise).then(function (results) {
              for (var _i = 0, results_2 = results; _i < results_2.length; _i++) {
                var obj = results_2[_i];
                c_2 += obj.rowCount;
              }
            })];
        case 2:
          _a.sent();
          return [4, client.query('commit')];
        case 3:
          _a.sent();
          return [2, c_2];
        case 4:
          e_2 = _a.sent();
          return [4, client.query('rollback')];
        case 5:
          _a.sent();
          throw e_2;
        case 6: return [2];
      }
    });
  });
}
exports.executeWithClient = executeWithClient;
function execWithClient(client, sql, args) {
  var p = toArray(args);
  return new Promise(function (resolve, reject) {
    return client.query(sql, p, function (err, results) {
      if (err) {
        buildError(err);
        return reject(err);
      }
      else {
        return resolve(results.rowCount);
      }
    });
  });
}
exports.execWithClient = execWithClient;
function queryWithClient(client, sql, args, m, bools) {
  var p = toArray(args);
  return new Promise(function (resolve, reject) {
    return client.query(sql, p, function (err, results) {
      if (err) {
        return reject(err);
      }
      else {
        return resolve(handleResults(results.rows, m, bools));
      }
    });
  });
}
exports.queryWithClient = queryWithClient;
function queryOneWithClient(client, sql, args, m, bools) {
  return queryWithClient(client, sql, args, m, bools).then(function (r) {
    return (r && r.length > 0 ? r[0] : null);
  });
}
exports.queryOneWithClient = queryOneWithClient;
function executeScalarWithClient(client, sql, args) {
  return queryOneWithClient(client, sql, args).then(function (r) {
    if (!r) {
      return null;
    }
    else {
      var keys = Object.keys(r);
      return r[keys[0]];
    }
  });
}
exports.executeScalarWithClient = executeScalarWithClient;
function countWithClient(client, sql, args) {
  return executeScalarWithClient(client, sql, args);
}
exports.countWithClient = countWithClient;
function toArray(arr) {
  if (!arr || arr.length === 0) {
    return [];
  }
  var p = [];
  var l = arr.length;
  for (var i = 0; i < l; i++) {
    if (arr[i] === undefined) {
      p.push(null);
    }
    else {
      p.push(arr[i]);
    }
  }
  return p;
}
exports.toArray = toArray;
function handleResults(r, m, bools) {
  if (m) {
    var res = mapArray(r, m);
    if (bools && bools.length > 0) {
      return handleBool(res, bools);
    }
    else {
      return res;
    }
  }
  else {
    if (bools && bools.length > 0) {
      return handleBool(r, bools);
    }
    else {
      return r;
    }
  }
}
exports.handleResults = handleResults;
function handleBool(objs, bools) {
  if (!bools || bools.length === 0 || !objs) {
    return objs;
  }
  for (var _i = 0, objs_1 = objs; _i < objs_1.length; _i++) {
    var obj = objs_1[_i];
    for (var _a = 0, bools_1 = bools; _a < bools_1.length; _a++) {
      var field = bools_1[_a];
      var value = obj[field.name];
      if (value != null && value !== undefined) {
        var b = field.true;
        if (b == null || b === undefined) {
          obj[field.name] = ('1' == value || 'T' == value || 'Y' == value);
        }
        else {
          obj[field.name] = (value == b ? true : false);
        }
      }
    }
  }
  return objs;
}
exports.handleBool = handleBool;
function map(obj, m) {
  if (!m) {
    return obj;
  }
  var mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return obj;
  }
  var obj2 = {};
  var keys = Object.keys(obj);
  for (var _i = 0, keys_1 = keys; _i < keys_1.length; _i++) {
    var key = keys_1[_i];
    var k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    obj2[k0] = obj[key];
  }
  return obj2;
}
exports.map = map;
function mapArray(results, m) {
  if (!m) {
    return results;
  }
  var mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return results;
  }
  var objs = [];
  var length = results.length;
  for (var i = 0; i < length; i++) {
    var obj = results[i];
    var obj2 = {};
    var keys = Object.keys(obj);
    for (var _i = 0, keys_2 = keys; _i < keys_2.length; _i++) {
      var key = keys_2[_i];
      var k0 = m[key];
      if (!k0) {
        k0 = key;
      }
      obj2[k0] = obj[key];
    }
    objs.push(obj2);
  }
  return objs;
}
exports.mapArray = mapArray;
function getFields(fields, all) {
  if (!fields || fields.length === 0) {
    return undefined;
  }
  var ext = [];
  if (all) {
    for (var _i = 0, fields_1 = fields; _i < fields_1.length; _i++) {
      var s = fields_1[_i];
      if (all.includes(s)) {
        ext.push(s);
      }
    }
    if (ext.length === 0) {
      return undefined;
    }
    else {
      return ext;
    }
  }
  else {
    return fields;
  }
}
exports.getFields = getFields;
function buildFields(fields, all) {
  var s = getFields(fields, all);
  if (!s || s.length === 0) {
    return '*';
  }
  else {
    return s.join(',');
  }
}
exports.buildFields = buildFields;
function getMapField(name, mp) {
  if (!mp) {
    return name;
  }
  var x = mp[name];
  if (!x) {
    return name;
  }
  if (typeof x === 'string') {
    return x;
  }
  return name;
}
exports.getMapField = getMapField;
function isEmpty(s) {
  return !(s && s.length > 0);
}
exports.isEmpty = isEmpty;
var StringService = (function () {
  function StringService(pool, table, column) {
    this.pool = pool;
    this.table = table;
    this.column = column;
    this.load = this.load.bind(this);
    this.save = this.save.bind(this);
  }
  StringService.prototype.load = function (key, max) {
    var _this = this;
    var s = "select " + this.column + " from " + this.table + " where " + this.column + " ilike $1 order by " + this.column + " fetch next " + max + " rows only";
    return query(this.pool, s, ['' + key + '%']).then(function (arr) {
      return arr.map(function (i) { return i[_this.column]; });
    });
  };
  StringService.prototype.save = function (values) {
    if (!values || values.length === 0) {
      return Promise.resolve(0);
    }
    var arr = [];
    for (var i = 1; i <= values.length; i++) {
      arr.push('($' + i + ')');
      i++;
    }
    var s = "insert into " + this.table + "(" + this.column + ")values" + arr.join(',') + " on conflict do nothing";
    return exec(this.pool, s, values);
  };
  return StringService;
}());
exports.StringService = StringService;
function version(attrs) {
  var ks = Object.keys(attrs);
  for (var _i = 0, ks_1 = ks; _i < ks_1.length; _i++) {
    var k = ks_1[_i];
    var attr = attrs[k];
    if (attr.version) {
      attr.name = k;
      return attr;
    }
  }
  return undefined;
}
exports.version = version;
var PostgreSQLWriter = (function () {
  function PostgreSQLWriter(pool, table, attributes, toDB, buildParam) {
    this.table = table;
    this.attributes = attributes;
    this.write = this.write.bind(this);
    if (typeof pool === 'function') {
      this.exec = pool;
    }
    else {
      this.pool = pool;
    }
    this.param = buildParam;
    this.map = toDB;
    var x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  PostgreSQLWriter.prototype.write = function (obj) {
    if (!obj) {
      return Promise.resolve(0);
    }
    var obj2 = obj;
    if (this.map) {
      obj2 = this.map(obj);
    }
    var stmt = build_1.save(obj2, this.table, this.attributes, this.version, this.param);
    if (stmt) {
      if (this.exec) {
        return this.exec(stmt.query, stmt.args);
      }
      else {
        return exec(this.pool, stmt.query, stmt.args);
      }
    }
    else {
      return Promise.resolve(0);
    }
  };
  return PostgreSQLWriter;
}());
exports.PostgreSQLWriter = PostgreSQLWriter;
var PostgreSQLBatchWriter = (function () {
  function PostgreSQLBatchWriter(pool, table, attributes, toDB, buildParam) {
    this.table = table;
    this.attributes = attributes;
    this.write = this.write.bind(this);
    if (typeof pool === 'function') {
      this.execute = pool;
    }
    else {
      this.pool = pool;
    }
    this.param = buildParam;
    this.map = toDB;
    var x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  PostgreSQLBatchWriter.prototype.write = function (objs) {
    if (!objs || objs.length === 0) {
      return Promise.resolve(0);
    }
    var list = objs;
    if (this.map) {
      list = [];
      for (var _i = 0, objs_2 = objs; _i < objs_2.length; _i++) {
        var obj = objs_2[_i];
        var obj2 = this.map(obj);
        list.push(obj2);
      }
    }
    var stmts = build_1.saveBatch(list, this.table, this.attributes, this.version, this.param);
    if (stmts && stmts.length > 0) {
      if (this.execute) {
        return this.execute(stmts);
      }
      else {
        return execute(this.pool, stmts);
      }
    }
    else {
      return Promise.resolve(0);
    }
  };
  return PostgreSQLBatchWriter;
}());
exports.PostgreSQLBatchWriter = PostgreSQLBatchWriter;
var PostgreSQLChecker = (function () {
  function PostgreSQLChecker(pool, service, timeout) {
    this.pool = pool;
    this.service = service;
    this.timeout = timeout;
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
  PostgreSQLChecker.prototype.check = function () {
    return __awaiter(this, void 0, void 0, function () {
      var obj, promise;
      var _this = this;
      return __generator(this, function (_a) {
        switch (_a.label) {
          case 0:
            obj = {};
            return [4, this.pool.connect()];
          case 1:
            _a.sent();
            promise = new Promise(function (resolve, reject) {
              _this.pool.query('select now()', function (err, result) {
                if (err) {
                  return reject(err);
                }
                else {
                  resolve(obj);
                }
              });
            });
            if (this.timeout > 0) {
              return [2, promiseTimeOut(this.timeout, promise)];
            }
            else {
              return [2, promise];
            }
            return [2];
        }
      });
    });
  };
  PostgreSQLChecker.prototype.name = function () {
    return this.service;
  };
  PostgreSQLChecker.prototype.build = function (data, err) {
    if (err) {
      if (!data) {
        data = {};
      }
      data['error'] = err;
    }
    return data;
  };
  return PostgreSQLChecker;
}());
exports.PostgreSQLChecker = PostgreSQLChecker;
function promiseTimeOut(timeoutInMilliseconds, promise) {
  return Promise.race([
    promise,
    new Promise(function (resolve, reject) {
      setTimeout(function () {
        reject("Timed out in: " + timeoutInMilliseconds + " milliseconds!");
      }, timeoutInMilliseconds);
    })
  ]);
}
