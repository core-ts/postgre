"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var build_1 = require("./build");
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
var PostgreWriter = (function () {
  function PostgreWriter(table, attributes, exec, map, buildParam) {
    this.table = table;
    this.attributes = attributes;
    this.exec = exec;
    this.map = map;
    this.buildParam = buildParam;
    this.write = this.write.bind(this);
    var x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  PostgreWriter.prototype.write = function (obj) {
    if (!obj) {
      return Promise.resolve(0);
    }
    var obj2 = obj;
    if (this.map) {
      obj2 = this.map(obj);
    }
    var stmt = build_1.save(obj2, this.table, this.attributes, this.version, this.buildParam);
    if (stmt) {
      return this.exec(stmt.query, stmt.args);
    }
    else {
      return Promise.resolve(0);
    }
  };
  return PostgreWriter;
}());
exports.PostgreWriter = PostgreWriter;
var PostgreBatchWriter = (function () {
  function PostgreBatchWriter(table, attributes, execute, map, buildParam) {
    this.table = table;
    this.attributes = attributes;
    this.execute = execute;
    this.map = map;
    this.buildParam = buildParam;
    this.write = this.write.bind(this);
    var x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  PostgreBatchWriter.prototype.write = function (objs) {
    if (!objs || objs.length === 0) {
      return Promise.resolve(0);
    }
    var list = objs;
    if (this.map) {
      list = [];
      for (var _i = 0, objs_1 = objs; _i < objs_1.length; _i++) {
        var obj = objs_1[_i];
        var obj2 = this.map(obj);
        list.push(obj2);
      }
    }
    var stmts = build_1.saveBatch(list, this.table, this.attributes, this.version, this.buildParam);
    if (stmts && stmts.length > 0) {
      return this.execute(stmts);
    }
    else {
      return Promise.resolve(0);
    }
  };
  return PostgreBatchWriter;
}());
exports.PostgreBatchWriter = PostgreBatchWriter;
