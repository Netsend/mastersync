/**
 * Copyright 2015 Netsend.
 *
 * This file is part of Mastersync.
 *
 * Mastersync is free software: you can redistribute it and/or modify it under the
 * terms of the GNU Affero General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * Mastersync is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with Mastersync. If not, see <https://www.gnu.org/licenses/>.
 */

/* jshint -W116 */

'use strict';

var crypto = require('crypto');

var async = require('async');
var bson = require('bson');
var BSON = new bson.BSONPure.BSON();
var through2 = require('through2');
var isEqual = require('is-equal');

var t = require('./core-util-is-fork');
var Tree = require('./tree');
var merge = require('./merge');
var invalidLocalHeader = require('./invalid_local_header');
var streamify = require('./streamify');
var ConcatReadStream = require('./concat_read_stream');

var noop = function() {};

// wrapper around streamify that supports "reopen" recursively
function streamifier(dag) {
  var result = streamify(dag);
  result.reopen = function() {
    return streamifier(dag);
  };
  return result;
}

/**
 * MergeTree
 *
 * Accept objects from different perspectives. Merge other perspectives into the
 * local perspective.
 *
 * If local updates can come in, as well as updates by other perspectives then a
 * mergeHandler should be provided. This function is called with every newly
 * created merge (either by fast-forward or three-way-merge). Make sure any
 * processed merges are written back to this merge tree (createLocalWriteStream)
 * in the same order as mergeHandler was called. It's ok to miss some items as
 * long as the order does not change.
 *
 * @param {LevelUP.db} db  database for persistent storage
 * @param {Object} [opts]  object containing configurable parameters
 *
 * opts:
 *   local {String, default "_local"}  name of the local tree, should not exceed
 *                                     254 bytes
 *   stage {String, default "_stage"}  name of the staging tree, should not
 *                                     exceed 254 bytes
 *   perspectives {Array}  Names of different sources that should be merged to
 *                         the local tree. A name should not exceed 254 bytes.
 *   mergeHandler {Function}  function that should handle newly created merges
 *                            signature: function (merged, lhead, next)
 *   vSize {Number, default 6}  number of bytes used for the version. Should be:
 *                              0 < vSize <= 6
 *   iSize {Number, default 6}  number of bytes used for i. Should be:
 *                              0 < iSize <= 6
 *   transform {Function}  transformation function to run on each item
 *                         signature: function(item, cb) cb should be called
 *                         with an error or a (possibly transformed) item
 *   log {Object, default console}  log object that contains debug2, debug, info,
 *       notice, warning, err, crit and emerg functions. Uses console.log and
 *       console.error by default.
 */
function MergeTree(db, opts) {
  if (typeof db !== 'object' || db === null) { throw new TypeError('db must be an object'); }

  if (opts == null) { opts = {}; }
  if (typeof opts !== 'object' || Array.isArray(opts)) { throw new TypeError('opts must be an object'); }

  if (opts.local != null && typeof opts.local !== 'string') { throw new TypeError('opts.local must be a string'); }
  if (opts.stage != null && typeof opts.stage !== 'string') { throw new TypeError('opts.stage must be a string'); }
  if (opts.perspectives != null && !Array.isArray(opts.perspectives)) { throw new TypeError('opts.perspectives must be an array'); }
  if (opts.mergeHandler != null && typeof opts.mergeHandler !== 'function') { throw new TypeError('opts.mergeHandler must be a function'); }
  if (opts.log != null && typeof opts.log !== 'object') { throw new TypeError('opts.log must be an object'); }
  if (opts.vSize != null && typeof opts.vSize !== 'number') { throw new TypeError('opts.vSize must be a number'); }
  if (opts.iSize != null && typeof opts.iSize !== 'number') { throw new TypeError('opts.iSize must be a number'); }
  if (opts.transform != null && typeof opts.transform !== 'function') { throw new TypeError('opts.transform must be a function'); }

  opts.objectMode = true;

  this._localName = opts.local || '_local';
  this._stageName = opts.stage || '_stage';
  this._perspectives = opts.perspectives || [];
  this._transform = opts.transform || function(item, cb) { cb(null, item); };

  if (Buffer.byteLength(this._localName) > 254) { throw new Error('opts.local must not exceed 254 bytes'); }
  if (Buffer.byteLength(this._stageName) > 254) { throw new Error('opts.stage must not exceed 254 bytes'); }

  if (this._localName === this._stageName) { throw new Error('local and stage names can not be the same'); }

  var that = this;

  this._perspectives.forEach(function(perspective) {
    if (Buffer.byteLength(perspective) > 254) { throw new Error('each perspective name must not exceed 254 bytes'); }
    if (perspective === that._localName) { throw new Error('every perspective should have a name that differs from the local name'); }
    if (perspective === that._stageName) { throw new Error('every perspective should have a name that differs from the stage name'); }
  });

  this._vSize = opts.vSize || 6;
  this._iSize = opts.iSize || 6;

  if (opts.mergeHandler) {
    this._mergeHandler = opts.mergeHandler;
  } else {
    // emediately confirm writes if no mergehandler is provided
    var writable = this.createLocalWriteStream();
    this._mergeHandler = function(merged, lhead, next) { writable(merged, next); };
  }

  if (this._vSize < 0 || this._vSize > 6) { throw new Error('opts.vSize must be between 0 and 6'); }
  if (this._iSize < 0 || this._iSize > 6) { throw new Error('opts.iSize must be between 0 and 6'); }

  this._db = db;

  this._log = opts.log || {
    emerg:   console.error,
    alert:   console.error,
    crit:    console.error,
    err:     console.error,
    warning: console.log,
    notice:  console.log,
    info:    console.log,
    debug:   console.log,
    debug2:  console.log,
    getFileStream: noop,
    getErrorStream: noop,
    close: noop
  };

  // create trees
  this._pe = {};

  var treeOpts = {
    vSize: this._vSize,
    iSize: this._iSize,
    log: this._log
  };

  this._perspectives.forEach(function(perspective) {
    that._pe[perspective] = new Tree(db, perspective, treeOpts);
  });

  this._local = new Tree(db, this._localName, treeOpts);
  this._stage = new Tree(db, this._stageName, {
    vSize: this._vSize,
    iSize: this._iSize,
    log: this._log,
    skipValidation: true
  });
}

module.exports = MergeTree;

/**
 * Save a new version of a certain perspective in the appropriate Tree.
 *
 * New items should have the following structure:
 * {
 *   h: {Object}  header containing the following values:
 *     id:  {mixed}  id of this h
 *     v:   {base64 String}  version
 *     pa:  {Array}  parent versions
 *     pe:  {String}  perspective
 *     [d]: {Boolean}  true if this id is deleted
 *   [m]: {Object}  meta info to store with this document
 *   [b]: {mixed}  document to save
 * }
 *
 * @param {Object} item  item to save
 * @param {Function} cb  Callback that is called once the item is saved. First
 *                       parameter will be an error object or null.
 */
MergeTree.prototype.createRemoteWriteStream = function createRemoteWriteStream() {
  var that = this;
  var error;

  return through2.obj(function(item, encoding, cb) {
    if (typeof item !== 'object') {
      process.nextTick(function() {
        cb(new TypeError('item must be an object'));
      });
      return;
    }
    if (typeof cb !== 'function') {
      process.nextTick(function() {
        cb(new TypeError('cb must be a function'));
      });
      return;
    }

    var pe;

    try {
      pe = item.h.pe;
      if (typeof pe !== 'string') {
        throw new TypeError();
      }
    } catch(err) {
      process.nextTick(function() {
        cb(new TypeError('item.h.pe must be a string'));
      });
      return;
    }

    if (pe === that._localName) {
      error = 'perspective should differ from local name';
      process.nextTick(function() {
        that._log.err('merge_tree %s %s', error, pe);
        cb(new Error(error));
      });
      return;
    }

    if (pe === that._stageName) {
      error = 'perspective should differ from stage name';
      process.nextTick(function() {
        that._log.err('merge_tree %s %s', error, pe);
        cb(new Error(error));
      });
      return;
    }

    var tree = that._pe[pe];

    if (!tree) {
      error = 'perspective not found';
      process.nextTick(function() {
        that._log.err('merge_tree %s %s', error, pe);
        cb(new Error(error));
      });
      return;
    }

    that._pe[pe].write(item, cb);
  });
};

/**
 * Save a new version from the local perspective or a confirmation of a
 * handled merge (that originated from a remote) in the local tree.
 *
 * New items should have the following structure:
 * {
 *   h: {Object}  header containing the following values:
 *     id:  {mixed}  id of this item
 *     [v]: {base64 String}  supply version to confirm a handled merge or
 *                           deterministically set the version. If not set, a
 *                           new version will be generated based on content and
 *                           parents.
 *     [d]: {Boolean}  true if this id is deleted
 *   [m]: {mixed}  meta info to store with this document
 *   [b]: {mixed}  document to save
 * }
 *
 * @param {Object} item  item to save
 * @param {Function} cb  Callback that is called once the item is saved. First
 *                       parameter will be an error object or null.
 */
MergeTree.prototype.createLocalWriteStream = function createLocalWriteStream() {
  var that = this;

  return through2.obj(function(item, encoding, cb) {
    if (!t.isObject(item)) {
      process.nextTick(function() {
        cb(new TypeError('item must be an object'));
      });
      return;
    }
    if (!t.isFunction(cb)) {
      process.nextTick(function() {
        cb(new TypeError('cb must be a function'));
      });
      return;
    }

    var error = invalidLocalHeader(item.h);
    if (error) {
      process.nextTick(function() {
        cb(new TypeError('item.' + error));
      });
      return;
    }

    if (item.h.pa) {
      error = new Error('did not expect local item to have a parent defined');
      that._log.err('merge_tree createLocalWriteStream %s %j', error, item);
      process.nextTick(function() {
        cb(error);
      });
      return;
    }

    // use local and staging tree
    var local = that._local;
    var stage = that._stage;

    // check if this version is in the stage or not
    stage.getByVersion(item.h.v, function(err, exists) {
      if (err) { cb(err); return; }

      if (exists && isEqual(exists.b, item.b)) {
        // use header from item in stage and copy body and any meta info from the user supplied item
        exists.m = item.m;
        exists.b = item.b;
        item = exists;

        // ack, move everything up to this item to the local tree
        stage.iterateInsertionOrder({ id: item.h.id, last: item.h.v }, function(stageItem, next) {
          if (stageItem.h.pe) {
            // copy from perspective up to this item from stage
            local.lastByPerspective(stageItem.h.pe, 'base64', function(err, v) {
              if (err) { cb(err); return; }

              // see mergeWithLocal, if there is a parent, the first parent might point to a version that exists in the perspective it came from
              var opts = { last: item.h.v, excludeLast: true, transform: that._transform };
              if (v) {
                opts.first = v;
                opts.excludeFirst = true;
              }
              MergeTree._copyTo(that._pe[stageItem.h.pe], local, opts, function(err) {
                if (err) { cb(err); return; }

                // move item from stage to local
                local.write(stageItem, function(err) {
                  if (err) { next(err); return; }
                  stage.del(stageItem, next);
                });
              });
            });
          } else {
            // move item from stage to local
            local.write(stageItem, function(err) {
              if (err) { next(err); return; }
              stage.del(stageItem, next);
            });
          }
        }, cb);
      } else {
        // determine parent by last non-deleted and non-conflicting head in local
        var heads = [];
        local.getHeads({ id: item.h.id, skipDeletes: true, skipConflicts: true }, function(head, next) {
          heads.push(head.h.v);
          next();
        }, function(err) {
          if (err) { cb(err); return; }

          if (heads.length > 1) {
            error = 'more than one non-deleted and non-conflicting head in local tree';
            that._log.err('merge_tree createLocalWriteStream %s %j', error, item);
            cb(new Error(error));
            return;
          }

          var newItem = {
            h: {
              id: item.h.id,
              pa: heads
            }
          };

          if (item.b) {
            newItem.b = item.b;
          }

          if (item.m) {
            newItem.m = item.m;
          }

          // generate new version based on content
          if (item.h.v) {
            newItem.h.v = item.h.v;
          } else {
            newItem.h.v = MergeTree.versionContent(item, that._vSize);
          }

          if (item.h.d) {
            newItem.h.d = true;
          }

          local.write(newItem, cb);
        });
      }
    });
  });
};

/**
 * Merge src tree with the local tree using an intermediate staging tree. Merge
 * every new head with any previous head in stage. Call merge handler with the
 * new head and the previous head.
 * If there is a conflict, add the original shead to stage with h.c set, but
 * don't call merge handler with it. This is to ensure insertion order of stree.
 *
 * Runs transform on each item before saving to staging.
 *
 * Merge handler and transform can be provided to the constructor.
 *
 * @param {Object} stree  source tree to merge
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree.prototype.mergeWithLocal = function mergeWithLocal(stree, cb) {
  if (typeof stree !== 'object' || Array.isArray(stree)) { throw new TypeError('stree must be an object'); }
  if (typeof cb !== 'function') { throw new TypeError('cb must be a function'); }

  var stage = this._stage;
  var that = this;
  var error;

  MergeTree._mergeTrees(stree, this._local, function iterator(smerge, dmerge, shead, dhead, next) {
    if (shead.h.pe !== stree.name) {
      error = new Error('shead and stree perspective mismatch');
      that._log.err('mergeWithLocal %s %j', error, shead);
      next(error);
      return;
    }

    that._transform(shead, function(err, nitem) {
      if (err) {
        that._log.err('mergeWithLocal transform shead error %s %j', err, shead);
        next(err);
        return;
      }

      shead = nitem;

      // if there was a conflict, save shead in stage with conflict flag set
      if (Array.isArray(smerge)) {
        // set conflict flag
        shead.h.c = true;
        stage.write(shead, next);
        return;
      }

      // merge given head with non-deleted and non-conflicting head in stage
      function mergeNewHeadWithStage(newHead) {
        var headOpts = {
          id: newHead.h.id,
          skipDeletes: true,
          skipConflicts: true
        };
        var heads = [];
        stage.getHeads(headOpts, function(head, snext) {
          heads.push(head);
          snext();
        }, function(err) {
          if (err) { next(err); return; }

          that._log.debug('mergeWithLocal head in stage %j', heads);

          if (heads.length > 1) {
            next(new Error('more than one head in stage'));
            return;
          }

          // if no previous head in stage, insert new head in staging and call merge handler with the new head and the current head in the local tree
          if (!heads.length) {
            stage.write(newHead, function(err) {
              if (err) {
                // write error in stage
                that._log.err('mergeWithLocal stage write error %s %j', err, newHead);
                next(err);
                return;
              }
              that._log.debug('mergeWithLocal write first new head %j', newHead);
              that._mergeHandler(newHead, dhead, next);
            });
            return;
          }

          // merge with previous head
          var sX = new ConcatReadStream([streamifier(heads), stree.createReadStream({ id: newHead.h.id, reverse: true })]);
          var sY = new ConcatReadStream([streamifier(heads), stree.createReadStream({ id: newHead.h.id, reverse: true })]);

          var opts = {
            rootX: newHead,
            rootY: heads[0]
          };
          merge(sX, sY, opts, function(err, merged) {
            // if conflict, write new head with conflict flag set, don't call merge handler
            if (err) {
              that._log.err('mergeWithLocal staging merge error %s %j %j', err, newHead, heads[0]);
              // set conflict flag
              newHead.h.c = true;
              stage.write(newHead, next);
              return;
            }

            // if fast-forward, write new head and call merge handler with the new merged head and the previous head from stage
            if (merged.h.v && merged.h.v === newHead.h.v) {
              stage.write(newHead, function(err) {
                if (err) {
                  that._log.err('mergeWithLocal stage newHead write error %s %j %j', err, newHead);
                  next(err);
                  return;
                }
                that._log.debug('mergeWithLocal write new head by fast-forward %j %j', newHead, merged);
                that._mergeHandler(newHead, heads[0], next);
              });
              return;
            }

            // else insert new head and new merge and call merge handler with the new merged head and the previous head from stage
            that._log.debug('mergeWithLocal write new head %j and merge %j', newHead, merged);
            stage.write(newHead, function(err) {
              if (err) {
                that._log.err('mergeWithLocal stage newHead write error %s %j %j', err, newHead);
                next(err);
                return;
              }

              merged.h.v = MergeTree.versionContent(merged);

              stage.write(merged, function(err) {
                if (err) {
                  that._log.err('mergeWithLocal stage merge write error %s %j %j', err, newHead, merged);
                  next(err);
                  return;
                }
                that._mergeHandler(merged, heads[0], next);
              });
            });
          });
        });
      }

      // determine new item, merge, merge ff
      if (!smerge) {
        // merge ff
        mergeNewHeadWithStage(shead);
        return;
      }

      // merge
      that._transform(smerge, function(err, nitem) {
        if (err) {
          that._log.err('mergeWithLocal transform smerge error %s %j', err, smerge);
          next(err);
          return;
        }

        mergeNewHeadWithStage(nitem);
      });
    });
  }, cb);
};



/////////////////////
//// PRIVATE API ////
/////////////////////



/**
 * Merge src tree with dst tree. Find the last version of src tree that is in
 * dst tree and try to merge each head in src (in insertion order) with every
 * head of the DAG in dst tree. Call back with the head from src, the head from
 * dst and the merged result. If there is a merge conflict, smerge in the
 * iterator will be an array of conflicting attributes and no dmerge is set.
 *
 * @param {Object} stree  source tree to merge
 * @param {Object} dtree  dest tree to merge with
 * @param {Function} iterator  function(smerge, dmerge, shead, dhead, next) called with
 *                             merges from both perspectives and both heads a
 *                             merge is based on. merges are only created if not
 *                             a fast-forward
 *                             Fifth parameter is a next handler.
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree._mergeTrees = function _mergeTrees(stree, dtree, iterator, cb) {
  if (!t.isObject(stree)) { throw new TypeError('stree must be an object'); }
  if (!t.isObject(dtree)) { throw new TypeError('dtree must be an object'); }
  if (!t.isFunction(iterator)) { throw new TypeError('iterator must be a function'); }
  if (!t.isFunction(cb)) { throw new TypeError('cb must be a function'); }

  MergeTree._iterateMissing(stree, dtree, function(sitem, snext) {
    // is this new version a head in stree?
    stree.getHeads({ id: sitem.h.id }, function(shead, shnext) {
      if (shead.h.v !== sitem.h.v) {
        shnext();
        return;
      }

      // merge with all head(s) in dtree
      var dheads = [];
      dtree.getHeads({ id: sitem.h.id }, function(dhead, dnext) {
        dheads.push(dhead);
        dnext();
      }, function(err) {
        if (err) { shnext(err); return; }

        if (!dheads.length) {
          // new item not in dtree yet, fast-forward
          iterator(null, null, shead, null, shnext);
          return;
        }

        // merge with all dheads
        async.eachSeries(dheads, function(dhead, cb2) {
          var sX = stree.createReadStream({ id: shead.h.id, reverse: true });
          var sY = dtree.createReadStream({ id: dhead.h.id, reverse: true });
          var opts = {
            rootX: shead,
            rootY: dhead
          };
          merge(sX, sY, opts, function(err, smerge, dmerge) {
            if (err) {
              // signal merge conflict
              iterator(err.conflict, null, shead, dhead, cb2);
              return;
            }

            // this is either a new item, an existing item, or a merge by fast-forward
            if (!dmerge.h.v) {
              // merge, create a version based on content
              var version = MergeTree.versionContent(smerge);
              dmerge.h.v = version;
              smerge.h.v = version;
              iterator(smerge, dmerge, shead, dhead, cb2);
              return;
            } else if (!smerge.h.i || !dmerge.h.i) {
              if (smerge.h.v !== dmerge.h.v) {
                cb2(new Error('unexpected version mismatch'));
                return;
              }
              // merge by fast-forward
              iterator(smerge, dmerge, shead, dhead, cb2);
            } else {
              // existing item with h.v and h.i
              iterator(null, null, shead, dhead, cb2);
            }
          });
        }, shnext);
      });
    }, snext);
  }, cb);
};

/**
 * Start iterating over src starting at the last version of stree that is in dtree.
 *
 * Ensures insertion order of src.
 *
 * @param {Object} stree  source tree
 * @param {Object} dtree  destination tree
 * @param {Function} iterator  function(item, next) called with src item and
 *                             next handler
 * @param {Function} cb  First parameter will be an error object or null.
 */
MergeTree._iterateMissing = function _iterateMissing(stree, dtree, iterator, cb) {
  if (!t.isObject(stree)) { throw new TypeError('stree must be an object'); }
  if (!t.isObject(dtree)) { throw new TypeError('dtree must be an object'); }
  if (!t.isFunction(iterator)) { throw new TypeError('iterator must be a function'); }
  if (!t.isFunction(cb)) { throw new TypeError('cb must be a function'); }

  // determine offset
  dtree.lastByPerspective(stree.name, 'base64', function(err, v) {
    if (err) { cb(err); return; }

    var opts = {};
    if (v) {
      opts.first = v;
      opts.excludeFirst = true;
    }
    stree.iterateInsertionOrder(opts, iterator, cb);
  });
};

/**
 * Copy all items from stree to dtree. Maintains insertion order of stree.
 *
 * @param {Object} stree  source tree to search
 * @param {Object} dtree  destination tree
 * @param {Object} [opts]  object containing configurable parameters
 * @param {Function} cb  First parameter will be an error object or null.
 *
 * opts:
 *   first {base64 String}  first version that should be used
 *   last {base64 String}  last version to copy
 *   excludeFirst {Boolean, default false}  whether or not first should be
 *                                         excluded
 *   excludeLast {Boolean, default false}  whether or not last should be
 *                                         excluded
 *   transform {Function}  transformation function to run on each item
 *                         signature: function(item, cb2) cb2 should be called
 *                         with an optional error and a possibly transformed
 *                         item
 */
MergeTree._copyTo = function _copyTo(stree, dtree, opts, cb) {
  if (t.isFunction(opts)) {
    cb = opts;
    opts = null;
  }

  opts = opts || {};
  if (!t.isObject(stree)) { throw new TypeError('stree must be an object'); }
  if (!t.isObject(dtree)) { throw new TypeError('dtree must be an object'); }
  if (!t.isObject(opts)) { throw new TypeError('opts must be an object'); }
  if (!t.isFunction(cb)) { throw new TypeError('cb must be a function'); }

  opts.transform = opts.transform || function(item, cb2) { cb2(null, item); };

  var first = true;
  stree.iterateInsertionOrder({ first: opts.first }, function(item, next) {
    if (first) {
      first = false;
      if (opts.first && item.h.v === opts.first && opts.excludeFirst) {
        next();
        return;
      }
    }

    if (opts.last && item.h.v === opts.last && opts.excludeLast) {
      next(null, false);
      return;
    }

    opts.transform(item, function(err, nitem) {
      if (err) { next(err); return; }
      if (nitem === null || typeof nitem === 'undefined') {
        next();
        return;
      }

      // allow one error without bubbling up
      dtree.once('error', function() {});

      dtree.write(nitem, function(err) {
        if (err) { next(err); return; }
        if (opts.last && opts.last === item.h.v) {
          next(null, false);
        } else {
          next();
        }
      });
    });
  }, cb);
};

// create a content based version number
MergeTree.versionContent = function versionContent(item, vSize) {
  // determine vSize, assume this is base64
  vSize = vSize || item.h.pa[0].length * 6 / 8;
  if (vSize < 1) {
    throw new Error(new Error('version too small'));
  }
  var h = crypto.createHash('sha256');
  h.update(BSON.serialize(item), 'base64');
  // read first vSize bytes for version
  return h.digest().toString('base64', 0, vSize);
};
