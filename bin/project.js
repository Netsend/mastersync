#!/usr/bin/env node

/**
 * Copyright 2016 Netsend.
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

'use strict';

var program = require('commander');
var LDJSONStream = require('ld-jsonstream');

program
  .version('0.0.1')
  .usage('attr1 [attr2 ...]')
  .description('read newline delimited json objects on stdin and project given attributes on stdout')
  .parse(process.argv);

if (!program.args[0]) { program.help(); }

var ls = new LDJSONStream();
process.stdin.pipe(ls).on('readable', function() {
  var obj = ls.read();
  if (!obj) { return; } // end reached

  var newObj = {};
  program.args.forEach(function(key) {
    if (obj.hasOwnProperty(key)) {
      newObj[key] = obj[key];
    }
  });

  process.stdout.write(JSON.stringify(newObj) + '\n');
});
