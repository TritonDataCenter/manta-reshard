#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var lib_status = require('../lib/status');


function
get_status()
{
	var s = new lib_status.Status();

	s.update('processing phase "get_status"');
	s.prop('abc', 'def');

	for (var i = 0; i < 12; i++) {
		var c = s.child();

		c.update('first level child task %d', i);
		c.prop('xxx', 'zzzzz ' + i);
		c.prop('yyy', 'asiodfjasid ' + i);

		var nc = Math.round(Math.random() * 8);

		for (var j = 0; j < nc; j++) {
			var cc = c.child();

			cc.update('second level child task %d', j);

			var ncc = Math.round(Math.random() * 3);

			for (var k = 0; k < ncc; k++) {
				var ccc = cc.child();

				ccc.update('third level task %d', k);
				ccc.prop('final', 'hah!');
			}
		}
	}

	return (s);
}


var s = get_status();

lib_status.pretty_print(s.dump());
