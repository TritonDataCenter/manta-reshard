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
	s.prop('xyzzy', 'nothing happens');

	for (var i = 0; i < 4; i++) {
		var c = s.child();

		c.update('first level child task %d ' +
		    'aaaaaaaa ' +
		    'bbb ccc dddddd ee ffffg ggggggggg hhh ih ihiih ijsdf ' +
		    'jijfijf ii sissn nen wiqi jsijdij!', i);
		c.prop('xxx', 'zzzzz ' + i);
		c.prop('yyy', 'asiodfjasid ' + i);

		var nc = i;

		for (var j = 0; j < nc; j++) {
			var cc = c.child();

			cc.update('second level child task %d ' +
			    'asdjifjasidfj aisdjf iasdjf iasjdf iajsdfij', j);
			cc.prop('super wide property',
			    'this is so very, very wide -- really _quite_ ' +
			    'wide, all up!');
			cc.prop('and', 'another');

			var ncc = i - 1;

			for (var k = 0; k < ncc; k++) {
				var ccc = cc.child();

				ccc.update('third level task %d ' +
				    'xxx xxxxx xxxx xxxx xxxx xxxx xxxx ' +
				    'xxx xxxxx xxxx xxxx xxxx xxxx xxxx ', k);
				ccc.prop('final', 'hah!');
			}
		}
	}

	return (s);
}


var s = get_status();

lib_status.pretty_print(s.dump());
