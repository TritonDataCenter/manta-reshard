#!/usr/bin/env node

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

		var nc = Math.round(Math.random() * 1);

		for (var j = 0; j < nc; j++) {
			var cc = c.child();

			cc.update('second level child task %d', j);
			cc.prop('super wide property',
			    'this is so very, very wide -- really _quite_ ' +
			    'wide, all up!');
			cc.prop('and', 'another');

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
