#!/usr/bin/env node

var lib_status = require('../lib/status');


function
get_status()
{
	var s = new lib_status.Status();

	s.update('processing phase "get_status"');

	for (var i = 0; i < 12; i++) {
		var c = s.child();

		c.update('first level child task %d', i);

		var nc = Math.round(Math.random() * 8);

		for (var j = 0; j < nc; j++) {
			var cc = c.child();

			cc.update('second level child task %d', j);

			var ncc = Math.round(Math.random() * 3);

			for (var k = 0; k < ncc; k++) {
				var ccc = cc.child();

				ccc.update('third level task %d', k);
			}
		}
	}

	return (s);
}


var s = get_status();

pretty_print(s);


function
lvlsp(stk)
{
	var out = '';

	for (var i = 1; i < stk.length; i++) {
		if (stk[i]) {
			out += '│   ';
		} else {
			out += '    ';
		}
	}

	return (out);
}

function
pp(d, opts)
{
	var peri = '';
	var nstk = opts.stk.concat([]);
	if (opts.stk.length > 0) {
		if (!opts.last) {
			peri = '├── ';
			nstk.push(true);
		} else {
			peri = '└── ';
			nstk.push(false);
		}
	} else {
		nstk.push(false);
	}

	console.log('%s%s%s', lvlsp(opts.stk), peri, d.m);

	for (var i = 0; i < d.c.length; i++) {
		var c = d.c[i];

		var nopts = {
			last: i === d.c.length - 1,
			stk: nstk
		};

		pp(c, nopts);
	}
}

function
pretty_print(status)
{
	return (pp(status.dump(), { stk: [] }));
}
