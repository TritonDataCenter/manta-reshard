/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 * Copyright 2024 MNX Cloud, Inc.
 */


var mod_assert = require('assert-plus');
var mod_extsprintf = require('extsprintf');
var mod_jsprim = require('jsprim');

var sprintf = mod_extsprintf.sprintf;


var ID = 0;

var UTF8 = false;
if ((process.env.LANG && process.env.LANG.match(/utf-8/i)) ||
    (process.env.LC_ALL && process.env.LC_ALL.match(/utf-8/i))) {
	UTF8 = true;
}

function
Status()
{
	var self = this;

	self.sts_id = ++ID;
	self.sts_parent = null;
	self.sts_msg = null;
	self.sts_children = [];
	self.sts_active = true;
	self.sts_props = {};
}

Status.prototype.dump = function
dump()
{
	var self = this;

	if (!self.sts_active || self.sts_msg === null) {
		return (null);
	}

	var d = {
		m: self.sts_msg,
		c: self.sts_children.map(function (c) {
			return (c.dump());
		}).filter(function (a) {
			return (a !== null);
		}),
		p: mod_jsprim.deepCopy(self.sts_props)
	};

	return (d);
};

Status.prototype.child = function
child()
{
	var self = this;

	/*
	 * XXX
	 */
	var ch = new Status();
	ch.sts_parent = self;
	ch.sts_msg = null;

	self.sts_children.push(ch);

	return (ch);
};

Status.prototype.prop = function
prop(k, v)
{
	var self = this;

	mod_assert.string(k, 'k');

	if (v === null) {
		delete self.sts_props[k];
		return;
	}

	mod_assert.string(v, 'v');

	self.sts_props[k] = v;
};

Status.prototype.clear = function
clear()
{
	var self = this;

	self.sts_props = {};
};

Status.prototype.trunc = function
trunc()
{
	var self = this;

	self.sts_children = [];
};

Status.prototype.update = function
update()
{
	var self = this;

	var args = Array.prototype.slice.call(arguments);
	self.sts_msg = mod_extsprintf.sprintf.apply(mod_extsprintf, args);
};

/*
 * Remove this status child from the hierarchy.
 */
Status.prototype.done = function
done()
{
	var self = this;

	self.sts_active = false;
};



function
status_print_indent(stk, clip, g)
{
	var out = '';

	for (var i = 1; i < stk.length; i++) {
		if (stk[i]) {
			out += g.utf8 ? '│   ' : '|    ';
		} else {
			out += '    ';
		}
	}

	if (clip) {
		out = out.substr(0, out.length - 3);
	}

	return (out);
}

// Replacement for monowrap. See TRITON-2433.
// This function was lifted from
// https://github.com/IonicaBizau/wrap-text/blob/master/lib/index.js
// (which uses the MIT license) and made compatible with node v4.
function
monowrap(input, width)
{
    width = parseInt(width) || 80;
    var res = []
      , cLine = ""
      , words = input.split(" ")
      ;

    for (var i = 0; i < words.length; ++i) {
        var cWord = words[i];
        if ((cLine + cWord).length <= width) {
            cLine += (cLine ? " " : "") + cWord;
        } else {
            res.push(cLine);
            cLine = cWord;
        }
    }

    if (cLine) {
        res.push(cLine);
    }

    return res.join("\n");
}

function
wrap_text(msg, indent_width)
{
	if (process.stdout.isTTY) {
		/*
		 * This process is interactive.  Wrap text to fit in the
		 * terminal.
		 */
		var cols = Number(process.env.COLUMNS);
		if (!cols)
			cols = process.stdout.columns;
		var w = cols - indent_width - 2;

		msg = monowrap(msg, w);
	}

	return (msg.split('\n'));
}

function
status_print_level(d, opts, g)
{
	var peri = '';
	var nstk = opts.stk.concat([]);
	if (opts.stk.length > 0) {
		if (!opts.last) {
			peri = g.utf8 ? '├── ' : '+-- ';
			nstk.push(true);
		} else {
			peri = g.utf8 ? '└── ' : '\\-- ';
			nstk.push(false);
		}
	} else {
		nstk.push(false);
	}
	var pstk = nstk.concat([ d.c.length > 0 ]);
	var pern = status_print_indent(pstk, true, g);

	/*
	 * If this is an interactive process, wrap text to fit in the terminal.
	 */
	var msg = wrap_text(d.m, opts.stk.length * 4);

	msg.forEach(function (l, i) {
		if (i === 0) {
			console.log('%s%s%s', status_print_indent(opts.stk,
			    false, g), peri, l);
		} else {
			console.log(g.utf8 ? '%s…%s' : '%s %s', pern, l);
		}
	});

	var did = false;
	mod_jsprim.forEachKey(d.p, function (k, v) {
		var row = wrap_text(sprintf('%s: %s', k, v),
		    pstk.length * 4 - 2);

		row.forEach(function (l, i) {
			var fmt;
			if (i === 0) {
				fmt = g.utf8 ? '%s· %s' : '%s* %s';
			} else {
				fmt = g.utf8 ? '%s …%s' : '%s  %s';
			}
			console.log(fmt, pern, l);
		});

		did = true;
	});
	if (did) {
		console.log('%s', pern);
	}

	for (var i = 0; i < d.c.length; i++) {
		var c = d.c[i];

		var nopts = {
			last: i === d.c.length - 1,
			stk: nstk
		};

		status_print_level(c, nopts, g);
	}
}

function
pretty_print(status_dump, extra_opts)
{
	var g = { utf8: UTF8 };
	if (extra_opts && extra_opts.force_utf8) {
		g.utf8 = true;
	}

	return (status_print_level(status_dump, { stk: [] }, g));
}


module.exports = {
	Status: Status,
	pretty_print: pretty_print,
};
