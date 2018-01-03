

var mod_assert = require('assert-plus');
var mod_extsprintf = require('extsprintf');

var ID = 0;

function
Status()
{
	var self = this;

	self.sts_id = ++ID;
	self.sts_parent = null;
	self.sts_msg = null;
	self.sts_children = [];
	self.sts_active = true;
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
		})
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
status_print_indent(stk)
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
status_print_level(d, opts)
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

	console.log('%s%s%s', status_print_indent(opts.stk), peri, d.m);

	for (var i = 0; i < d.c.length; i++) {
		var c = d.c[i];

		var nopts = {
			last: i === d.c.length - 1,
			stk: nstk
		};

		status_print_level(c, nopts);
	}
}

function
pretty_print(status_dump)
{
	return (status_print_level(status_dump, { stk: [] }));
}

module.exports = {
	Status: Status,
	pretty_print: pretty_print,
};
