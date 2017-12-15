


var mod_path = require('path');
var mod_fs = require('fs');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var VE = mod_verror.VError;


function
Template(required, compiled)
{
	var self = this;

	mod_assert.arrayOfString(required, 'required');
	mod_assert.arrayOfObject(compiled, 'compiled');

	self.tmpl_required = required;
	self.tmpl_compiled = compiled;
}

Template.prototype.render = function
render(opts)
{
	var self = this;

	mod_assert.object(opts, 'opts');

	/*
	 * Check input options.
	 */
	var missing = self.tmpl_required.filter(function (k) {
		return (typeof (opts[k]) !== 'string');
	});

	if (missing.length !== 0) {
		throw (new VE('missing properties: %j', missing));
	}

	/*
	 * Process the template to produce an output string.
	 */
	var out = '';

	self.tmpl_compiled.forEach(function (t) {
		switch (t.t) {
		case 'string':
			out += t.v;
			break;

		case 'expand':
			out += opts[t.v];
			break;

		default:
			throw (new VE('invalid t: %s', t.t));
		}
	});

	return (out);
};


function
compile(template)
{
	mod_assert.string(template, 'template');

	var state = 'REST';
	var pos = 0;
	var compiled = [];
	var accum = '';

	var commit = function (type) {
		if (accum !== '') {
			compiled.push({ t: type, v: accum });
			accum = '';
		}
	};

	for (;;) {
		if (pos >= template.length) {
			break;
		}

		var c = template[pos++];

		switch (state) {
		case 'REST':
			if (c === '%') {
				commit('string');
				state = 'PERCENT';
				break;
			}
			accum += c;
			break;

		case 'PERCENT':
			if (c === '%') {
				state = 'PROPERTY';
				break;
			}
			accum += '%';
			accum += c;
			state = 'REST';
			break;

		case 'PROPERTY':
			if (c === '\n') {
				return (new VE('unexpected newline'));
			}
			if (c === '%') {
				state = 'PERCENT2';
				break;
			}
			accum += c;
			break;

		case 'PERCENT2':
			if (c === '%') {
				commit('expand');
				state = 'REST';
				break;
			}
			return (new VE('unexpected character after "%"'));
		}
	}

	if (state !== 'REST') {
		return (new VE('unexpected terminal state: %s', state));
	}
	commit('string');

	/*
	 * Determine the full set of required properties.
	 */
	var props = [];
	for (var i = 0; i < compiled.length; i++) {
		if (compiled[i].t === 'expand') {
			if (props.indexOf(compiled[i].v) === -1) {
				props.push(compiled[i].v);
			}
		}
	}

	return (new Template(props, compiled));
}

function
template_load(name, callback)
{
	var path = mod_path.join(__dirname, '..', 'templates', name);
	var template;

	mod_vasync.waterfall([ function (done) {
		/*
		 * Load template from disk.
		 */
		mod_fs.readFile(path, { encoding: 'utf8' },
		    function (err, data) {
			if (err) {
				done(new VE(err, 'reading'));
				return;
			}

			done(null, data);
		});

	}, function (data, done) {
		/*
		 * Compile the template.
		 */
		template = compile(data);
		if (template instanceof Error) {
			done(new VE(template, 'compiling'));
			return;
		}

		setImmediate(done);

	} ], function (err) {
		if (err) {
			callback(new VE(err, 'loading template "%s"', name));
			return;
		}

		callback(null, template);
	});
}

module.exports = {
	template_load: template_load,
};
