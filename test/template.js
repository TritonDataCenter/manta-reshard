#!/usr/bin/env node

var mod_fs = require('fs');
var mod_util = require('util');

var mod_verror = require('verror');

var lib_template = require('../lib/template');

var VE = mod_verror.VError;

var opts = {};

process.argv.slice(3).forEach(function (arg) {
	var t = arg.split('=');

	if (t.length !== 2) {
		console.error('ERROR: invalid argument "%s"', t);
		process.exit(1);
	}

	opts[t[0]] = t[1];
});


lib_template.template_load(process.argv[2], function (err, t) {
	if (err) {
		console.error('ERROR: %s', VE.fullStack(err));
		process.exit(1);
	}

	var data;
	try {
		data = t.render(opts);
	} catch (ex) {
		console.log('ERROR: %s', VE.fullStack(ex));
		process.exit(1);
	}

	console.log('%s', data);
});
