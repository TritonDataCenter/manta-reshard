#!/usr/bin/env node

var mod_fs = require('fs');
var mod_util = require('util');

var mod_verror = require('verror');

var lib_manatee_adm = require('../lib/manatee_adm');



var data = mod_fs.readFileSync(process.argv[2]).toString('utf8');

var result = lib_manatee_adm.parse_manatee_adm_show(data);


if (result instanceof Error) {
	var info = mod_verror.info(result);
	var stack = mod_verror.fullStack(result);

	console.error('ERROR: %s', stack);
	console.error('\tinfo: %s', mod_util.inspect(info, false, 10, true));
	process.exit(1);
}

console.log('result:\n%s', mod_util.inspect(result, false, 10, true));
