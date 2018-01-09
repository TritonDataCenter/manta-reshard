

var mod_verror = require('verror');

var lib_common = require('../lib/common');
var lib_http_client = require('../lib/http_client');

var VE = mod_verror.VError;


if (process.argv.length !== 3 || !lib_common.is_uuid(process.argv[2])) {
	console.error('ERROR: Usage: pause <plan_uuid>');
	process.exit(1);
}

lib_http_client.http_post('127.0.0.1', 80,
    '/plan/' + process.argv[2] + '/pause', null,
    lib_http_client.cmd_print_result);
