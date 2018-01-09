

var mod_verror = require('verror');

var lib_common = require('../lib/common');
var lib_http_client = require('../lib/http_client');

var VE = mod_verror.VError;


if (process.argv.length !== 3 || !lib_common.is_uuid(process.argv[2])) {
	console.error('ERROR: Usage: get <plan_uuid>');
	process.exit(1);
}

lib_http_client.http_get('127.0.0.1', 80, '/plans', function (err, res) {
	if (err) {
		lib_http_client.cmd_print_result(err, res);
		return;
	}

	var plan = res.plans[process.argv[2]];
	if (!plan) {
		console.error('ERROR: plan %s not found', process.argv[2]);
		process.exit(1);
	}

	delete plan.status;

	console.log(JSON.stringify(plan, null, 4));
});
