#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_util = require('util');

var mod_verror = require('verror');

var lib_manatee_adm = require('../lib/manatee_adm');


var result = lib_manatee_adm.generate_cluster_state({
	peers: [
		{ zone: '884697f8-d5e6-40ab-8023-86f96879db43',
		    ip: '10.20.1.8' },
		{ zone: '71f9bd05-9296-496b-b46a-7cc51ca008c6',
		    ip: '10.20.2.8' },
		{ zone: 'e2adddb2-22cf-4861-a120-478ea60f706d',
		    ip: '10.20.0.8' },
	],
	init_wal: '0/00000000',
	plan_uuid: 'cb3208bc-837f-45c1-ab23-62add438d051'
});

if (result instanceof Error) {
	var info = mod_verror.info(result);
	var stack = mod_verror.fullStack(result);

	console.error('ERROR: %s', stack);
	console.error('\tinfo: %s', mod_util.inspect(info, false, 10, true));
	process.exit(1);
}

console.log('result:\n%s', JSON.stringify(result, null, 2));
