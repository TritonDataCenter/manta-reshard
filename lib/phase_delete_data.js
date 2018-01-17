
var mod_net = require('net');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');

var lib_common = require('../lib/common');
var lib_template = require('../lib/template');
var lib_postgres = require('../lib/postgres');

var VE = mod_verror.VError;

function
phase_delete_data(ctl)
{
	var insts;
	var p = ctl.plan();
	var status = ctl.status();
	var hold = false;

	var vnode_data = null;
	var handle_post = function (body, callback) {
		if (vnode_data !== null) {
			if (mod_jsprim.deepEqual(vnode_data, body)) {
				/*
				 * If this POST contains the same data we
				 * received the first time, allow it.
				 */
				setImmediate(callback);
				return;
			}

			callback(new VE('second POST with mismatched data'));
			return;
		}

		var validate_vnode_array = function (shard_name) {
			if (!Array.isArray(body[shard_name])) {
				return (false);
			}

			if (body[shard_name].length < 1) {
				return (false);
			}

			for (var i = 0; i < body[shard_name].length; i++) {
				if (typeof (body[shard_name][i]) !== 'number') {
					return (false);
				}
			}

			return (true);
		};

		if (!validate_vnode_array(p.shard) ||
		    !validate_vnode_array(p.new_shard)) {
			callback(new VE('data invalid'));
			return;
		}

		vnode_data = body;

		callback();
	};
	var opts = {
		POST_URL: ctl.register_http_handler(handle_post),
		HASH_RING_IMAGE: ctl.prop_get('new_hash_ring_uuid'),
	};
	mod_assert.uuid(opts.HASH_RING_IMAGE, 'HASH_RING_IMAGE');

	var script;

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		status.update('loading script templates');
		lib_template.template_load('electric-moray-get-vnodes.sh',
		    done);

	}, function (_script, done) {
		if (ctl.pausing(done)) {
			return;
		}

		script = _script;

		status.update('refreshing "manta" SAPI application');
		ctl.get_manta_app(function (err, app) {
			if (err) {
				done(err);
				return;
			}

			opts.HASH_RING_IMGAPI_SERVICE =
			    app.metadata.HASH_RING_IMGAPI_SERVICE;
			mod_assert.string(opts.HASH_RING_IMGAPI_SERVICE,
			    'HASH_RING_IMGAPI_SERVICE');

			done();
		});

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		status.update('listing instances of "electric-moray"');
		ctl.get_instances({ service: 'electric-moray' },
		    function (err, _insts) {
			if (err) {
				done(err);
				return;
			}

			insts = _insts;
			done();
		});

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		var i = insts[Object.keys(insts)[0]];

		ctl.log.info({ script_opts: opts }, 'script options');

		status.update('extracting vnode list');
		status.prop('via zone', i.uuid);
		ctl.log.info('using Electric Moray zone %s for unpack',
		    i.uuid);
		ctl.zone_exec(i.uuid, script.render(opts),
		    function (err, res) {
			if (err) {
				done(err);
				return;
			}

			ctl.log.info({ result: res }, 'output');

			if (res.exit_status !== 0) {
				done(new VE({ info: {
				    stderr: res.stderr.trim() }},
				    'get vnodes failed'));
				return;
			}

			if (vnode_data === null) {
				done(new VE('script did not POST data'));
				return;
			}

			mod_assert.arrayOfNumber(vnode_data[p.shard],
			    'p.shard(' + p.shard +')');
			mod_assert.arrayOfNumber(vnode_data[p.new_shard],
			    'p.new_shard(' + p.new_shard +')');

			done();
		});

	}, function (done) {
		var sort_nums = function (a, b) {
			mod_assert.number(a, 'a');
			mod_assert.number(b, 'b');

			return (a < b ? -1 : a > b ? 1 : 0);
		};

		vnode_data[p.shard].sort(sort_nums);
		vnode_data[p.new_shard].sort(sort_nums);

		var summary = { vnode_count: {} };

		summary.vnode_count[p.shard] = vnode_data[p.shard].length;
		summary.vnode_count[p.new_shard] =
		    vnode_data[p.new_shard].length;

		ctl.log.info({ summary: summary }, 'vnode data summary');

		setImmediate(done);

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Get the list of buckets for this shard.  We will be removing
		 * ghost data from either side of the split in each of these
		 * tables.  If a table is _not_ sharded, the "_vnode" column
		 * will not be populated.
		 */
		status.clear();
		status.update('listing Moray buckets');
		ctl.target_moray().listBuckets(done);

	}, function (buckets, done) {
		if (ctl.pausing(done)) {
			return;
		}

		mod_assert.arrayOfObject(buckets, 'buckets');

		vnode_data.tables = buckets.map(function (b) {
			mod_assert.string(b.name, 'bucket name');

			return (b.name);
		});

		status.update('deleting ghost data');

		var tasks = [ {
			t_delete_from_shard: p.shard,
			t_ghost_shard: p.new_shard,
			t_status: status.child(),
		}, {
			t_delete_from_shard: p.new_shard,
			t_ghost_shard: p.shard,
			t_status: status.child(),
		} ];

		lib_common.parallel(ctl, { inputs: tasks,
		    concurrency: tasks.length,
		    retry_delay: 15 * 1000,
		    func: function (t, idx, next) {
			if (ctl.pausing(next)) {
				return;
			}

			mod_assert.ok(t.t_delete_from_shard !==
			    t.t_ghost_shard, 'must be different shards');

			delete_ghost_data(ctl, vnode_data,
			    t.t_delete_from_shard, t.t_ghost_shard, t.t_status,
			    function (err) {
				if (err) {
					t.t_status.trunc();
					var ch = t.t_status.child();
					ch.update('error: %s (retrying)',
					    err.message);
					next(err);
					return;
				}

				next();
			});
		}}, done);

	} ], function (err) {
		if (err) {
			if (hold) {
				ctl.hold(err);
			} else {
				ctl.retry(err);
			}
			return;
		}

		ctl.finish();
	});
}

function
delete_ghost_data(ctl, vnode_data, delete_from_shard, ghost_shard, status,
    callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.object(vnode_data, 'vnode_data');
	mod_assert.arrayOfString(vnode_data.tables, 'vnode_data.tables');
	mod_assert.string(delete_from_shard, 'delete_from_shard');
	mod_assert.string(ghost_shard, 'ghost_shard');
	mod_assert.ok(delete_from_shard !== ghost_shard,
	    'must not be same shard');
	mod_assert.func(callback, 'callback');

	var vnodes = vnode_data[ghost_shard];
	mod_assert.arrayOfNumber(vnodes, 'vnodes');

	var vnodes_delpos = 0;

	status.trunc();
	status.update('shard %s', delete_from_shard);
	ctl.log.info('delete ghost data from shard "%s"', delete_from_shard);
	var stch = status.child();

	var delete_complete = ctl.prop_get('shard_' + delete_from_shard +
	    '_delete_complete');
	if (delete_complete !== null) {
		stch.update('ok');
		ctl.log.info({ delete_complete: delete_complete },
		    'delete already complete!');
		setImmediate(callback);
		return;
	}

	var postgres_ip;
	var pg = new lib_postgres.Postgres(ctl);

	/*
	 * If this process is interrupted, we want to resume deletion from
	 * the same vnode where we left off.
	 */
	var resume_at = ctl.prop_get('shard_' + delete_from_shard +
	    '_delete_marker');
	if (resume_at !== null) {
		vnodes_delpos = vnodes.indexOf(resume_at);
		mod_assert.ok(vnodes_delpos !== -1,
		    'delete resume vnode ' + resume_at + ' not found');
	}

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		stch.update('listing instances of "postgres"');
		stch.prop('shard', delete_from_shard);
		ctl.get_instances({ service: 'postgres',
		    shard: delete_from_shard }, done);

	}, function (insts, done) {
		if (ctl.pausing(done)) {
			return;
		}

		var i = insts[Object.keys(insts)[0]];

		stch.clear();
		stch.update('checking cluster status');
		stch.prop('via zone', i.uuid);
		lib_common.manatee_adm_show(ctl, i.uuid, done);

	}, function (cl, done) {
		if (ctl.pausing(done)) {
			return;
		}

		if (!cl.props.freeze.match(/^frozen since /)) {
			setImmediate(done, new VE('cluster not frozen: "%s"',
			    cl.props.freeze));
			return;
		}

		mod_assert.strictEqual(cl.peers[0].role, 'primary',
		    'peer 0 must be primary');
		postgres_ip = cl.peers[0].ip;
		mod_assert.ok(mod_net.isIPv4(postgres_ip), 'postgres_ip');

		/*
		 * Connect to PostgreSQL on the primary peer for the shard.
		 */
		stch.clear();
		stch.update('connecting to PostgreSQL');
		stch.prop('ip', postgres_ip);
		pg.connect({ host: postgres_ip }, done);

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * We want to delete the data in small batches, as there may be
		 * millions of rows for any particular vnode and we do not
		 * want to inhibit production traffic.
		 * XXX This should be tuneable.
		 */
		var again = function () {
			if (ctl.pausing(done)) {
				return;
			}

			if (vnodes_delpos >= vnodes.length) {
				ctl.log.info('no more vnodes for shard %s!',
				    delete_from_shard);
				setImmediate(done);
				return;
			}

			stch.clear();
			stch.trunc();
			stch.update('deleting vnode %d (#%d of %d)',
			    vnodes[vnodes_delpos], vnodes_delpos + 1,
			    vnodes.length);
			delete_one_vnode(ctl, pg, vnode_data.tables,
			    vnodes[vnodes_delpos], stch, function (err) {
				if (err) {
					done(err);
					return;
				}

				ctl.log.info('shard %s vnode %d complete!',
				    delete_from_shard, vnodes[vnodes_delpos]);

				/*
				 * XXX
				 */
				ctl.prop_put('shard_' + delete_from_shard +
				    '_delete_marker', vnodes[vnodes_delpos]);
				vnodes_delpos++;
				ctl.prop_commit(again);
			});
		};

		setImmediate(again);

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * Confirm that we have deleted all of the data we expected to
		 * delete.  We do this now to ensure that our mechanism for
		 * increased deletion parallelism is not defective in some
		 * unexpected way, and also to ensure that database-level
		 * triggers have not sought to reconstitute deleted rows behind
		 * our back.
		 */
		var vnodes_checkpos = 0;
		var check_again = function () {
			if (ctl.pausing(done)) {
				return;
			}

			if (vnodes_checkpos >= vnodes.length) {
				ctl.log.info('no more vnodes for shard %s!',
				    delete_from_shard);
				setImmediate(done);
				return;
			}

			stch.clear();
			stch.trunc();
			stch.update('confirming vnode %d deletion (#%d of %d)',
			    vnodes[vnodes_checkpos], vnodes_checkpos + 1,
			    vnodes.length);
			check_one_vnode(ctl, pg, vnode_data.tables,
			    vnodes[vnodes_checkpos], stch, function (err) {
				if (err) {
					done(err);
					return;
				}

				ctl.log.info('shard %s vnode %d checked!',
				    delete_from_shard, vnodes[vnodes_checkpos]);

				vnodes_checkpos++;
				setImmediate(check_again);
			});
		};

		setImmediate(check_again);

	} ], function (err) {
		pg.destroy(function () {
			if (err) {
				callback(new VE(err, 'delete ghost data ' +
				    '(shard "%s") from shard "%s"',
				    ghost_shard, delete_from_shard));
				return;
			}

			stch.clear();
			stch.trunc();
			stch.update('ok');
			ctl.prop_put('shard_' + delete_from_shard +
			    '_delete_complete', (new Date()).toISOString());
			ctl.prop_commit(callback);
		});
	});
}

function
delete_one_vnode(ctl, pg, tables, vnode, status, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.object(pg, 'pg');
	mod_assert.arrayOfString(tables, 'tables');
	mod_assert.number(vnode, 'vnode');
	mod_assert.object(status, 'status');
	mod_assert.func(callback, 'callback');

	var track = {};

	tables.forEach(function (t) {
		track[t] = {
			count: 0,
			status: status.child(),
		};

		track[t].status.update('%-28s: pending', t);
	});

	var one_table = function (table, subcb) {
		if (ctl.pausing(subcb)) {
			return;
		}

		mod_assert.string(table, 'table');
		mod_assert.func(subcb, 'subcb');

		var tr = track[table];

		var delete_batch_size = ctl.tuning_get('delete_batch_size', {
		    type: 'number', min: 1, max: 100000, def: 10 });
		if (delete_batch_size instanceof Error) {
			subcb(delete_batch_size);
			return;
		}

		var delete_pause_ms = ctl.tuning_get('delete_pause_ms', {
		    type: 'number', min: 0, max: 60 * 1000, def: 1000 });
		if (delete_pause_ms instanceof Error) {
			subcb(delete_pause_ms);
			return;
		}

		status.prop('delete batch size', '' + delete_batch_size);
		status.prop('delete pause (ms)', '' + delete_pause_ms);

		var q = [
			'DELETE FROM', table, 'WHERE _id IN (',
			'SELECT _id FROM', table, 'WHERE _vnode = $1',
			'LIMIT $2) AND _vnode = $1',
		].join(' ');
		var p = [ vnode, delete_batch_size ];

		tr.status.update('%-28s: deleting (%d rows)', table,
		    tr.count);
		ctl.log.info('checking table %s vnode %d', table, vnode);
		pg.query({ text: q, values: p }, function (err, res) {
			if (err) {
				subcb(new VE(err, 'table %s vnode %d',
				    table, vnode));
				return;
			}

			if (res.rowCount === 0) {
				tr.status.update('%-28s: done (%d rows)',
				    table, tr.count);
				ctl.log.info('no match (table %s, vnode %d)',
				    table, vnode);
				subcb();
				return;
			}

			tr.count += res.rowCount;
			ctl.log.info('deleted %d rows for ' +
			    'table %s vnode %d', res.rowCount, table, vnode);

			if (ctl.pausing(subcb)) {
				return;
			}

			setTimeout(function () {
				one_table(table, subcb);
			}, delete_pause_ms);
		});
	};

	mod_vasync.forEachPipeline({ inputs: tables, func: one_table },
	    function (err) {
		if (err) {
			callback(err);
			return;
		}

		callback();
	});
}

function
check_one_vnode(ctl, pg, tables, vnode, status, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.object(pg, 'pg');
	mod_assert.arrayOfString(tables, 'tables');
	mod_assert.number(vnode, 'vnode');
	mod_assert.object(status, 'status');
	mod_assert.func(callback, 'callback');

	var track = {};

	tables.forEach(function (t) {
		track[t] = {
			status: status.child(),
		};

		track[t].status.update('%-28s: pending', t);
	});

	mod_vasync.forEachPipeline({ inputs: tables,
	    func: function check_one_table(table, next) {
		if (ctl.pausing(next)) {
			return;
		}

		mod_assert.string(table, 'table');
		mod_assert.func(next, 'next');

		var tr = track[table];

		var q = [
			'SELECT COUNT(x._id) AS found_rows FROM (SELECT _id',
			'FROM', table, 'WHERE _vnode = $1 LIMIT 1) x',
		].join(' ');
		var p = [ vnode ];

		tr.status.update('%-28s: checking', table);
		ctl.log.info('confirming delete on table %s vnode %d', table,
		    vnode);
		pg.query({ text: q, values: p }, function (err, res) {
			if (err) {
				next(new VE(err, 'table %s vnode %d',
				    table, vnode));
				return;
			}

			mod_assert.strictEqual(res.rowCount, 1,
			    'expected one result row from query');
			mod_assert.number(res.rows[0].found_rows, 'found_rows');

			if (res.rows[0].found_rows === 0) {
				tr.status.update('%-28s: done', table);
				ctl.log.info('no rows (table %s, vnode %d)',
				    table, vnode);
				next();
				return;
			}

			err = new VE({ info: { table: table, vnode: vnode,
			    hold: true }}, 'found rows left after deletion');
			ctl.log.warn(err, 'rows left after deletion ' +
			    '(table %s; vnode %s)', table, vnode);
			next(err);
		});

	}}, function (err) {
		if (err) {
			callback(err);
			return;
		}

		callback();
	});
}


module.exports = {
	phase_delete_data: phase_delete_data,
};
