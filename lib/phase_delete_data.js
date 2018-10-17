/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_net = require('net');

var mod_assert = require('assert-plus');
var mod_verror = require('verror');
var mod_vasync = require('vasync');
var mod_jsprim = require('jsprim');
var mod_extsprintf = require('extsprintf');

var lib_common = require('../lib/common');
var lib_template = require('../lib/template');
var lib_postgres = require('../lib/postgres');

var VE = mod_verror.VError;
var sprintf = mod_extsprintf.sprintf;


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

		ctl.get_instances({ service: 'electric-moray' }, status,
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

		/*
		 * Because of a custom database-level update trigger installed
		 * by libmanta, the "manta_directory_counts" table is populated
		 * by changes to the "manta" table.  The trigger in question
		 * has no effect if we delete a row from the "manta" table
		 * and there is no corresponding row in the count table, so
		 * arrange to purge ghost data from the count table first.
		 */
		vnode_data.tables.sort();
		var mdci = vnode_data.tables.indexOf('manta_directory_counts');
		if (mdci !== -1) {
			var x = vnode_data.tables.splice(mdci, 1);
			vnode_data.tables.unshift(x[0]);
		}
		mod_assert.arrayOfString(vnode_data.tables,
		    'vnode_data.tables');

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
get_pg_conns(ctl)
{
	return (ctl.tuning_get('delete_pg_conn_count',
	    { type: 'number', min: 1, max: 256, def: 10 }));
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

	var tables = vnode_data.tables;

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

	var delete_pg_conns = get_pg_conns(ctl);
	if (delete_pg_conns instanceof Error) {
		setImmediate(callback, delete_pg_conns);
		return;
	}

	var postgres_ip;
	var insts;
	var pg_set = [];
	for (var i = 0; i < delete_pg_conns; i++) {
		pg_set.push(new lib_postgres.Postgres(ctl));
	}

	mod_vasync.waterfall([ function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		ctl.get_instances({ service: 'postgres',
		    shard: delete_from_shard }, stch, done);

	}, function (_insts, done) {
		if (ctl.pausing(done)) {
			return;
		}

		insts = _insts;

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
		stch.prop('zone', cl.peers[0].uuid);
		stch.prop('ip', postgres_ip);
		var count = 0;
		mod_vasync.forEachPipeline({ inputs: pg_set,
		    func: function (pg, next) {
			count++;
			stch.update('connecting to PostgreSQL (%d/%d)',
			    count, pg_set.length);
			pg.connect({ host: postgres_ip }, function (err) {
				if (err) {
					next(new VE(err,
					    'PostgreSQL %s (%d/%d)',
					    postgres_ip, count, pg_set.length));
					return;
				}

				var pgu = cl.peers[0].uuid;
				pg.info_set(pgu + ' (dc "' +
				    insts[pgu].metadata.DATACENTER + '")');

				next();
			});

		}}, function (err) {
			done(err);
		});

	}, function (done) {
		if (ctl.pausing(done)) {
			return;
		}

		/*
		 * We want to delete the data in small batches, as there may be
		 * millions of rows for any particular vnode and we do not
		 * want to inhibit production traffic.
		 */
		var again = function () {
			if (ctl.pausing(done)) {
				return;
			}

			/*
			 * Select the next table to work on.
			 */
			var table = null;
			var markprop;
			for (var i = 0; i < tables.length; i++) {
				var t = tables[i];

				/*
				 * Check to see if we have completed deletion
				 * for this table in this shard already.
				 */
				markprop = sprintf(
				    'shard_%s_table_%s_delete_complete',
				    delete_from_shard, t);

				var complete = ctl.prop_get(markprop);
				if (complete === null) {
					table = t;
					break;
				}

				ctl.log.debug('shard "%s" table "%s" delete ' +
				    'already completed @ %s', delete_from_shard,
				    t, complete);
			}

			if (table === null) {
				ctl.log.info('no more tables for shard %s!',
				    delete_from_shard);
				setImmediate(done);
				return;
			}

			ctl.log.info('deleting from table "%s" in shard "%s"',
			    table, delete_from_shard);

			stch.clear();
			stch.trunc();
			stch.update('deleting from table "%s" (%d/%d)',
			    table, tables.indexOf(table) + 1, tables.length);
			delete_one_table(ctl, pg_set, table, vnodes, stch,
			    function (err) {
				if (err) {
					done(err);
					return;
				}

				ctl.log.info('shard "%s" table "%s" complete!',
				    delete_from_shard, table);

				/*
				 * Mark this table as cleared of all ghost
				 * vnode data.
				 */
				ctl.prop_put(markprop, (new Date()).
				    toISOString());
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
			stch.prop('via zone', pg_set[0].info());
			check_one_vnode(ctl, pg_set[0], tables,
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
		mod_vasync.forEachPipeline({ inputs: pg_set,
		    func: function (pg, next) {
			pg.destroy(next);

		}}, function () {
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

/*
 * Each object in a Moray bucket has an integer "_id" property which we can use
 * when assembling a limited set of rows to remove from the table.  In some
 * deployments, the original "_id" column was limited to 32-bit values.  A
 * second 64-bit column ("_idx") was added; the "_id" property value may appear
 * in either one of these columns for any particular row.
 *
 * Determine whether this particular table has the "_idx" extended identifier
 * column.
 */
function
check_extended_id(ctl, pg_set, table, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.arrayOfObject(pg_set, 'pg_set');
	mod_assert.string(table, 'table');
	mod_assert.func(callback, 'callback');

	var column_exists = false;
	var index_ready = false;
	var pg = pg_set[0];

	mod_vasync.waterfall([ function (next) {
		/*
		 * Check to see if the table for this bucket has the "_idx"
		 * column.  If this column is present, the "_id" property of
		 * objects in this bucket is virtual: the actual ID value might
		 * be in either the "_id" column or the "_idx" column.
		 */
		var q = [
			'SELECT',
			'    TRUE as exists',
			'FROM',
			'    pg_catalog.pg_attribute pga',
			'WHERE',
			'    pga.attrelid = \'' + table + '\'::regclass AND',
			'    pga.attname = \'_idx\' AND',
			'    NOT pga.attisdropped'
		].join(' ');
		pg.query({ text: q }, function (err, res) {
			if (err) {
				next(new VE(err, 'checking for "_idx" column'));
				return;
			}

			mod_assert.number(res.rowCount, 'rowCount');
			if (res.rowCount !== 1) {
				next();
				return;
			}

			mod_assert.strictEqual(res.rows[0].exists, true,
			    'exists');
			column_exists = true;
			next();
		});

	}, function (next) {
		if (!column_exists) {
			setImmediate(next);
			return;
		}

		/*
		 * Check to see if the index for the "_idx" column exists and
		 * is able to be used by queries.
		 */
		var q = [
			'SELECT',
			'    pgc.relname AS table_name,',
			'    pgc.oid AS table_oid,',
			'    pgci.relname AS index_name,',
			'    pgi.indexrelid AS index_oid,',
			'    pgi.indisvalid AS index_valid',
			'FROM',
			'    pg_catalog.pg_class pgc INNER JOIN',
			'    pg_catalog.pg_index pgi ON',
			'        pgc.oid = pgi.indrelid INNER JOIN',
			'    pg_catalog.pg_class pgci ON',
			'        pgi.indexrelid = pgci.oid',
			'WHERE',
			'    pgc.relname = \'' + table + '\' AND',
			'    pgci.relname = \'' + table + '__idx_idx\''
		].join(' ');
		pg.query({ text: q }, function (err, res) {
			if (err) {
				next(new VE(err, 'checking for "_idx" index'));
				return;
			}

			mod_assert.number(res.rowCount, 'rowCount');
			if (res.rowCount !== 1) {
				next();
				return;
			}

			mod_assert.strictEqual(res.rows[0].index_name,
			    table + '__idx_idx', 'expected index');
			mod_assert.bool(res.rows[0].index_valid, 'index_valid');
			index_ready = res.rows[0].index_valid;
			next();
		});

	} ], function (err) {
		if (err) {
			callback(new VE(err, 'checking for extended ' +
			    'identifier column (table "%s")', table));
			return;
		}

		/*
		 * If the "_idx" column exists and is correctly indexed, we
		 * will use it in queries.
		 */
		callback(null, !!(column_exists && index_ready));
	});
}

function
delete_one_table(ctl, pg_set, table, vnodes, status, callback)
{
	mod_assert.object(ctl, 'ctl');
	mod_assert.arrayOfObject(pg_set, 'pg_set');
	mod_assert.string(table, 'table');
	mod_assert.arrayOfNumber(vnodes, 'vnodes');
	mod_assert.object(status, 'status');
	mod_assert.func(callback, 'callback');

	status.prop('via zone', pg_set[0].info());
	status.prop('delete pg conn count', '' + pg_set.length);

	var total_rows = 0;
	var datapoints = [];
	var extended_id;

	/*
	 * Keep a running total of rows deleted for this table, and attempt
	 * to estimate the rate of rows deleted per second.
	 */
	var record_deletes = function (count) {
		mod_assert.number(count, 'count');

		total_rows += count;
		datapoints.push({
			dp_total: total_rows,
			dp_when: process.hrtime()
		});
		while (datapoints.length > pg_set.length * 10) {
			datapoints.shift();
		}

		var str = '' + total_rows;
		if (datapoints.length > 4) {
			var last = datapoints[datapoints.length - 1];
			var first = datapoints[0];

			var hrd = mod_jsprim.hrtimeDiff(last.dp_when,
			    first.dp_when);
			var secs = mod_jsprim.hrtimeMillisec(hrd) / 1000;
			var delta = last.dp_total - first.dp_total;

			var rows_ps = Math.round(delta / secs);

			str += ' (' + rows_ps + '/sec)';
		}

		status.prop('overall progress', str);
	};

	/*
	 * Establish a queue of vnodes from which to delete ghost data.
	 * Each queue entry will track the number of rows deleted so far.
	 */
	var vnode_queue = vnodes.map(function (vnode, idx) {
		return ({
			vq_vnode: vnode,
			vq_count: 0,
			vq_ordinal: idx + 1,
			vq_column: '_id',
		});
	});

	/*
	 * For each PostgreSQL connection, establish a line in the status
	 * output and keep track of the current vnode queue entry.
	 */
	var dots = pg_set.map(function (pg) {
		return ({
			dot_pg: pg,
			dot_status: status.child(),
			dot_vq: null
		});
	});

	var worker = function (dot, worker_cb) {
		if (ctl.pausing(worker_cb)) {
			return;
		}

		mod_assert.object(dot.dot_pg, 'dot_pg');
		mod_assert.object(dot.dot_status, 'dot_status');

		var vq;
		if ((vq = dot.dot_vq) === null) {
			if (vnode_queue.length < 1) {
				/*
				 * There is no more work to do.
				 */
				dot.dot_status.done();
				setImmediate(worker_cb);
				return;
			}

			/*
			 * Pull a vnode to work on from the vnode queue.
			 */
			vq = dot.dot_vq = vnode_queue.shift();
		}

		mod_assert.number(vq.vq_vnode, 'vq_vnode');
		mod_assert.number(vq.vq_count, 'vq_count');

		/*
		 * Make sure any error we pass to the callback is wrapped
		 * with some useful context.
		 */
		var done = function (err) {
			if (err) {
				err = new VE(err, 'deleting vnode %d from ' +
				    'table "%s"', vq.vq_node, table);
			}

			setImmediate(worker_cb, err);
		};

		var delete_pg_conns = get_pg_conns(ctl);
		if (delete_pg_conns instanceof Error) {
			done(delete_pg_conns);
			return;
		}

		/*
		 * If the desired concurrency has been adjusted by the
		 * operator, we need to tear everything down and start again
		 * with the correct number of PostgreSQL connections.
		 */
		if (delete_pg_conns !== pg_set.length) {
			done(new VE({ info: {
			    concurrency_change: true }},
			    'concurrency change (%d -> %d)',
			    pg_set.length, delete_pg_conns));
			return;
		}

		var delete_batch_size = ctl.tuning_get('delete_batch_size', {
		    type: 'number', min: 1, max: 100000, def: 10 });
		if (delete_batch_size instanceof Error) {
			done(delete_batch_size);
			return;
		}

		var delete_pause_ms = ctl.tuning_get('delete_pause_ms', {
		    type: 'number', min: 0, max: 60 * 1000, def: 1000 });
		if (delete_pause_ms instanceof Error) {
			done(delete_pause_ms);
			return;
		}

		status.prop('delete batch size', '' + delete_batch_size);
		status.prop('delete pause (ms)', '' + delete_pause_ms);
		status.prop('moray extended id?', extended_id ? 'yes' : 'no');

		/*
		 * Delete a limited quantity of rows in this batch.  As
		 * PostgreSQL does not support a LIMIT clause on a DELETE
		 * statement, we first assemble the list of rows in a subquery.
		 */
		mod_assert.ok(vq.vq_column === '_id' || vq.vq_column === '_idx',
		    'vq_column not _id[x]');
		var sq = [
			'SELECT', vq.vq_column, 'FROM', table,
			'WHERE _vnode = $1 AND', vq.vq_column, 'IS NOT NULL',
			'LIMIT $2'
		].join(' ');
		var q = [
			'DELETE FROM', table, 'WHERE',
			vq.vq_column, 'IS NOT NULL AND',
			vq.vq_column, 'IN (', sq, ') AND',
			'_vnode = $1'
		].join(' ');

		var p = [ vq.vq_vnode, delete_batch_size ];

		var update_status = function () {
			dot.dot_status.update(
			    'vnode %10d: deleted %d rows (#%d) column "%s"',
			    vq.vq_vnode, vq.vq_count, vq.vq_ordinal,
			    vq.vq_column);
		};
		update_status();

		ctl.log.info({ extended_id: extended_id },
		    'checking table %s vnode %d', table,
		    vq.vq_vnode);
		var start = process.hrtime();
		dot.dot_pg.query({ text: q, values: p }, function (err, res) {
			if (err) {
				done(new VE(err, 'pg query'));
				return;
			}

			var latency_ms = mod_jsprim.hrtimeMillisec(
			    process.hrtime(start));
			var log_props = {
				del: true,
				latency_ms: latency_ms,
				delete_batch_size: delete_batch_size,
				row_count: res.rowCount,
				table: table,
				vnode: vq.vq_vnode,
				column: vq.vq_column
			};

			mod_assert.ok(vq.vq_column === '_id' ||
			    vq.vq_column === '_idx', 'vq_column not _id[x]');
			if (res.rowCount === 0 && vq.vq_column === '_id' &&
			    extended_id) {
				/*
				 * If we have exhausted rows with an "_id"
				 * value, but extended identifiers are in use,
				 * then we need to continue deleting using the
				 * "_idx" column.
				 */
				ctl.log.info(log_props,
				    '"_id" rows exhausted, moving to ' +
				    '"_idx" rows (table %s, vnode %d)',
				    table, vq.vq_vnode);
				vq.vq_column = '_idx';
				setImmediate(worker, dot, worker_cb);
				return;
			}

			if (res.rowCount === 0) {
				/*
				 * Otherwise, there are no more rows to delete
				 * for this vnode.
				 */
				dot.dot_vq = null;
				ctl.log.info(log_props,
				    'no match (table %s, vnode %d, column %s)',
				    table, vq.vq_vnode, vq.vq_column);
				vq.vq_column = null;
				setImmediate(worker, dot, worker_cb);
				return;
			}

			mod_assert.number(vq.vq_count, 'vq_count');
			vq.vq_count += res.rowCount;
			ctl.log.info(log_props,
			    'deleted %d rows for table %s vnode %d column %s',
			    res.rowCount, table, vq.vq_vnode, vq.vq_column);

			update_status();
			record_deletes(res.rowCount);

			if (ctl.pausing(done)) {
				return;
			}

			setTimeout(worker, delete_pause_ms, dot, worker_cb);
		});

	};

	check_extended_id(ctl, pg_set, table, function (err, _extended_id) {
		if (err) {
			callback(err);
			return;
		}

		extended_id = _extended_id;

		mod_vasync.forEachParallel({ inputs: dots, func: worker },
		    function (err) {
			if (err) {
				callback(err);
				return;
			}

			callback();
		});
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
			'SELECT COUNT(x._key) AS found_rows FROM (SELECT _key',
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
			mod_assert.string(res.rows[0].found_rows, 'found_rows');

			if (res.rows[0].found_rows === '0') {
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
