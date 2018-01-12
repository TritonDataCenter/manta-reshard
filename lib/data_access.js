

var mod_assert = require('assert-plus');
var mod_moray = require('moray');
var mod_vasync = require('vasync');
var mod_verror = require('verror');
var mod_jsprim = require('jsprim');
var mod_uuid = require('uuid');

var VE = mod_verror.VError;


var BUCKET_PLANS = 'manta_reshard_plans';
var BUCKET_LOCKS = 'manta_reshard_locks';

var BUCKETS = {};

BUCKETS[BUCKET_PLANS] = {
	index: {
		shard: {
			type: 'string',
			unique: false
		},
		active: {
			type: 'boolean',
		}
	},
	options: {
		version: 1
	}
};

BUCKETS[BUCKET_LOCKS] = {
	index: {
	},
	options: {
		version: 1
	}
};


function
create_buckets(ctx, c, callback)
{
	mod_assert.func(callback, 'callback');

	var log = ctx.ctx_log;

	mod_vasync.forEachPipeline({ inputs: Object.keys(BUCKETS),
	    func: function (name, next) {
		var bcfg = BUCKETS[name];

		log.debug({ bucket_config: bcfg }, 'creating bucket "%s"',
		    name);

		c.putBucket(name, bcfg, function (err) {
			if (err) {
				next(new VE(err, 'bucket "%s"', name));
				return;
			}

			next();
		});

	}}, function (err) {
		if (err) {
			callback(new VE(err, 'creating buckets'));
			return;
		}

		log.debug('moray setup complete');

		ctx.ctx_moray = c;

		callback();
	});
}

function
create_moray_client(ctx, callback)
{
	mod_assert.object(ctx, 'opts');
	mod_assert.object(ctx.ctx_cfg.moray.reshard.options, 'moray config');
	mod_assert.object(ctx.ctx_log, 'ctx_log');
	mod_assert.func(callback, 'callback');

	var log = ctx.ctx_log;

	var cfg = mod_jsprim.deepCopy(ctx.ctx_cfg.moray.reshard.options);
	log.debug({ moray_config: cfg }, 'connecting to moray');

	cfg.log = log.child({ component: 'moray' });

	var c = mod_moray.createClient(cfg);

	c.once('connect', function () {
		log.debug('connected to moray');
		create_buckets(ctx, c, callback);
	});
}

function
object_to_plan(obj)
{
	mod_assert.string(obj._etag, '_etag');

	var plan = obj.value;
	plan._etag = obj._etag;
	if (!plan.hasOwnProperty('phase')) {
		plan.phase = null;
	}
	if (!plan.hasOwnProperty('hold')) {
		plan.hold = null;
	} else if (plan.hold !== null) {
		if (!plan.hold.date) {
			plan.hold.date = new Date().toISOString();
		}
	}
	if (!plan.hasOwnProperty('postgres_image')) {
		plan.postgres_image = null;
	}
	if (!plan.hasOwnProperty('completed')) {
		plan.completed = false;
	}
	if (!plan.hasOwnProperty('paused')) {
		plan.paused = false;
	}
	if (!plan.hasOwnProperty('props')) {
		plan.props = {};
	}
	if (!plan.hasOwnProperty('pause_at_phase')) {
		plan.pause_at_phase = null;
	}
	if (!plan.hasOwnProperty('tuning')) {
		plan.tuning = {};
	}

	/*
	 * For now, a strict version check will allow us the option of making a
	 * backwards compatible change in the future.  By incrementing the "v"
	 * value, we can prevent older software versions from mistakenly
	 * misinterpreting a new schema.
	 */
	if (!plan.hasOwnProperty('v')) {
		plan.v = 1;
	} else if (plan.v !== 1) {
		return (new VE('plan "%s" has invalid version: %j', plan.uuid,
		    plan.v));
	}

	return (plan);
}

function
plans_active(ctx, callback)
{
	mod_assert.object(ctx, 'ctx');
	mod_assert.func(callback, 'callback');

	/*
	 * XXX Use filter construction library.
	 */
	var plans = [];
	var problems = [];
	var fo = ctx.ctx_moray.findObjects(BUCKET_PLANS, '(active=true)');

	fo.once('error', function (err) {
		callback(new VE('getting active plans'));
	});

	fo.on('record', function (obj) {
		var plan = object_to_plan(obj);

		if (plan instanceof Error) {
			problems.push(plan);
		} else {
			plans.push(object_to_plan(obj));
		}
	});

	fo.on('end', function () {
		callback(VE.errorFromList(problems), plans);
	});
}

function
plans_for_shard(ctx, shard, callback)
{
	mod_assert.object(ctx, 'ctx');
	mod_assert.string(shard, 'shard');
	mod_assert.func(callback, 'callback');

	plans_active(ctx, function (err, plans) {
		if (err) {
			callback(new VE(err, 'getting plans for shard "%s"',
			    shard));
			return;
		}

		/*
		 * XXX It would probably be better to do this with filters
		 * and indexed columns in Moray.
		 */
		var matching_plans = plans.filter(function (p) {
			mod_assert.string(p.shard, 'p.shard');
			mod_assert.string(p.new_shard, 'p.new_shard');

			return (p.shard === shard || p.new_shard === shard);
		});

		callback(null, matching_plans);
	});
}

function
plan_load(ctx, uuid, callback)
{
	mod_assert.object(ctx, 'ctx');
	mod_assert.uuid(uuid, 'uuid');
	mod_assert.func(callback, 'callback');

	ctx.ctx_moray.getObject(BUCKET_PLANS, uuid, function (err, obj) {
		if (err) {
			callback(new VE(err, 'load plan "%s"', uuid));
			return;
		}

		if (!obj) {
			var i = { info: { not_found: true }};
			callback(new VE(i, 'plan "%s" not found', uuid));
			return;
		}

		mod_assert.equal(uuid, obj.value.uuid, 'uuid match');

		var plan = object_to_plan(obj);

		if (plan instanceof Error) {
			ctx.ctx_log.error(plan, 'error loading plan "%s"',
			    plan.uuid);
			callback(plan);
			return;
		}

		ctx.ctx_log.debug({ plan: plan }, 'load plan "%s"', plan.uuid);

		callback(null, plan);
	});
}

function
plan_store(ctx, plan, callback)
{
	mod_assert.object(ctx, 'ctx');
	mod_assert.object(plan, 'plan');
	mod_assert.func(callback, 'callback');

	/*
	 * XXX Use a schema validator?
	 */
	mod_assert.number(plan.v, 'plan.v');
	mod_assert.strictEqual(plan.v, 1, 'plan version must be 1');
	mod_assert.uuid(plan.uuid, 'plan.uuid');
	mod_assert.bool(plan.active, 'plan.active');
	mod_assert.bool(plan.completed, 'plan.completed');
	mod_assert.bool(plan.paused, 'plan.paused');
	mod_assert.optionalUuid(plan.postgres_image, 'plan.postgres_image');
	mod_assert.string(plan.shard, 'plan.shard');
	mod_assert.string(plan.new_shard, 'plan.new_shard');
	mod_assert.number(plan.split_count, 'plan.split_count');
	mod_assert.object(plan.props, 'plan.props');
	mod_assert.object(plan.tuning, 'plan.tuning');
	mod_assert.date(plan.start_time, 'plan.start_time');

	var val = mod_jsprim.deepCopy(plan);
	delete val._etag;

	var opts = {
		_etag: plan._etag ? plan._etag : null
	};

	ctx.ctx_log.debug({ plan: plan }, 'store plan "%s"', plan.uuid);

	ctx.ctx_moray.putObject(BUCKET_PLANS, plan.uuid, val, opts,
	    function (err, res) {
		if (err) {
			callback(new VE(err, 'store plan "%s"', plan.uuid));
			return;
		}

		mod_assert.string(res.etag, 'res.etag');
		plan._etag = res.etag;

		callback(null, plan);
	});
}

function
object_to_lock(obj)
{
	mod_assert.string(obj._etag, '_etag');

	var lock = obj.value;
	lock._etag = obj._etag;
	if (!lock.hasOwnProperty('owner')) {
		lock.owner = null;
	}
	if (!lock.hasOwnProperty('events')) {
		lock.events = [];
	}

	return (lock);
}


function
lock_load(ctx, name, callback)
{
	mod_assert.object(ctx, 'ctx');
	mod_assert.string(name, 'name');
	mod_assert.func(callback, 'callback');

	ctx.ctx_moray.getObject(BUCKET_LOCKS, name, function (err, obj) {
		if (err) {
			callback(new VE(err, 'load lock "%s"', name));
			return;
		}

		if (!obj) {
			var i = { info: { not_found: true }};
			callback(new VE(i, 'lock "%s" not found', name));
			return;
		}

		mod_assert.equal(name, obj.value.name, 'uuid match');

		var lock = object_to_lock(obj);

		ctx.ctx_log.debug({ lock: lock }, 'load lock "%s"', lock.name);

		callback(null, lock);
	});
}

/*
 * Each object in the "manta_reshard_locks" bucket represents a lock that can
 * be taken by a plan.  These locks are re-entrant, so if a particular plan
 * already owns the lock a subsequent acquisition will succeed.
 */
function
lock_store(ctx, lock, callback)
{
	mod_assert.object(ctx, 'ctx');
	mod_assert.object(lock, 'lock');
	mod_assert.func(callback, 'callback');

	/*
	 * XXX Use a schema validator?
	 */
	mod_assert.string(lock.name, 'lock.name');
	mod_assert.optionalString(lock.owner, 'lock.owner');
	if (!lock.owner) {
		mod_assert.ok(lock.owner === null, 'owner is string or null');
	}

	var val = mod_jsprim.deepCopy(lock);
	delete val._etag;

	var opts = {
		_etag: lock._etag ? lock._etag : null
	};

	ctx.ctx_log.debug({ lock: lock }, 'store lock "%s"', lock.name);

	ctx.ctx_moray.putObject(BUCKET_LOCKS, lock.name, val, opts,
	    function (err, res) {
		if (err) {
			callback(new VE(err, 'store lock "%s"', lock.name));
			return;
		}

		mod_assert.string(res.etag, 'res.etag');
		lock._etag = res.etag;

		callback(null, lock);
	});
}



module.exports = {
	create_moray_client: create_moray_client,
	plan_load: plan_load,
	plan_store: plan_store,
	plans_for_shard: plans_for_shard,
	plans_active: plans_active,
	lock_load: lock_load,
	lock_store: lock_store,
};
