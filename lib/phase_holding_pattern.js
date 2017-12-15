
function
phase_holding_pattern(ctl)
{
	ctl.log.info('entering holding pattern!');

	setInterval(function () {}, 1000);
}

module.exports = {
	phase_holding_pattern: phase_holding_pattern,
};
