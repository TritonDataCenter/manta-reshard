
function
phase_holding_pattern(ctl)
{
	ctl.log.info('entering holding pattern!');

	var interv = setInterval(function () {
		if (ctl.pausing(ctl.retry)) {
			clearInterval(interv);
			return;
		}
	}, 1000);
}

module.exports = {
	phase_holding_pattern: phase_holding_pattern,
};
