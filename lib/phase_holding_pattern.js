
function
phase_holding_pattern(ctl)
{
	var i = 0;
	var again = function () {
		ctl.log.info({ count: i++ }, 'holding pattern!');
		setTimeout(again, 3000);
	};
	setTimeout(again, 1000);
}

module.exports = {
	phase_holding_pattern: phase_holding_pattern,
};
