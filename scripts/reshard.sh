#!/bin/bash

case "$1" in
plan)
	case "$2" in
	unhold|archive|pause|resume)
		exec "plan_$2" "$3"
		;;
	esac
	;;
esac

printf 'ERROR: usage: reshard plan [unhold|archive|pause|resume] PLAN_UUID\n' \
    >&2
exit 1
