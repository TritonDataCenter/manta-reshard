#!/bin/bash

export PATH='/usr/bin:/usr/sbin:/sbin'

workspace='/var/tmp/reshard.%%WORKSPACE_ID%%'

if ! rm -rf "$workspace"; then
	printf 'ERROR: could not remove workspace\n' >&2
	exit 1
fi

exit 0
