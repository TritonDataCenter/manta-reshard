#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2018, Joyent, Inc.
#

#
# This script will download a pristine copy of the current hash ring database
# from IMGAPI and unpack it in a workspace directory specific to the current
# reshard plan.  Once unpacked, other scripts can be run to perform specific
# modifications to the database.
#

NODE='/opt/smartdc/electric-moray/build/node/bin/node'
EM_BIN='/opt/smartdc/electric-moray/node_modules/.bin'
SDC_IMGADM="$EM_BIN/sdc-imgadm"
export SDC_IMGADM_URL='%%HASH_RING_IMGAPI_SERVICE%%'
HASH_RING_IMAGE='%%HASH_RING_IMAGE%%'

export PATH='/usr/bin:/usr/sbin:/sbin'

workspace='/var/tmp/reshard.%%WORKSPACE_ID%%'

if ! rm -rf "$workspace"; then
	printf 'ERROR: could not remove workspace\n' >&2
	exit 1
fi

if ! mkdir "$workspace" || ! cd "$workspace"; then
	printf 'ERROR: could not mkdir or chdir into workspace\n' >&2
	exit 1
fi

#
# Obtain the current hash ring from IMGAPI.
#
if ! "$NODE" "$SDC_IMGADM" get-file -q -o 'hash_ring.tar.gz' \
    "$HASH_RING_IMAGE" >/dev/null 2>&1; then
	printf 'ERROR: could not download hash ring image "%s" from IMGAPI\n' \
	    "$HASH_RING_IMAGE" >&2
	exit 1
fi

#
# Unpack the image.  The tar file contains the directory "hash_ring".
#
if ! tar xfz 'hash_ring.tar.gz'; then
	printf 'ERROR: could not extract tar file\n' >&2
	exit 1
fi

exit 0
