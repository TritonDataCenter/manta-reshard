#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2018, Joyent, Inc.
#

NODE='/opt/smartdc/electric-moray/build/node/bin/node'
EM_BIN='/opt/smartdc/electric-moray/node_modules/.bin'
FASH="$EM_BIN/fash"
SDC_IMGADM="$EM_BIN/sdc-imgadm"
export SDC_IMGADM_URL='%%HASH_RING_IMGAPI_SERVICE%%'
HASH_RING_IMAGE='%%HASH_RING_IMAGE%%'

export PATH='/usr/bin:/usr/sbin:/sbin'

function cleanup {
	if [[ -n $tmpdir ]]; then
		if ! cd /; then
			printf 'ERROR: failed to chdir to / for cleanup\n' >&2
			exit 1
		fi
		if ! rm -rf "$tmpdir"; then
			printf 'ERROR: failed to remove tmpdir for cleanup\n' \
			    >&2
			exit 1
		fi
	fi
}

function fatal {
	printf 'ERROR: %s\n' "$1" >&2
	cleanup
	exit 1
}

#
# Create a temporary directory into which to download the hash ring from
# IMGAPI.
#
if ! tmpdir=$(mktemp -d) || ! cd "$tmpdir"; then
	fatal 'could not create temporary directory'
fi

#
# Obtain the current hash ring from IMGAPI.
#
if ! "$NODE" "$SDC_IMGADM" get-file -q -o 'hash_ring.tar.gz' \
    "$HASH_RING_IMAGE" >/dev/null 2>&1; then
	fatal 'could not download hash ring image from IMGAPI'
fi

#
# Unpack the image.  The tar file contains the directory "hash_ring".
#
if ! tar xfz 'hash_ring.tar.gz'; then
	fatal 'could not extract tar file'
fi

FASHARGS=(
	'-b' 'leveldb'
	'-l' "$tmpdir/hash_ring"
)

#
# Extract the list of pnodes from the hash ring database.
#
if ! pnodes_res=$("$NODE" "$FASH" get-pnodes "${FASHARGS[@]}"); then
	fatal 'could not list pnodes'
fi
if ! pnodes_list=( $(/usr/bin/awk "{ sub(\"^[^']*'\", \"\", \$0);
    sub(\"[ ,'\\\\]]*\$\", \"\", \$0); printf(\"%s\n\", \$0); }" \
    <<< "$pnodes_res") ); then
	fatal 'could not parse pnodes list'
fi

#
# Extract the list of vnodes from the old and new shards.  Build the
# JSON-formatted POST body as we go.
#
post_body='{'
for (( i = 0; i < ${#pnodes_list[@]}; i++ )); do
	pnode=${pnodes_list[$i]}

	#
	# Remove the protocol and port number from the URL string in order
	# to get the bare shard name.
	#
	shard_name=${pnode#tcp://}
	shard_name=${shard_name%:2020}

	if ! shard_vnodes=$("$NODE" "$FASH" get-vnodes "${FASHARGS[@]}" \
	    "$pnode") || [[ -z $shard_vnodes ]]; then
		fatal "could not list vnodes for \"$shard_name\""
	fi

	post_body+="\"$shard_name\":$shard_vnodes"
	if (( i < ${#pnodes_list[@]} - 1 )); then
		post_body+=','
	fi
done
post_body+='}'

#
# POST the data back to the reshard server.
#
ok=false
for (( retrycount = 0; retrycount < 5; retrycount++ )); do
	if curl --max-time 45 -sSf -X POST \
	    -H 'Content-Type: application/json' -d '@-' \
	    '%%POST_URL%%' <<< "$post_body"; then
		ok=true
		break
	fi
done

if [[ $ok != true ]]; then
	fatal 'could not POST vnode set back to reshard server'
fi

cleanup
exit 0
