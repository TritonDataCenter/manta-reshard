#!/bin/bash

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
	fatal 'ERROR: could not extract tar file'
fi

FASHARGS=(
	'-b' 'leveldb'
	'-l' "$tmpdir/hash_ring"
)

#
# Extract the list of vnodes from the old and new shards.
#
if ! shard_vnodes=$("$NODE" "$FASH" get-vnodes "${FASHARGS[@]}" \
    'tcp://%%SHARD%%:2020') || [[ -z $shard_vnodes ]]; then
	fatal 'could not list vnodes for %%SHARD%%'
fi
if ! new_shard_vnodes=$("$NODE" "$FASH" get-vnodes "${FASHARGS[@]}" \
    'tcp://%%NEW_SHARD%%:2020') || [[ -z $new_shard_vnodes ]]; then
	fatal 'could not list vnodes for %%NEW_SHARD%%'
fi

#
# POST the data back to the reshard server.
#
ok=false
for (( retrycount = 0; retrycount < 5; retrycount++ )); do
	if curl --max-time 45 -sSf -X POST \
	    -H 'Content-Type: application/json' -d '@-' \
	    '%%POST_URL%%'; then
		ok=true
		break
	fi <<-EOF
	{
		"%%SHARD%%": $shard_vnodes,
		"%%NEW_SHARD%%": $new_shard_vnodes
	}
	EOF
done

if [[ $ok != true ]]; then
	fatal 'could not POST vnode set back to reshard server'
fi

cleanup
exit 0
