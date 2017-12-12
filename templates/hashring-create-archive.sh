#!/bin/bash

NODE='/opt/smartdc/electric-moray/build/node/bin/node'
EM_BIN='/opt/smartdc/electric-moray/node_modules/.bin'
SDC_IMGADM="$EM_BIN/sdc-imgadm"
export SDC_IMGADM_URL='%%HASH_RING_IMGAPI_SERVICE%%'

export PATH='/usr/bin:/usr/sbin:/sbin'

workspace='/var/tmp/reshard.%%WORKSPACE_ID%%'

if ! cd "$workspace"; then
	printf 'ERROR: could not chdir into workspace\n' >&2
	exit 1
fi

#
# Create tar archive of hash ring database.  Note that the "hash_ring"
# directory must appear in the root of the archive, and contain the LevelDB
# files with no other intervening directories.
#
file='new_hash_ring.tar.gz'
if ! tar Ecfz "$file" 'hash_ring'; then
	printf 'ERROR: could not create tar file\n' >&2
	exit 1
fi

#
# Generate an image manifest that describes the hash ring database.
#
if ! file_sha1=$(digest -a sha1 "$file") || ! file_size=$(wc -c "$file"); then
	printf 'ERROR: could not get size or checksum of file\n' >&2
	exit 1
fi
if ! iso_date=$(date -uR +%FT%TZ) || ! image_uuid=$(uuid -v4); then
	printf 'ERROR: could not generate date stamp or image UUID\n' >&2
	exit 1
fi
manifest='manifest.json'
if ! cat >"$manifest"; then
	printf 'ERROR: could not write manifest JSON file\n' >&2
	exit 1
fi <<MANIFEST
{
	"v": 2,
	"uuid": "$image_uuid",
	"owner": "%%POSEIDON_UUID%%",
	"name": "manta-hash-ring",
	"version": "$iso_date",
	"state": "active",
	"public": false,
	"published_at": "$iso_date",
	"type": "other",
	"os": "other",
	"files": [
		{
			"sha1": "$file_sha1",
			"size": "$file_size",
			"compression": "gzip"
		}
	],
	"description": "Manta Hash Ring",
	"tags": {
		"manta_reshard_plan": "%%PLAN_UUID%%",
		"manta_reshard_transition": "%%TRANSITION%%"
	}
}
MANIFEST

#
# Upload the hash ring database to IMGAPI.
#
if ! "$NODE" "$SDC_IMGADM" import -q -m "$manifest" -s "$file_sha1" \
    -f "$file" >&2; then
	printf 'ERROR: could not upload image to IMGAPI.\n' >&2
	exit 1
fi

printf '%s\n' "$image_uuid"
