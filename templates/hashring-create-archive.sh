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
# Create tar archive of hash ring database.
#
if ! tar Ecfz 'new_hash_ring.tar.gz' 'hash_ring'; then
	printf 'ERROR: could not create tar file\n' >&2
	exit 1
fi

#
# Upload the hash ring database to the resharding server.
#
if ! curl -sSf -T 'new_hash_ring.tar.gz' '%%PUT_FILE_URL%%' >/dev/null; then
	printf 'ERROR: could not store hash ring to reshard server\n' >&2
	exit 1
fi

#
# Emit the MD5 sum for the uploaded file.
#
digest -a md5 'new_hash_ring.tar.gz'
