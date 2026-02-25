<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2019 Joyent, Inc.
    Copyright 2024 MNX Cloud, Inc.
-->

# Manta Resharding System

This repository is part of the Triton Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/TritonDataCenter/manta) project page.

This is the resharding system for Manta.  It comprises a service and a set of
client tools for breaking up Moray shards within the Manta indexing tier to
increase the performance and storage capacity of the system.

It works by splitting a Manta metadata (index) shard into two new shards
and redistributing virtual nodes (vnodes) in the consistent hash ring used by
Electric Moray for request routing.

## Active Branches

There are currently two active branches of this repository, for the two
active major versions of Manta. See the [mantav2 overview
document](https://github.com/TritonDataCenter/manta/blob/master/docs/mantav2.md)
for details on major Manta versions.

- [`master`](../../tree/master/) - For development of mantav2, the latest
  version of Manta.
- [`mantav1`](../../tree/mantav1/) - For development of mantav1, the long
  term support maintenance version of Manta.

## Overview

Manta stores metadata across multiple Moray/PostgreSQL shards.  Electric Moray
maps each incoming key to a vnode via a consistent hash, then maps that vnode to
a physical shard (pnode).  When a shard grows too large, `manta-reshard` splits
it by creating a new Manatee cluster, remapping half the vnodes to the new
shard, and cleaning up leftover data.

The reshard operation executes as a 20-phase plan that is persisted in Moray and
can survive service restarts.  Plans advance automatically, and can be paused,
resumed, or held on error for operator intervention.

## Components

`manta-reshard` interacts with:

- **Electric Moray** -- placement/routing service that owns the hash ring.
  The ring image (`HASH_RING_IMAGE` in SAPI metadata) is stored in IMGAPI.
  The shard list is `INDEX_MORAY_SHARDS` in the Manta SAPI application.
- **Moray** -- metadata storage.  `manta-reshard` stores its own plans and
  locks in the `manta_reshard_plans` and `manta_reshard_locks` buckets on
  a dedicated Moray shard.
- **Manatee** -- PostgreSQL replication.  New Manatee peers are provisioned
  for the target shard, then reconfigured into a standalone cluster.
- **SAPI** -- source of shard configuration.  Phases read and update
  `INDEX_MORAY_SHARDS` and `HASH_RING_IMAGE`.
- **IMGAPI** -- stores hash ring tar archives as images.
- **Ur** -- used for remote script execution on compute nodes.

## Reshard Phases

Each plan advances through these phases in order:

| # | Phase | Purpose |
|---|-------|---------|
| 1 | `check_postgres_version` | Verify PostgreSQL image compatibility. |
| 2 | `freeze_cluster` | Freeze the source Manatee cluster to prevent topology changes. |
| 3 | `create_sentinel_bucket` | Create a sentinel Moray bucket on the source shard to detect stale async peers. |
| 4 | `create_manatee_peers` | Provision new Manatee peers for the target shard. |
| 5 | `critical_section_enter` | Acquire a lock to serialise ring modifications across concurrent plans. |
| 6 | `remap_ring` | Download the current hash ring, remap vnodes to the new shard, upload the new ring to IMGAPI. |
| 7 | `update_sapi_mark_readonly` | Set `readOnly: true` on the source shard in `INDEX_MORAY_SHARDS`. |
| 8 | `restart_electric_moray_readonly` | Restart all Electric Moray instances and verify they report the shard as read-only. |
| 9 | `shutdown_new_peers` | Shut down the newly provisioned Manatee peers before reconfiguration. |
| 10 | `reconfigure_peers` | Reconfigure the new peers into an independent Manatee cluster for the target shard. |
| 11 | `establish_cluster` | Wait for the new Manatee cluster to establish and reach a healthy state. |
| 12 | `create_morays` | Provision Moray instances for the target shard. |
| 13 | `install_ring` | Update the `HASH_RING_IMAGE` SAPI metadata to point to the new ring. |
| 14 | `update_electric_moray_ring` | Download and install the new ring in every Electric Moray zone, restarting each instance. |
| 15 | `update_sapi_mark_readwrite` | Remove `readOnly` flags and add the new shard to `INDEX_MORAY_SHARDS`. |
| 16 | `restart_electric_moray_readwrite` | Restart all Electric Moray instances and verify read-write status. |
| 17 | `sapi_cleanup` | Clean up any temporary SAPI metadata. |
| 18 | `critical_section_exit` | Release the serialisation lock. |
| 19 | `delete_data` | Delete ghost data from both shards (rows whose vnodes now belong to the other shard). |
| 20 | `unfreeze_cluster` | Unfreeze the source Manatee cluster. |

Phases 5-18 are serialised: only one plan may be in this section at a time.

## REST API

The server listens on port 80 and exposes:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/ping` | Health check. |
| `GET` | `/phases` | List phase names. |
| `GET` | `/plans` | List all active plans with status. |
| `POST` | `/plan` | Create a new reshard plan. Body: `{ "plan_opts": { "shard", "new_shard", "server_list", "split_count", "postgres_image" } }` |
| `POST` | `/plan/:uuid/pause` | Pause a plan. Optional body: `{ "pause_at_phase": "<phase_name>" }` |
| `POST` | `/plan/:uuid/resume` | Resume a paused plan. |
| `POST` | `/plan/:uuid/archive` | Archive (deactivate) a completed plan. |
| `POST` | `/plan/:uuid/unhold` | Release a plan held on error. |
| `GET` | `/plan/:uuid/tune` | Get tuning parameters. |
| `POST` | `/plan/:uuid/tune/:name` | Set a tuning parameter. Body: `{ "tuning_value": <number|null> }` |

## CLI Tools

Located in `bin/` and `cmd/`.  All communicate with the local server on
`127.0.0.1:80`.

| Command | Description |
|---------|-------------|
| `cmd/status.js` | Display plan status.  `-r` redraws continuously, `-x` shows only errors. |
| `cmd/plan_list.js` | List active plan UUIDs. |
| `cmd/plan_get.js` | Get details of a single plan. |
| `cmd/plan_pause.js` | Pause a plan. |
| `cmd/plan_resume.js` | Resume a paused plan. |
| `cmd/plan_unhold.js` | Release a held plan. |
| `cmd/plan_archive.js` | Archive a plan. |
| `cmd/plan_tune.js` | View or set tuning parameters. |
| `cmd/phases.js` | List all phases. |

## Shell Templates

The `templates/` directory contains bash scripts rendered at runtime with
`%%VARIABLE%%` placeholders.  These run inside Electric Moray zones via Ur to
perform hash ring operations:

- `hashring-prime-workspace.sh` -- set up a temporary workspace and download the current ring from IMGAPI.
- `hashring-remap-vnodes.sh` -- remap vnodes from the source pnode to the target pnode using `fash`.
- `hashring-create-archive.sh` -- create a tar archive of the updated ring and upload it to IMGAPI.
- `hashring-cleanup.sh` -- remove the temporary workspace.
- `electric-moray-update-ring.sh` -- download a ring image from IMGAPI and install it in an Electric Moray zone.
- `electric-moray-get-vnodes.sh` -- query the local ring for vnodes assigned to a given pnode.

## Configuration

Deployed as a SAPI service in the Manta application.  Configuration is
generated from `sapi_manifests/manta-reshard/template` and includes the SAPI
URL, UFDS credentials, DNS domain, and the Moray shard connection used for
plan storage.
