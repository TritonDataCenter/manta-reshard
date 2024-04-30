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

## Active Branches

There are currently two active branches of this repository, for the two
active major versions of Manta. See the [mantav2 overview
document](https://github.com/TritonDataCenter/manta/blob/master/docs/mantav2.md)
for details on major Manta versions.

- [`master`](../../tree/master/) - For development of mantav2, the latest
  version of Manta.
- [`mantav1`](../../tree/mantav1/) - For development of mantav1, the long
  term support maintenance version of Manta.
