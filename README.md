<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2017, Joyent, Inc.
-->

# Manta Resharding System

This repository is part of the Joyent Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/joyent/manta) project page.

This is the resharding system for Manta.  It comprises a service and a set of
client tools for breaking up Moray shards within the Manta indexing tier to
increase the performance and storage capacity of the system.

## Before pushing changes

- Your code should be `make prepush` clean.  That includes both `make check` and
  `make test` (the test suite).
- Assess the test impact of your changes -- make sure you've run all manual
  tests you can think of, and automated all of the ones that can reasonably be
  automated.
- Assess any impact on fresh install / deployment and make sure you've tested
  that if necessary.
- Assess any impact on upgrade, including flag days for developers or existing
  deployments.
- Code review (via Gerrit) is required.
