Dynatune for Multi-Paxos: Dynamic Election Parameter Tuning for Fast Leader Failover
====================================================================================

This repository is forked from [JPaxos/Jpaxos](https://github.com/JPaxos/Jpaxos)
at commit
[`cad00384dc1e69f2f4bf53b7627eea9c66af55ed`](https://github.com/JPaxos/Jpaxos/commit/cad00384dc1e69f2f4bf53b7627eea9c66af55ed).

This repository provides a research implementation of Dynatune integrated into
[JPaxos](https://github.com/JPaxos/Jpaxos). Dynatune enables dynamic tuning of
election parameters for faster and more stable leader failover. This
implementation is used in the evaluation of our research paper on timely leader
failover in state machine replication.

For information about JPaxos itself, please refer to the
[original JPaxos README](https://github.com/JPaxos/Jpaxos/blob/master/README.md).

Key Changes
-----------

The following changes are introduced on top of JPaxos from commit
[`cad00384dc1e69f2f4bf53b7627eea9c66af55ed`](https://github.com/JPaxos/Jpaxos/commit/cad00384dc1e69f2f4bf53b7627eea9c66af55ed):

* **Dynatune integration**: Introduces Dynatune, which dynamically adjusts
  election parameters based on network conditions measured via heartbeat
  exchanges in WAN environments. It enables faster and more stable leader
  failover under fluctuating network conditions, reducing service downtime.

* **Experiment log instrumentation**: Adds explicit log markers to capture key
  events in the leader failover process for evaluation in the research paper
  and in the accompanying benchmark tools.

Research Paper
--------------

This implementation is used in the following work:

* *Dynamic Tuning of Election Parameters for Timely Leader Failover in State
  Machine Replication*  
  IEEE Access (under review; details to be announced)

The evaluation compares a JPaxos baseline (with experiment logs) against a
Dynatune-integrated version of JPaxos:

* **Baseline**: JPaxos with experiment logs, based on commit
  [`81655f0a7841f4bc8b61a14922e1d6ab64a1484d`](https://github.com/skoya76/JPaxos/commit/81655f0a7841f4bc8b61a14922e1d6ab64a1484d)
* **Dynatune**: JPaxos with experiment logs and Dynatune, using the latest
  commits in this repository

Previous Publications
---------------------

* Dynatune: Dynamic Tuning of Raft Election Parameters Using Network Measurement
  APDCM 2025 (IPDPS Workshop) — Preprint: https://arxiv.org/abs/2507.15154

License
-------

This project is a fork of JPaxos and is distributed under the
[LGPL-3.0](./LICENSE) license, following the licensing terms of the original
JPaxos project. The modifications introduced in this repository, including
Dynatune integration and experiment log instrumentation, are also distributed
under LGPL-3.0.

Original JPaxos authors: Distributed System Laboratory (LSR-EPFL) and Poznan
University of Technology (PUT).
