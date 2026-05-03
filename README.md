JPaxos and mPaxos /develop/
===========================

Description
-----------

JPaxos is a Java library and runtime system for building efficient replicated
state machines (highly-available services). With JPaxos it is easy to make
a user-provided service which is tolerant to machine crashes. Our system
supports the crash-recovery model of failure and tolerates message loss and
communication delays.

mPaxos builds on JPaxos and offers a programming framework which leverages 
persistent memory (pmem), also known as Non-Volatile Memory (NVM), for building
more performant replicated state machines, which write critical data to pmem
rather than to stable storage. Parts of mPaxos are built in C++ using PMDK
library. For more on pmem, see: 
https://www.snia.org/technology-focus/persistent-memory.

mPaxosSM is a sibling version of mPaxos that requires that also the 
user-provided service writes to pmem. It is available at: 
https://github.com/JPaxos/mPaxosSM.

All our systems are based on solid theoretical foundations, following the 
state-of-the-art in group communication research.

You are free to use our systems as the experimental platforms for doing 
research on software-based replication, or for any other purposes, provided
that the LGPL 3.0 licence is respected, moreover, any research papers which
are presenting the results obtained with the use of our systems must reference
the appropriate articles enlisted in the LICENCE file and below.

Research Papers
---------------

* Failure Recovery from Persistent Memory in Paxos-based State Machine 
  Replication. Jan Kończak, Paweł T. Wojciechowski. 40th International 
  Symposium on Reliable Distributed Systems (SRDS 2021).

* Recovery Algorithms for Paxos-based State Machine Replication. Jan Kończak,
  Paweł T. Wojciechowski, Nuno Santos, Tomasz Żurkowski and André Schiper.
  IEEE Transactions on Dependable and Secure Computing (TPDS), 
  Volume: 18, Issue: 2, March-April 1, 2021, available at: 
  https://ieeexplore.ieee.org/abstract/document/8758801

* JPaxos: State Machine Replication Based on the Paxos Protocol. Jan Kończak,
  Nuno Santos, Tomasz Żurkowski, Paweł T. Wojciechowski and André Schiper.
  Technical Report 167765, EPFL, July 2011, available at: 
  https://infoscience.epfl.ch/record/167765.

Key Changes
-----------

This branch adds the following extensions on top of the upstream JPaxos:

### Pre-vote

Before initiating a Paxos `Prepare` phase, a candidate broadcasts a
`PreVoteRequest` and waits for a quorum of `PreVoteReply` grants.  A follower
grants the request only when it has not heard from a leader recently, which
prevents unnecessary view changes when the candidate itself is isolated.

### Dynatune — adaptive failure-detector tuning

`ActiveFailureDetector` is extended to dynamically adjust the suspicion
timeout (`E_t`) and the heartbeat interval (`h`) based on observed network
conditions:

- The leader sends periodic `Alive` heartbeats stamped with a sequence number,
  a send timestamp, the last measured RTT to the recipient, and the current
  heartbeat interval.
- Each follower maintains a sliding window of received RTTs and missing-ID
  counts.  Once the window is warm (≥ `DynatuneMinListSize` samples), `E_t`
  is computed as `ceil(safetyFactor × percentile(window, targetProbability))`
  and the suggested heartbeat interval `h = E_t / 2` is piggybacked in the
  `AliveReply`.
- The leader applies per-follower heartbeat intervals suggested in
  `AliveReply` messages and schedules each follower's heartbeat independently.

### E_t randomization

When a follower suspects the leader, the actual suspicion timeout used is
drawn uniformly from `[1.0, 2.0) × E_t`.  This spreads simultaneous view
changes across followers and reduces the probability of split-brain elections.

Baseline
--------

Commit `f26e09f` is the **baseline** for research evaluation: it includes
the pre-vote mechanism and experiment logging, but no Dynatune tuning.

Configuration
-------------

Dynatune is configured in `paxos.properties`:

| Property | Default | Meaning |
|---|---|---|
| `DynatuneEnabled` | `false` | Enable adaptive timeout computation |
| `DynatuneSafetyFactor` | `2.0` | Multiplier applied to the computed percentile |
| `DynatuneHeartbeatProbability` | `0.999` | Target delivery probability (percentile) |
| `DynatuneMinListSize` | `10` | Minimum window samples before Dynatune activates |
| `DynatuneMaxListSize` | `1000` | RTT window size cap |
| `FDSuspectTimeout` | `1000` ms | Static suspicion timeout (when Dynatune is off) |
| `FDSendTimeout` | `500` ms | Heartbeat send interval |

Version
-------

The gitub repository may not contain the most recent version of our systems.
Please query the authors for more recent code, especially if the last commit
is far in the past.

License
-------

This software is distributed under the LGPL 3.0 licence. For license details
please read the LICENCE file.

Contact and authors
-------------------

JPaxos was a joint work between the Distributed System Laboratory (LSR-EPFL)
and Poznan University of Technology (PUT). It has been further developed by
Poznan University of Technology. mPaxos has been developed at PUT.

Institutional pages and contact details:

* EPFL: http://archiveweb.epfl.ch/lsrwww.epfl.ch/
* PUT:  http://www.cs.put.poznan.pl/persistentdatastore/

Contributors:

At EPFL-LSR:

* Andre Schiper (no longer active)
* Nuno Santos (no longer active)
* Lisa Nguyen Quang Do (no longer active)

At PUT:

* Jan Kończak
* Maciej Kokociński
* Tadeusz Kobus
* Paweł T. Wojciechowski
* Tomasz Żurkowski (no longer active)
