# Distributed Membership & Failure Detection (SWIM-Inspired)

A lightweight, extensible group membership and failure detection system written in Go.  
Implements a SWIM-style protocol with a pluggable detection mode (currently a Ping/Ack variant; Gossip stubbed for extension), suspicion propagation, piggybacked membership updates, and operational tooling for live cluster introspection.

---

## Core Features

- Membership tracking with incarnation numbers  
- Failure detection via randomized periodic Ping/Ack probing (`PingAckMode`)  
- Suspicion phase before declaring failure using a tunable multi-report policy ([`utils.SuspicionManager`](utils/suspicion.go))  
- Piggyback dissemination of recent membership deltas ([`utils.MembershipList.AddRecentUpdate`](utils/membership.go))  
- Structured statuses: Alive → Suspected → Failed ([`utils.MemberStatus`](utils/types.go))  
- Time–bounded retention of confirmed failures  
- Local HTTP control server for safe automation ([`NewControlServer`](main.go))  
- Interactive CLI ticker summarizing membership health ([`StartCLI`](main.go))  
- Remote / local monitoring script: `scripts/monitor_node.sh`  
- Network abstraction with handler registry & artificial drop-rate injection ([`utils.NetworkLayer`](utils/network.go))  

---

## Architecture Overview

Component | Responsibility
--------- | --------------
Controller ([`Controller`](main.go)) | Orchestrates network, detector mode, suspicion manager
Network Layer ([`utils.NetworkLayer`](utils/network.go)) | UDP messaging, handler dispatch, bounded queue
Membership Store ([`utils.MembershipList`](utils/membership.go)) | Thread-safe state, random peer sampling, update window
Failure Detection ([`detectors.PingAckManager`](detectors/pingack.go)) | Periodic direct + indirect probes, ACK correlation
Suspicion Manager ([`utils.SuspicionManager`](utils/suspicion.go)) | Aggregates suspicion reports, escalates to failure
CLI / Control Plane | HTTP endpoints + periodic status logging
Scripts | Cluster ops (log generation, monitoring)

Data flow (Ping/Ack):
1. Periodic selection of a random target
2. Direct ping → wait for ACK
3. On timeout: indirect probes via k helper nodes
4. On continued silence: declare Suspected ([`PingAckManager.declareSuspicion`](detectors/pingack.go))
5. Suspicion reports aggregated → promote to Failed after timeout or quorum
6. Updates piggybacked onto outgoing protocol messages

---

## Getting Started

### Build
```
go build -o mp2-node .
```

### Run (single node introducer)
```
./mp2-node -port 8080 -is-introducer
```

### Join from another node
```
./mp2-node -port 8081 -introducer 127.0.0.1:8080
```

### Control Commands (local HTTP)
```
./mp2-node -cmd list_mem
./mp2-node -cmd list_self
./mp2-node -cmd display_suspects
```

### Monitoring (live suspicion events)
```
./scripts/monitor_node.sh localhost 8080
```

---

## Command-Line Flags (subset)

Flag | Description
---- | -----------
`-port` | UDP listen port (default 8080)
`-introducer` | Introducer ip:port to join
`-is-introducer` | Start as seed node
`-mode` | `gossip` (stub) or `pingack`
`-cmd` | One-off control client command
`-control-port` | Override local HTTP control port (defaults to port+10000)
`-foreground` | Skip daemonization

---

## HTTP Control Endpoints

Endpoint | Purpose
-------- | -------
`/list_mem` | Current membership view
`/list_self` | Local node identity
`/display_suspects` | Active suspicion entries
`/join?introducer=IP:PORT` | Force join
`/leave` | Voluntary leave (graceful)
`/switch` | Switch detection mode / suspicion toggle (future extension)
`/display_protocol` | Active mode

Served on `127.0.0.1:<control-port>`.

---

## Status Lifecycle

State | Trigger | Notes
----- | ------- | -----
Alive | Heartbeats / pings observed | Normal operation
Suspected | Timeout & insufficient ACKs | `SuspicionTimeout`, quorum-based escalation
Failed | Suspicion confirmed | Retained temporarily for convergence

See [`PingAckManager.performSWIMProtocolPeriod`](detectors/pingack.go) and [`SuspicionManager.loop`](utils/suspicion.go).

---

## Design Choices

- Incarnation numbering avoids stale overwrites
- Indirect probing reduces false positives under transient network loss
- Batching: recent updates window limits redundant payload growth
- Separation of suspicion vs. failure lowers incorrect failure declarations
- Deterministic local-only control API avoids exposing cluster mutation externally

---

## Extensibility Roadmap

- Activate full gossip-based dissemination engine
- Adaptive probe intervals based on recent stability
- Metrics / Prometheus exporter
- Encryption / Auth for control plane
- Pluggable transport (QUIC / TCP fallback)

---

## Development & Testing

```
go vet ./...
go test ./...   # (Add tests; current suite minimal)
```

Simulate packet loss (future flag hook):
- Introduce adjustable drop rate in `NetworkLayer`.

---

## Repository Scripts

Script | Description
------ | -----------
`scripts/log_generator.sh` | Generate large logs across hosts
`scripts/monitor_node.sh` | Interactive suspicion dashboard

---

## Key Source References

- Controller startup: [`Controller.Start`](main.go)
- Membership lifecycle: [`MembershipList`](utils/membership.go)
- Failure detection loop: [`PingAckManager.performSWIMProtocolPeriod`](detectors/pingack.go)
- Suspicion escalation: [`SuspicionManager`](utils/suspicion.go)
- Types & statuses: [`Member`, `MemberStatus`](utils/types.go)
