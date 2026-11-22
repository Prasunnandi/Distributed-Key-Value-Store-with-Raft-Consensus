# Distributed In-Memory Key-Value Store with Raft Consensus

[![Go](https://img.shields.io/badge/Go-1.21-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Consensus](https://img.shields.io/badge/Protocol-Raft%20(Ongaro%20%26%20Ousterhout)-purple.svg)]()
[![Linearizable](https://img.shields.io/badge/Consistency-Linearizable%20(Strong)-brightgreen.svg)]()
[![Tests](https://img.shields.io/badge/Chaos%20Tests-100%25%20Passed-success.svg)]()
[![License](https://img.shields.io/badge/License-MIT-blue.svg)]()

A production-grade distributed in-memory key-value store engineered in Go, utilizing the **Raft consensus algorithm** (based on Diego Ongaro and John Ousterhout's Stanford paper). Designed for high availability, fault tolerance, split-brain immunity, and linearizable operations under network partitions and sudden leader failures.

---

## 🏛️ Architecture & Raft Protocol Overview

```
                          CLIENT WRITE / READ REQUEST
                                       │
                                       ▼
                       ┌───────────────────────────────┐
                       │  HTTP / REST API Gateway      │
                       │  (/v1/kv, /v1/cluster/status) │
                       └───────────────┬───────────────┘
                                       │
                                       ▼
     ┌──────────────────────────────────────────────────────────────────┐
     │                    RAFT CLUSTER TOPOLOGY                         │
     │                                                                  │
     │   ┌─────────────────────┐               ┌─────────────────────┐  │
     │   │   Node 1 (8001)     │  Heartbeats   │    Node 2 (8002)    │  │
     │   │      LEADER 👑      │──────────────▶│       FOLLOWER      │  │
     │   │  (Term T, Commit C) │               │  (Term T, Commit C) │  │
     │   └──────────┬──────────┘               └──────────┬──────────┘  │
     │              │                                     │             │
     │              │ AppendEntries (RPC)                 │             │
     │              ▼                                     ▼             │
     │   ┌─────────────────────────────────────────────────────────┐    │
     │   │                      Node 3 (8003)                      │    │
     │   │                         FOLLOWER                        │    │
     │   └─────────────────────────────────────────────────────────┘    │
     └─────────────────────────────────┬────────────────────────────────┘
                                       │
                       ┌───────────────┴───────────────┐
                       │   Quorum Replicated Commit    │
                       │   (Matches >= (N/2) + 1)      │
                       └───────────────┬───────────────┘
                                       │
                                       ▼
                       ┌───────────────────────────────┐
                       │  Linearizable State Machine   │
                       │      (In-Memory KV Store)     │
                       └───────────────────────────────┘
```

---

## ⚡ Core Protocol Guarantees

1. **Election Safety:** At most one leader can be elected in a given term.
2. **Leader Append-Only:** A leader never overwrites or truncates its own log entries; it only appends new entries.
3. **Log Matching Property:** If two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index.
4. **Leader Completeness:** If a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms.
5. **State Machine Safety:** If a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.
6. **Split-Brain Prevention:** In the event of an asymmetric network partition (e.g. 1 node isolated from 2), writes to the minority partition are blocked due to inability to assemble a majority quorum ($N/2 + 1$).

---

## 🧪 Chaos Testing & Split-Brain Verification Suite

A built-in simulator test runner (`simulator/verify_raft.py`) executes chaos fault injections:

| Test Case | Scenario | Expected Outcome | Status |
| :--- | :--- | :--- | :--- |
| **Test 1** | Normal 3-node initialization | Leader elected with majority quorum | **PASSED** |
| **Test 2** | Client command replication | Key-value committed to state machine on all nodes | **PASSED** |
| **Test 3** | Network Partition (Isolate leader) | Minority partition blocks writes | **PASSED** |
| **Test 4** | Majority partition reelection | New leader elected in Term 2, commits continue | **PASSED** |
| **Test 5** | Partition healing & reconciliation | Reconnected follower rolls back uncommitted log | **PASSED** |

### Run Verification Suite
```bash
python simulator/verify_raft.py
```

Output:
```
=================================================================
  RAFT DISTRIBUTED CONSENSUS SIMULATOR & CHAOS VERIFICATION
=================================================================

[TEST 1] Leader Election under Normal Conditions...
 [PASSED] node1 successfully elected Leader (Term 1, Quorum 2/3)

[TEST 2] Log Replication across 3 nodes...
 [PASSED] State machine replicated to all nodes: Committed at index 1

[TEST 3] Network Partition (Isolating node1 in minority partition)...
 [PASSED] Minority leader write safely blocked: Failed to achieve quorum replication (minority partition)

[TEST 4] Majority Partition Election & Higher Term Progress...
 [PASSED] New leader node2 in Term 2 committed write: Committed at index 2

[TEST 5] Partition Healing & Rollback of Uncommitted Logs...
 [PASSED] Node1 safely rolled back uncommitted log, synced to Term 2 state machine

=================================================================
 ALL RAFT CONSENSUS CHAOS SUITES PASSED SUCCESSFULLY (100% PASS RATE)!
=================================================================
```

---

## 🚀 Running the Cluster

### 1. Launch a 3-Node Cluster
In 3 separate terminal sessions:
```bash
# Terminal 1 (Node 1)
go run cmd/server/main.go -id localhost:8001 -peers localhost:8001,localhost:8002,localhost:8003

# Terminal 2 (Node 2)
go run cmd/server/main.go -id localhost:8002 -peers localhost:8001,localhost:8002,localhost:8003

# Terminal 3 (Node 3)
go run cmd/server/main.go -id localhost:8003 -peers localhost:8001,localhost:8002,localhost:8003
```

### 2. Client Operations via `raft-cli`
```bash
# Check cluster status and current term leader
go run cmd/cli/main.go status http://localhost:8001

# Write a key-value pair through leader
go run cmd/cli/main.go set "user:100" '{"name":"Alice","tier":"pro"}' http://localhost:8001

# Read linearizable state from any follower
go run cmd/cli/main.go get "user:100" http://localhost:8002

# Dump full synchronized state machine
go run cmd/cli/main.go dump http://localhost:8003
```

### 3. Interactive Web Dashboard
Open `web/index.html` in any modern web browser to interactively simulate:
- Dynamic cluster topologies
- Partition injection and split-brain resolution
- Real-time log append and commit index animations

---

## 📁 Repository Directory Structure

```
├── cmd/
│   ├── cli/main.go              # Interactive CLI client tool
│   └── server/main.go           # Multi-node server daemon
├── pkg/
│   ├── cluster/
│   │   └── node.go              # HTTP transport & REST API handlers
│   ├── raft/
│   │   ├── log.go               # Raft log compaction & slicing
│   │   ├── raft.go              # Core Raft state machine & timers
│   │   └── rpc.go               # RequestVote & AppendEntries structs
│   └── storage/
│       └── kvstore.go           # In-memory thread-safe state machine
├── simulator/
│   └── verify_raft.py           # Chaos test harness and partition simulator
├── web/
│   └── index.html               # Interactive visual cluster dashboard
├── go.mod                       # Go 1.21 module configuration
└── README.md                    # Technical documentation and architecture
```

---

## 📜 License
MIT License. Created by [Prasun Nandi](https://github.com/Prasunnandi).
