package raft

import (
	"encoding/json"
	"fmt"
	"sync"
)

// OpType represents the operation kind
type OpType string

const (
	OpSet    OpType = "SET"
	OpDelete OpType = "DELETE"
	OpNoOp   OpType = "NOOP"
)

// LogEntry encapsulates a single state-machine transition in Raft
type LogEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Op    OpType `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// RaftLog manages in-memory log entries with zero-based snapshot compaction offset
type RaftLog struct {
	mu            sync.RWMutex
	entries       []LogEntry
	snapshotIndex uint64
	snapshotTerm  uint64
}

func NewRaftLog() *RaftLog {
	return &RaftLog{
		entries: []LogEntry{
			{Index: 0, Term: 0, Op: OpNoOp}, // Sentinel dummy entry
		},
		snapshotIndex: 0,
		snapshotTerm:  0,
	}
}

func (rl *RaftLog) LastLogInfo() (uint64, uint64) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	last := rl.entries[len(rl.entries)-1]
	return last.Index, last.Term
}

func (rl *RaftLog) Append(entry LogEntry) uint64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	last := rl.entries[len(rl.entries)-1]
	entry.Index = last.Index + 1
	rl.entries = append(rl.entries, entry)
	return entry.Index
}

func (rl *RaftLog) GetEntry(index uint64) (LogEntry, error) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	if index < rl.snapshotIndex || index >= rl.snapshotIndex+uint64(len(rl.entries)) {
		return LogEntry{}, fmt.Errorf("index %d out of bounds (compacted at %d)", index, rl.snapshotIndex)
	}
	idx := index - rl.snapshotIndex
	return rl.entries[idx], nil
}

func (rl *RaftLog) SliceFrom(index uint64) []LogEntry {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	if index < rl.snapshotIndex {
		return nil
	}
	idx := index - rl.snapshotIndex
	if idx >= uint64(len(rl.entries)) {
		return nil
	}
	result := make([]LogEntry, len(rl.entries[idx:]))
	copy(result, rl.entries[idx:])
	return result
}

func (rl *RaftLog) TruncateAndAppend(prevLogIndex uint64, newEntries []LogEntry) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	idx := prevLogIndex - rl.snapshotIndex
	if idx+1 < uint64(len(rl.entries)) {
		rl.entries = rl.entries[:idx+1]
	}
	rl.entries = append(rl.entries, newEntries...)
}

func (rl *RaftLog) Compact(snapshotIndex uint64, snapshotTerm uint64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if snapshotIndex <= rl.snapshotIndex {
		return
	}

	idx := snapshotIndex - rl.snapshotIndex
	if idx >= uint64(len(rl.entries)) {
		rl.entries = []LogEntry{{Index: snapshotIndex, Term: snapshotTerm, Op: OpNoOp}}
	} else {
		newEntries := make([]LogEntry, 0, len(rl.entries)-int(idx)+1)
		newEntries = append(newEntries, LogEntry{Index: snapshotIndex, Term: snapshotTerm, Op: OpNoOp})
		newEntries = append(newEntries, rl.entries[idx+1:]...)
		rl.entries = newEntries
	}

	rl.snapshotIndex = snapshotIndex
	rl.snapshotTerm = snapshotTerm
}

func (rl *RaftLog) MarshalJSON() ([]byte, error) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return json.Marshal(rl.entries)
}
