package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type NodeRole string

const (
	RoleFollower  NodeRole = "FOLLOWER"
	RoleCandidate NodeRole = "CANDIDATE"
	RoleLeader    NodeRole = "LEADER"
)

// ApplyMsg communicated to state machine on log commit
type ApplyMsg struct {
	CommandValid bool
	CommandIndex uint64
	CommandTerm  uint64
	Op           OpType
	Key          string
	Value        string
}

// PeerTransport interface for inter-node RPC
type PeerTransport interface {
	CallRequestVote(ctx context.Context, peerID string, args *RequestVoteArgs) (*RequestVoteReply, error)
	CallAppendEntries(ctx context.Context, peerID string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	CallInstallSnapshot(ctx context.Context, peerID string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Raft consensus module
type Raft struct {
	mu        sync.RWMutex
	peers     []string
	nodeID    string
	transport PeerTransport
	applyCh   chan ApplyMsg

	// Persistent state
	currentTerm uint64
	votedFor    string
	log         *RaftLog

	// Volatile state
	commitIndex uint64
	lastApplied uint64
	role        NodeRole

	// Leader-specific volatile state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Timers
	electionTimeout time.Duration
	lastHeartbeat   time.Time
	stopCh          chan struct{}
}

func NewRaft(nodeID string, peers []string, transport PeerTransport, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:           peers,
		nodeID:          nodeID,
		transport:       transport,
		applyCh:         applyCh,
		currentTerm:     0,
		votedFor:        "",
		log:             NewRaftLog(),
		commitIndex:     0,
		lastApplied:     0,
		role:            RoleFollower,
		nextIndex:       make(map[string]uint64),
		matchIndex:      make(map[string]uint64),
		electionTimeout: randomizeElectionTimeout(),
		lastHeartbeat:   time.Now(),
		stopCh:          make(chan struct{}),
	}

	go rf.runElectionLoop()
	return rf
}

func randomizeElectionTimeout() time.Duration {
	base := 150
	jitter := rand.Intn(150)
	return time.Duration(base+jitter) * time.Millisecond
}

func (rf *Raft) Role() NodeRole {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.role
}

func (rf *Raft) Term() uint64 {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm
}

func (rf *Raft) Status() map[string]interface{} {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	lastIdx, lastTerm := rf.log.LastLogInfo()
	return map[string]interface{}{
		"node_id":      rf.nodeID,
		"role":         rf.role,
		"term":         rf.currentTerm,
		"commit_index": rf.commitIndex,
		"last_applied": rf.lastApplied,
		"last_log_idx": lastIdx,
		"last_log_trm": lastTerm,
		"voted_for":    rf.votedFor,
	}
}

// RequestVote RPC handler
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = RoleFollower
		rf.votedFor = ""
	}

	lastIdx, lastTerm := rf.log.LastLogInfo()
	isLogUpToDate := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if (rf.votedFor == "" || rf.votedFor == args.CandidateID) && isLogUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.lastHeartbeat = time.Now()
	}
}

// AppendEntries RPC handler (Heartbeats and Log Replication)
func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.lastHeartbeat = time.Now()

	if args.Term > rf.currentTerm || rf.role == RoleCandidate {
		rf.currentTerm = args.Term
		rf.role = RoleFollower
		rf.votedFor = ""
	}

	// Consistency Check: verify prevLogIndex and prevLogTerm
	lastIdx, _ := rf.log.LastLogInfo()
	if args.PrevLogIndex > lastIdx {
		reply.ConflictIndex = lastIdx + 1
		return
	}

	if args.PrevLogIndex > 0 {
		entry, err := rf.log.GetEntry(args.PrevLogIndex)
		if err != nil || entry.Term != args.PrevLogTerm {
			reply.ConflictIndex = args.PrevLogIndex
			return
		}
	}

	// Append any new entries not already in the log
	if len(args.Entries) > 0 {
		rf.log.TruncateAndAppend(args.PrevLogIndex, args.Entries)
	}

	// Update commit index
	if args.LeaderCommit > rf.commitIndex {
		newLastIdx, _ := rf.log.LastLogInfo()
		if args.LeaderCommit < newLastIdx {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = newLastIdx
		}
		go rf.applyEntries()
	}

	reply.Success = true
}

func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		entry, err := rf.log.GetEntry(rf.lastApplied)
		if err == nil && entry.Op != OpNoOp {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
				Op:           entry.Op,
				Key:          entry.Key,
				Value:        entry.Value,
			}
		}
	}
}

// StartCommand proposes a new command to the Raft cluster
func (rf *Raft) StartCommand(op OpType, key string, value string) (uint64, uint64, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != RoleLeader {
		return 0, 0, false
	}

	entry := LogEntry{
		Term:  rf.currentTerm,
		Op:    op,
		Key:   key,
		Value: value,
	}
	index := rf.log.Append(entry)
	rf.matchIndex[rf.nodeID] = index
	rf.nextIndex[rf.nodeID] = index + 1

	go rf.broadcastAppendEntries()
	return index, rf.currentTerm, true
}

func (rf *Raft) runElectionLoop() {
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rf.stopCh:
			return
		case <-ticker.C:
			rf.mu.Lock()
			elapsed := time.Since(rf.lastHeartbeat)
			role := rf.role
			timeout := rf.electionTimeout
			rf.mu.Unlock()

			if role != RoleLeader && elapsed > timeout {
				rf.startElection()
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.role = RoleCandidate
	rf.currentTerm++
	rf.votedFor = rf.nodeID
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = randomizeElectionTimeout()

	term := rf.currentTerm
	lastIdx, lastTerm := rf.log.LastLogInfo()
	peers := make([]string, len(rf.peers))
	copy(peers, rf.peers)
	rf.mu.Unlock()

	votes := 1 // Vote for self
	var voteMu sync.Mutex

	for _, peer := range peers {
		if peer == rf.nodeID {
			continue
		}
		go func(p string) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.nodeID,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			reply, err := rf.transport.CallRequestVote(ctx, p, args)
			if err != nil || reply == nil {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = RoleFollower
				rf.votedFor = ""
				return
			}

			if rf.role == RoleCandidate && rf.currentTerm == term && reply.VoteGranted {
				voteMu.Lock()
				votes++
				hasMajority := votes > (len(peers)+1)/2
				voteMu.Unlock()

				if hasMajority {
					rf.role = RoleLeader
					lastIdx, _ := rf.log.LastLogInfo()
					for _, pr := range peers {
						rf.nextIndex[pr] = lastIdx + 1
						rf.matchIndex[pr] = 0
					}
					go rf.runLeaderLoop()
				}
			}
		}(peer)
	}
}

func (rf *Raft) runLeaderLoop() {
	ticker := time.NewTicker(50 * time.Millisecond) // 50ms heartbeat
	defer ticker.Stop()

	for {
		rf.mu.Lock()
		if rf.role != RoleLeader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		rf.broadcastAppendEntries()

		select {
		case <-rf.stopCh:
			return
		case <-ticker.C:
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.RLock()
	if rf.role != RoleLeader {
		rf.mu.RUnlock()
		return
	}
	term := rf.currentTerm
	leaderCommit := rf.commitIndex
	peers := make([]string, len(rf.peers))
	copy(peers, rf.peers)
	rf.mu.RUnlock()

	for _, peer := range peers {
		if peer == rf.nodeID {
			continue
		}
		go func(p string) {
			rf.mu.RLock()
			nextIdx := rf.nextIndex[p]
			prevLogIndex := nextIdx - 1
			prevLogTerm := uint64(0)
			if prevLogIndex > 0 {
				if prevEntry, err := rf.log.GetEntry(prevLogIndex); err == nil {
					prevLogTerm = prevEntry.Term
				}
			}
			entries := rf.log.SliceFrom(nextIdx)
			rf.mu.RUnlock()

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     rf.nodeID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			reply, err := rf.transport.CallAppendEntries(ctx, p, args)
			if err != nil || reply == nil {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = RoleFollower
				rf.votedFor = ""
				return
			}

			if rf.role == RoleLeader && rf.currentTerm == term {
				if reply.Success {
					rf.nextIndex[p] = prevLogIndex + uint64(len(entries)) + 1
					rf.matchIndex[p] = prevLogIndex + uint64(len(entries))
					rf.checkAndUpdateCommitIndex()
				} else {
					if rf.nextIndex[p] > 1 {
						rf.nextIndex[p]--
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) checkAndUpdateCommitIndex() {
	lastIdx, _ := rf.log.LastLogInfo()
	for n := rf.commitIndex + 1; n <= lastIdx; n++ {
		entry, err := rf.log.GetEntry(n)
		if err != nil || entry.Term != rf.currentTerm {
			continue
		}
		matches := 1 // self
		for _, peer := range rf.peers {
			if peer != rf.nodeID && rf.matchIndex[peer] >= n {
				matches++
			}
		}
		if matches > (len(rf.peers)+1)/2 {
			rf.commitIndex = n
			go rf.applyEntries()
		}
	}
}

func (rf *Raft) Stop() {
	close(rf.stopCh)
}
