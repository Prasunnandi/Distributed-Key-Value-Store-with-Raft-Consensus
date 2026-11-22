package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Prasunnandi/Distributed-Key-Value-Store-with-Raft-Consensus/pkg/raft"
	"github.com/Prasunnandi/Distributed-Key-Value-Store-with-Raft-Consensus/pkg/storage"
)

type HTTPTransport struct {
	client *http.Client
}

func NewHTTPTransport() *HTTPTransport {
	return &HTTPTransport{
		client: &http.Client{Timeout: 200 * time.Millisecond},
	}
}

func (t *HTTPTransport) CallRequestVote(ctx context.Context, peerID string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	url := fmt.Sprintf("http://%s/raft/request_vote", peerID)
	data, _ := json.Marshal(args)
	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var reply raft.RequestVoteReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *HTTPTransport) CallAppendEntries(ctx context.Context, peerID string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	url := fmt.Sprintf("http://%s/raft/append_entries", peerID)
	data, _ := json.Marshal(args)
	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var reply raft.AppendEntriesReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *HTTPTransport) CallInstallSnapshot(ctx context.Context, peerID string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return &raft.InstallSnapshotReply{}, nil
}

type Node struct {
	ID        string
	Peers     []string
	Raft      *raft.Raft
	Store     *storage.KVStore
	applyCh   chan raft.ApplyMsg
	transport *HTTPTransport
	mu        sync.RWMutex
}

func NewNode(nodeID string, peers []string) *Node {
	applyCh := make(chan raft.ApplyMsg, 1000)
	transport := NewHTTPTransport()
	store := storage.NewKVStore()

	node := &Node{
		ID:        nodeID,
		Peers:     peers,
		Store:     store,
		applyCh:   applyCh,
		transport: transport,
	}

	node.Raft = raft.NewRaft(nodeID, peers, transport, applyCh)

	go node.applyLoop()
	return node
}

func (n *Node) applyLoop() {
	for msg := range n.applyCh {
		if !msg.CommandValid {
			continue
		}
		switch msg.Op {
		case raft.OpSet:
			n.Store.Set(msg.Key, msg.Value)
		case raft.OpDelete:
			n.Store.Delete(msg.Key)
		}
	}
}

func (n *Node) RegisterRoutes(mux *http.ServeMux) {
	// Raft internal RPC endpoints
	mux.HandleFunc("/raft/request_vote", n.handleRequestVote)
	mux.HandleFunc("/raft/append_entries", n.handleAppendEntries)

	// User client REST API
	mux.HandleFunc("/v1/kv", n.handleKV)
	mux.HandleFunc("/v1/kv/", n.handleKVItem)
	mux.HandleFunc("/v1/cluster/status", n.handleStatus)
	mux.HandleFunc("/v1/cluster/dump", n.handleDump)
}

func (n *Node) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var args raft.RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var reply raft.RequestVoteReply
	n.Raft.HandleRequestVote(&args, &reply)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reply)
}

func (n *Node) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var args raft.AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var reply raft.AppendEntriesReply
	n.Raft.HandleAppendEntries(&args, &reply)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reply)
}

func (n *Node) handleKV(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	body, _ := io.ReadAll(r.Body)
	json.Unmarshal(body, &payload)

	if n.Raft.Role() != raft.RoleLeader {
		http.Error(w, "Not leader. Redirect to cluster leader.", http.StatusTemporaryRedirect)
		return
	}

	idx, term, ok := n.Raft.StartCommand(raft.OpSet, payload.Key, payload.Value)
	if !ok {
		http.Error(w, "Failed to replicate command", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "committed",
		"index":   idx,
		"term":    term,
		"key":     payload.Key,
		"value":   payload.Value,
	})
}

func (n *Node) handleKVItem(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/v1/kv/")

	switch r.Method {
	case "GET":
		val, ok := n.Store.Get(key)
		if !ok {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})

	case "DELETE":
		if n.Raft.Role() != raft.RoleLeader {
			http.Error(w, "Not leader", http.StatusTemporaryRedirect)
			return
		}
		idx, term, _ := n.Raft.StartCommand(raft.OpDelete, key, "")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"status": "deleted", "index": idx, "term": term})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := n.Raft.Status()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (n *Node) handleDump(w http.ResponseWriter, r *http.Request) {
	data := n.Store.All()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}
