package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/Prasunnandi/Distributed-Key-Value-Store-with-Raft-Consensus/pkg/cluster"
)

func main() {
	nodeID := flag.String("id", "localhost:8001", "Node listen address and ID (e.g. localhost:8001)")
	peersFlag := flag.String("peers", "localhost:8001,localhost:8002,localhost:8003", "Comma-separated cluster peers")
	flag.Parse()

	peers := strings.Split(*peersFlag, ",")

	fmt.Printf("==================================================================\n")
	fmt.Printf("  DISTRIBUTED RAFT KEY-VALUE STORE NODE STARTING\n")
	fmt.Printf("  Node ID: %s | Peers: %v\n", *nodeID, peers)
	fmt.Printf("==================================================================\n")

	node := cluster.NewNode(*nodeID, peers)
	mux := http.NewServeMux()
	node.RegisterRoutes(mux)

	log.Printf("Listening on %s ...", *nodeID)
	if err := http.ListenAndServe(*nodeID, mux); err != nil {
		log.Fatalf("Server exited with error: %v", err)
	}
}
