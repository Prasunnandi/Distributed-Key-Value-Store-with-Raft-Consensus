package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  raft-cli status [node-url]")
	fmt.Println("  raft-cli set <key> <val> [node-url]")
	fmt.Println("  raft-cli get <key> [node-url]")
	fmt.Println("  raft-cli delete <key> [node-url]")
	fmt.Println("  raft-cli dump [node-url]")
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		return
	}

	command := os.Args[1]
	nodeURL := "http://localhost:8001"

	switch command {
	case "status":
		if len(os.Args) > 2 {
			nodeURL = os.Args[2]
		}
		resp, err := http.Get(nodeURL + "/v1/cluster/status")
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Cluster Status: %s\n", string(body))

	case "set":
		if len(os.Args) < 4 {
			fmt.Println("Usage: raft-cli set <key> <val> [node-url]")
			return
		}
		key, val := os.Args[2], os.Args[3]
		if len(os.Args) > 4 {
			nodeURL = os.Args[4]
		}
		payload, _ := json.Marshal(map[string]string{"key": key, "value": val})
		resp, err := http.Post(nodeURL+"/v1/kv", "application/json", bytes.NewBuffer(payload))
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Response: %s\n", string(body))

	case "get":
		if len(os.Args) < 3 {
			fmt.Println("Usage: raft-cli get <key> [node-url]")
			return
		}
		key := os.Args[2]
		if len(os.Args) > 3 {
			nodeURL = os.Args[3]
		}
		resp, err := http.Get(nodeURL + "/v1/kv/" + key)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Result: %s\n", string(body))

	case "dump":
		if len(os.Args) > 2 {
			nodeURL = os.Args[2]
		}
		resp, err := http.Get(nodeURL + "/v1/cluster/dump")
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("KV Store Dump: %s\n", string(body))

	default:
		printUsage()
	}
}
