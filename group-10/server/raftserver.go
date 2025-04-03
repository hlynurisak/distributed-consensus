package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/mkyas/miniraft"
)

func main() {
	// Check command-line arguments: server identity and server configuration file.
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run raftserver.go server-host:server-port filename")
		os.Exit(1)
	}
	selfID := os.Args[1]
	configFile := os.Args[2]

	// Load peer server addresses from config file.
	peers, err := loadServerConfig(configFile)
	if err != nil {
		log.Fatalf("Failed to load server config: %v", err)
	}

	// Verify that the server's identity is in the config file.
	if !contains(peers, selfID) {
		log.Fatalf("Server identity %s not found in config file", selfID)
	}

	// Initialize Raft server state (term, log, state, commitIndex, etc.).
	initServerState(selfID, peers)

	// Resolve own UDP address and start listening.
	addr, err := net.ResolveUDPAddr("udp", selfID)
	if err != nil {
		log.Fatalf("Failed to resolve address: %v", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on UDP: %v", err)
	}
	defer conn.Close()

	// Start a goroutine to handle incoming UDP messages (from peers and clients).
	go handleUDPMessages(conn)

	// Connect to other servers.
	connectToPeers(peers, selfID)

	// TODO: Start Raft protocol routines (e.g., leader election, heartbeat sender).
	go runRaftProtocol()

	// Command-line interface for debugging (log, print, resume, suspend, etc.)
	for {
		// TODO: Read from stdin and process debugging commands.
		time.Sleep(1 * time.Second)
	}
}

// loadServerConfig reads a configuration file and returns a slice of server addresses.
func loadServerConfig(filename string) ([]string, error) {
	// TODO: Open the file, read each line, trim spaces, and return the list.
	return []string{}, nil
}

// contains checks if a slice contains the specified string.
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if strings.TrimSpace(s) == strings.TrimSpace(item) {
			return true
		}
	}
	return false
}

// initServerState initializes server variables such as current term, log, state, commitIndex, etc.
func initServerState(selfID string, peers []string) {
	// TODO: Set initial values for term, log entries, server state (Follower, Candidate, Leader, Failed),
	// leader voted for, commitIndex, lastApplied, etc.
}

// handleUDPMessages continuously reads UDP messages and dispatches them for handling.
func handleUDPMessages(conn *net.UDPConn) {
	// TODO: In a loop, read from the connection, unmarshal the protobuf Raft messages,
	// and call appropriate handlers (e.g., handleAppendEntries, handleRequestVote).
}

// connectToPeers sends initial messages to other servers to establish connections.
func connectToPeers(peers []string, selfID string) {
	// TODO: Iterate over peers (skipping self) and send a handshake or initial message if needed.
}

// runRaftProtocol manages state transitions (Follower, Candidate, Leader, Failed) and implements core Raft logic.
func runRaftProtocol() {
	// TODO: Implement leader election, heartbeat sending, log replication, and state transitions.
}

// handleAppendEntries handles incoming AppendEntries RPC messages.
func handleAppendEntries(msg *miniraft.Raft) {
	// TODO: Process the AppendEntriesRequest according to the Raft protocol.
}

// handleRequestVote handles incoming RequestVote RPC messages.
func handleRequestVote(msg *miniraft.Raft) {
	// TODO: Process the RequestVoteRequest and update server vote state.
}

// processClientCommand processes client commands (e.g., insert, select, update).
func processClientCommand(command string) {
	// TODO: Validate and log the command. If not the leader, buffer or redirect the command.
}
