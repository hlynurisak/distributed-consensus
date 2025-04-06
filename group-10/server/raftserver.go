package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/mkyas/miniraft"
)

// ServerState holds minimal state for this server.
type ServerState struct {
	SelfID      string
	Peers       []string
	CurrentTerm uint64
	VotedFor    string
	CommitIndex uint64
	LastApplied uint64
	LogEntries  []miniraft.LogEntry
	State       string // "Follower", "Candidate", "Leader", "Failed"
}

var serverState ServerState

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

	log.Printf("Server %s is up and listening.\n", selfID)
	log.Printf("Configured peers: %v\n", peers)

	// Start a goroutine to handle incoming UDP messages (from peers and clients).
	go handleUDPMessages(conn)

	// Connect to other servers.
	connectToPeers(peers, selfID)

	// Start Raft protocol routines (e.g., leader election, heartbeat sender).
	go runRaftProtocol()

	// Command-line interface for debugging (log, print, resume, suspend, etc.)
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command (log, print, resume, suspend, exit): ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading input: %v", err)
			continue
		}
		command := strings.TrimSpace(input)
		switch command {
		case "log":
			// For demonstration, print the current log entries.
			fmt.Println("Log entries:")
			for _, entry := range serverState.LogEntries {
				fmt.Printf("Term: %d, Index: %d, Command: %s\n", entry.Term, entry.Index, entry.CommandName)
			}
		case "print":
			// Print server state.
			fmt.Printf("Current Term: %d\n", serverState.CurrentTerm)
			fmt.Printf("Voted For: %s\n", serverState.VotedFor)
			fmt.Printf("State: %s\n", serverState.State)
			fmt.Printf("Commit Index: %d\n", serverState.CommitIndex)
			fmt.Printf("Last Applied: %d\n", serverState.LastApplied)
		case "resume":
			// Stub for resume.
			fmt.Println("Resuming normal execution (stub).")
		case "suspend":
			// Stub for suspend.
			fmt.Println("Suspending execution (stub).")
		case "exit":
			fmt.Println("Exiting.")
			return
		default:
			fmt.Printf("Unknown command: %s\n", command)
		}
	}
}

// loadServerConfig reads a configuration file and returns a slice of server addresses.
func loadServerConfig(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var servers []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			servers = append(servers, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return servers, nil
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
	serverState = ServerState{
		SelfID:      selfID,
		Peers:       peers,
		CurrentTerm: 0,
		VotedFor:    "",
		CommitIndex: 0,
		LastApplied: 0,
		LogEntries:  []miniraft.LogEntry{},
		State:       "Follower",
	}
	log.Println("Server state initialized.")
}

// handleUDPMessages continuously reads UDP messages and dispatches them for handling.
func handleUDPMessages(conn *net.UDPConn) {
	buffer := make([]byte, 65536)
	for {
		n, addr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("Error reading from UDP: %v", err)
			continue
		}
		data := buffer[:n]
		log.Printf("Received %d bytes from %s: %s\n", n, addr.String(), strings.TrimSpace(string(data)))

		// TODO: Unmarshal the protobuf Raft message and handle it.
		// Example:
		// var raftMsg miniraft.Raft
		// if err := proto.Unmarshal(data, &raftMsg); err != nil {
		//     log.Printf("Failed to unmarshal message: %v", err)
		// } else {
		//     if raftMsg.AppendEntriesRequest != nil {
		//         handleAppendEntries(&raftMsg)
		//     } else if raftMsg.RequestVoteRequest != nil {
		//         handleRequestVote(&raftMsg)
		//     } else {
		//         log.Printf("Unknown message type")
		//     }
		// }
	}
}

// connectToPeers sends initial messages to other servers to establish connections.
func connectToPeers(peers []string, selfID string) {
	for _, peer := range peers {
		if strings.TrimSpace(peer) == strings.TrimSpace(selfID) {
			continue
		}
		// Resolve UDP address of the peer.
		peerAddr, err := net.ResolveUDPAddr("udp", peer)
		if err != nil {
			log.Printf("Failed to resolve address for peer %s: %v", peer, err)
			continue
		}
		// For Milestone 1, log the connection attempt.
		log.Printf("Connecting to peer %s at %s\n", peer, peerAddr.String())
		// Optionally, send a simple handshake message.
		conn, err := net.DialUDP("udp", nil, peerAddr)
		if err != nil {
			log.Printf("Failed to dial peer %s: %v", peer, err)
			continue
		}
		handshake := []byte("handshake from " + selfID)
		_, err = conn.Write(handshake)
		if err != nil {
			log.Printf("Failed to send handshake to %s: %v", peer, err)
		}
		conn.Close()
	}
}

// runRaftProtocol manages state transitions (Follower, Candidate, Leader, Failed)
// and implements core Raft logic. For Milestone 1, this is a stub.
func runRaftProtocol() {
	log.Println("Raft protocol routine started (stub).")
	for {
		// In a full implementation, you'd handle timeouts, elections, and heartbeats.
		time.Sleep(5 * time.Second)
		log.Println("Raft protocol running (stub).")
	}
}

// handleAppendEntries handles incoming AppendEntries RPC messages.
func handleAppendEntries(msg *miniraft.Raft) {
	// TODO: Process the AppendEntriesRequest according to the Raft protocol.
	log.Println("handleAppendEntries called (stub).")
}

// handleRequestVote handles incoming RequestVote RPC messages.
func handleRequestVote(msg *miniraft.Raft) {
	// TODO: Process the RequestVoteRequest and update server vote state.
	log.Println("handleRequestVote called (stub).")
}

// processClientCommand processes client commands (e.g., insert, select, update).
func processClientCommand(command string) {
	// TODO: Validate and log the command. If not the leader, buffer or redirect the command.
	log.Printf("Processing client command: %s (stub)\n", command)
}
