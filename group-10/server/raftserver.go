package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mkyas/miniraft"
	"google.golang.org/protobuf/proto"
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

var (
	serverState       ServerState
	mu                sync.Mutex
	candidateVotes    int
	lastHeartbeat     time.Time   // Updated on valid heartbeat receipt.
	electionStartTime time.Time   // Set when a candidate starts an election.
)

func main() {
	// Expect two arguments: our own identity and the config file.
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run raftserver.go server-host:server-port filename")
		os.Exit(1)
	}
	selfID := os.Args[1]
	configFile := os.Args[2]

	// Load configuration.
	peers, err := loadServerConfig(configFile)
	if err != nil {
		log.Fatalf("Failed to load server config: %v", err)
	}
	if !contains(peers, selfID) {
		log.Fatalf("Server identity %s not found in config file", selfID)
	}

	// Initialize server state.
	initServerState(selfID, peers)

	// Start listening on the specified UDP address.
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

	// Start goroutines for message handling, initial handshake, and Raft protocol.
	go handleUDPMessages(conn)
	connectToPeers(peers, selfID)
	go runRaftProtocol()

	// Simple CLI for debugging.
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
			fmt.Println("Log entries:")
			mu.Lock()
			for _, entry := range serverState.LogEntries {
				fmt.Printf("Term: %d, Index: %d, Command: %s\n", entry.Term, entry.Index, entry.CommandName)
			}
			mu.Unlock()
		case "print":
			mu.Lock()
			fmt.Printf("Current Term: %d\n", serverState.CurrentTerm)
			fmt.Printf("Voted For: %s\n", serverState.VotedFor)
			fmt.Printf("State: %s\n", serverState.State)
			fmt.Printf("Commit Index: %d\n", serverState.CommitIndex)
			fmt.Printf("Last Applied: %d\n", serverState.LastApplied)
			mu.Unlock()
		case "resume":
			fmt.Println("Resuming normal execution (stub).")
		case "suspend":
			fmt.Println("Suspending execution (stub).")
		case "exit":
			fmt.Println("Exiting.")
			return
		default:
			fmt.Printf("Unknown command: %s\n", command)
		}
	}
}

// loadServerConfig reads the configuration file.
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

// contains checks if an item is in a slice.
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if strings.TrimSpace(s) == strings.TrimSpace(item) {
			return true
		}
	}
	return false
}

// initServerState initializes our global server state.
func initServerState(selfID string, peers []string) {
	mu.Lock()
	defer mu.Unlock()
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
	lastHeartbeat = time.Now()
	log.Println("Server state initialized.")
}

// handleUDPMessages listens for incoming UDP packets.
func handleUDPMessages(conn *net.UDPConn) {
	buffer := make([]byte, 65536)
	for {
		n, addr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("Error reading from UDP: %v", err)
			continue
		}
		data := buffer[:n]

		// Filter out plain-text handshake messages.
		if strings.HasPrefix(string(data), "handshake from") {
			log.Printf("Received handshake from %s: %s", addr.String(), strings.TrimSpace(string(data)))
			// Update heartbeat timer.
			mu.Lock()
			lastHeartbeat = time.Now()
			mu.Unlock()
			continue
		}

		var raftMsg miniraft.Raft
		err = proto.Unmarshal(data, &raftMsg)
		if err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			continue
		}

		switch {
		case raftMsg.GetRequestVoteRequest() != nil:
			go handleRequestVote(raftMsg.GetRequestVoteRequest(), addr)
		case raftMsg.GetRequestVoteResponse() != nil:
			go handleRequestVoteResponse(raftMsg.GetRequestVoteResponse())
		case raftMsg.GetAppendEntriesRequest() != nil:
			go handleAppendEntries(raftMsg.GetAppendEntriesRequest(), addr)
		case raftMsg.GetAppendEntriesResponse() != nil:
			// Optionally handle AppendEntries responses.
		case raftMsg.GetCommandName() != "":
			log.Printf("Received command: %s", raftMsg.GetCommandName())
		default:
			log.Printf("Unknown message type")
		}
	}
}

// connectToPeers sends an initial handshake to each peer.
func connectToPeers(peers []string, selfID string) {
	for _, peer := range peers {
		if strings.TrimSpace(peer) == strings.TrimSpace(selfID) {
			continue
		}
		addr, err := net.ResolveUDPAddr("udp", peer)
		if err != nil {
			log.Printf("Failed to resolve address for peer %s: %v", peer, err)
			continue
		}
		log.Printf("Connecting to peer %s at %s", peer, addr.String())
		conn, err := net.DialUDP("udp", nil, addr)
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

// runRaftProtocol handles election timeouts and heartbeat sending.
func runRaftProtocol() {
	rand.Seed(time.Now().UnixNano())
	// Use a randomized election timeout for followers (1500ms to 3000ms).
	electionTimeout := time.Duration(1500+rand.Intn(1500)) * time.Millisecond
	for {
		mu.Lock()
		state := serverState.State
		timeSinceHB := time.Since(lastHeartbeat)
		mu.Unlock()

		// Follower: If no heartbeat received within the timeout, start an election.
		if state == "Follower" && timeSinceHB >= electionTimeout {
			mu.Lock()
			startElection()
			electionStartTime = time.Now()
			lastHeartbeat = time.Now()
			mu.Unlock()
			// Choose a new randomized timeout.
			electionTimeout = time.Duration(1500+rand.Intn(1500)) * time.Millisecond
		} else if state == "Candidate" {
			// In Candidate mode, wait longer (2 seconds) for responses before restarting election.
			if time.Since(electionStartTime) >= 2*time.Second {
				mu.Lock()
				log.Printf("Candidate %s restarting election (term %d)", serverState.SelfID, serverState.CurrentTerm)
				startElection()
				electionStartTime = time.Now()
				mu.Unlock()
			}
			time.Sleep(50 * time.Millisecond)
		} else if state == "Leader" {
			sendHeartbeats()
			time.Sleep(100 * time.Millisecond)
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// startElection sets our state to Candidate, increments term, votes for self,
// and sends RequestVote RPCs to all peers.
func startElection() {
	serverState.State = "Candidate"
	serverState.CurrentTerm++
	serverState.VotedFor = serverState.SelfID
	candidateVotes = 1 // Vote for self.
	log.Printf("Starting election for term %d", serverState.CurrentTerm)
	for _, peer := range serverState.Peers {
		if strings.TrimSpace(peer) == serverState.SelfID {
			continue
		}
		go sendRequestVote(peer, serverState.CurrentTerm)
	}
}

// sendRequestVote sends a RequestVote RPC to a given peer.
func sendRequestVote(peer string, term uint64) {
	addr, err := net.ResolveUDPAddr("udp", peer)
	if err != nil {
		log.Printf("Failed to resolve peer address %s: %v", peer, err)
		return
	}
	req := &miniraft.Raft{
		Message: &miniraft.Raft_RequestVoteRequest{
			RequestVoteRequest: &miniraft.RequestVoteRequest{
				Term:          term,
				LastLogIndex:  0, // Simplified: no log entries yet.
				LastLogTerm:   0,
				CandidateName: serverState.SelfID,
			},
		},
	}
	data, err := proto.Marshal(req)
	if err != nil {
		log.Printf("Error marshaling RequestVote: %v", err)
		return
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Printf("Failed to dial peer %s: %v", peer, err)
		return
	}
	defer conn.Close()
	_, err = conn.Write(data)
	if err != nil {
		log.Printf("Error sending RequestVote to %s: %v", peer, err)
	}
}

// handleRequestVote processes an incoming RequestVote RPC.
func handleRequestVote(req *miniraft.RequestVoteRequest, addr *net.UDPAddr) {
	mu.Lock()
	defer mu.Unlock()

	// Reject if candidate's term is less than our term.
	if req.Term < serverState.CurrentTerm {
		sendRequestVoteResponse(addr, false)
		return
	}

	// If candidate's term is higher, update our term and revert to follower.
	if req.Term > serverState.CurrentTerm {
		serverState.CurrentTerm = req.Term
		serverState.State = "Follower"
		serverState.VotedFor = ""
	}

	// Grant vote if we haven't voted yet or have already voted for this candidate.
	if serverState.VotedFor == "" || serverState.VotedFor == req.CandidateName {
		serverState.VotedFor = req.CandidateName
		sendRequestVoteResponse(addr, true)
	} else {
		sendRequestVoteResponse(addr, false)
	}
}

// sendRequestVoteResponse sends a response for a RequestVote RPC.
func sendRequestVoteResponse(addr *net.UDPAddr, voteGranted bool) {
	resp := &miniraft.Raft{
		Message: &miniraft.Raft_RequestVoteResponse{
			RequestVoteResponse: &miniraft.RequestVoteResponse{
				Term:        serverState.CurrentTerm,
				VoteGranted: voteGranted,
			},
		},
	}
	data, err := proto.Marshal(resp)
	if err != nil {
		log.Printf("Error marshaling RequestVoteResponse: %v", err)
		return
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Printf("Error dialing for RequestVoteResponse: %v", err)
		return
	}
	defer conn.Close()
	_, err = conn.Write(data)
	if err != nil {
		log.Printf("Error sending RequestVoteResponse: %v", err)
	}
}

// handleRequestVoteResponse processes a vote response.
func handleRequestVoteResponse(resp *miniraft.RequestVoteResponse) {
	mu.Lock()
	defer mu.Unlock()

	// If the response's term is greater, update our term and revert to follower.
	if resp.Term > serverState.CurrentTerm {
		serverState.CurrentTerm = resp.Term
		serverState.State = "Follower"
		serverState.VotedFor = ""
		return
	}
	// If we're still a candidate in the current term, count the vote.
	if serverState.State == "Candidate" && resp.Term == serverState.CurrentTerm {
		if resp.VoteGranted {
			candidateVotes++
			log.Printf("Candidate %s received vote, total votes: %d", serverState.SelfID, candidateVotes)
			// With three nodes, majority is 2 votes.
			if candidateVotes > len(serverState.Peers)/2 {
				serverState.State = "Leader"
				log.Printf("Server %s elected as Leader for term %d", serverState.SelfID, serverState.CurrentTerm)
			}
		}
	}
}

// sendHeartbeats sends an AppendEntries (heartbeat) RPC to all peers.
func sendHeartbeats() {
	mu.Lock()
	term := serverState.CurrentTerm
	leaderID := serverState.SelfID
	commitIndex := serverState.CommitIndex
	mu.Unlock()

	for _, peer := range serverState.Peers {
		if strings.TrimSpace(peer) == leaderID {
			continue
		}
		go func(peer string) {
			addr, err := net.ResolveUDPAddr("udp", peer)
			if err != nil {
				log.Printf("Failed to resolve peer address %s: %v", peer, err)
				return
			}
			req := &miniraft.Raft{
				Message: &miniraft.Raft_AppendEntriesRequest{
					AppendEntriesRequest: &miniraft.AppendEntriesRequest{
						Term:         term,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						LeaderCommit: commitIndex,
						LeaderId:     leaderID,
						Entries:      []*miniraft.LogEntry{}, // empty heartbeat
					},
				},
			}
			data, err := proto.Marshal(req)
			if err != nil {
				log.Printf("Error marshaling heartbeat: %v", err)
				return
			}
			conn, err := net.DialUDP("udp", nil, addr)
			if err != nil {
				log.Printf("Failed to dial peer %s: %v", peer, err)
				return
			}
			defer conn.Close()
			_, err = conn.Write(data)
			if err != nil {
				log.Printf("Error sending heartbeat to %s: %v", peer, err)
			}
		}(peer)
	}
}

// handleAppendEntries processes an AppendEntries (heartbeat) RPC.
func handleAppendEntries(req *miniraft.AppendEntriesRequest, addr *net.UDPAddr) {
	mu.Lock()
	defer mu.Unlock()

	// Ignore if the leader's term is less than our term.
	if req.Term < serverState.CurrentTerm {
		return
	}
	// If leader's term is higher, update our term and revert to follower.
	if req.Term > serverState.CurrentTerm {
		serverState.CurrentTerm = req.Term
		serverState.State = "Follower"
		serverState.VotedFor = ""
	}
	// Reset our heartbeat timer.
	lastHeartbeat = time.Now()
	log.Printf("Received heartbeat from leader %s", req.LeaderId)
}
