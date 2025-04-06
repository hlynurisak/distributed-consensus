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
	State       string // Can be "Follower", "Candidate", "Leader", "Failed"
	
	// To track peers
	NextIndex   map[string]uint64
	MatchIndex  map[string]uint64
}

// Global variables
var (
	serverState       ServerState
	mu                sync.Mutex
	candidateVotes    int
	lastHeartbeat     time.Time      // Updated on valid heartbeat receipt.
	electionStartTime time.Time      // Set when a candidate starts an election.
)

// Constants for Raft protocol timing
const (
	// Base election timeout between 150-300ms as per Raft paper recommendation (section 5.1)
	minElectionTimeout = 150 * time.Millisecond
	maxElectionTimeout = 300 * time.Millisecond
	
	// Heartbeat interval (should be less than minElectionTimeout)
	heartbeatInterval = 50 * time.Millisecond
)

func main() {
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())
	
	// Expect two arguments: ip:port and config file.
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run raftserver.go server-host:server-port filename")
		os.Exit(1)
	}
	selfID := os.Args[1]
	configFile := os.Args[2]

	// Load server configuration.
	peers, err := loadServerConfig(configFile)
	if err != nil {
		log.Fatalf("Failed to load server config: %v", err)
	}
	if !contains(peers, selfID) {
		log.Fatalf("Server identity %s not found in config file", selfID)
	}

	// Initialize server state.
	initServerState(selfID, peers)

	// Start listening on the specified address.
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

	// Simple CLI
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
			for i := range serverState.LogEntries {
				entry := &serverState.LogEntries[i]
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
			fmt.Printf("Time since last heartbeat: %v\n", time.Since(lastHeartbeat))
			mu.Unlock()
		case "resume":
			mu.Lock()
			if serverState.State == "Failed" {
				serverState.State = "Follower"
				fmt.Println("Resuming as follower.")
			} else {
				fmt.Println("Server is already running.")
			}
			mu.Unlock()
		case "suspend":
			mu.Lock()
			serverState.State = "Failed"
			fmt.Println("Server suspended. Enter 'resume' to continue.")
			mu.Unlock()
		case "exit":
			fmt.Println("Exiting.")
			return
		default:
			fmt.Printf("Unknown command: %s\n", command)
		}
	}
}

// Parse and load the config file.
// https://stackoverflow.com/questions/8757389/reading-a-file-line-by-line-in-go
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

// Check if an item is in a slice.
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if strings.TrimSpace(s) == strings.TrimSpace(item) {
			return true
		}
	}
	return false
}

// Initialize the global server state.
func initServerState(selfID string, peers []string) {
	mu.Lock()
	defer mu.Unlock()
	
	// Initialize next and match indices for leader state
	nextIndex := make(map[string]uint64)
	// The highest index known to be commited
	matchIndex := make(map[string]uint64)
	
	for _, peer := range peers {
		if peer != selfID {
			nextIndex[peer] = 1  // Raft indexing starts at 1
			matchIndex[peer] = 0
		}
	}
	
	serverState = ServerState{
		SelfID:      selfID,
		Peers:       peers,
		CurrentTerm: 0,
		VotedFor:    "",
		CommitIndex: 0,
		LastApplied: 0,
		LogEntries:  []miniraft.LogEntry{},
		State:       "Follower",
		NextIndex:   nextIndex,
		MatchIndex:  matchIndex,
	}
	lastHeartbeat = time.Now()
	log.Println("Server state initialized.")
}

// Listen for incoming UDP packets.
func handleUDPMessages(conn *net.UDPConn) {
	// 64kB buffer
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
			go handleAppendEntriesResponse(raftMsg.GetAppendEntriesResponse(), addr)
		case raftMsg.GetCommandName() != "":
			go handleClientCommand(raftMsg.GetCommandName())
		default:
			log.Printf("Unknown message type")
		}
	}
}

// Send a handshake to each peer in the network.
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

// The main loop. Handles election timeouts and sending out the heartbeats.
func runRaftProtocol() {
	// Get initial randomized election timeout
	electionTimeout := getRandomElectionTimeout()
	
	for {
		mu.Lock()
		state := serverState.State
		timeSinceLast := time.Since(lastHeartbeat)
		mu.Unlock()

		switch state {
		case "Follower":
			// If no heartbeat received within the timeout, start an election.
			if timeSinceLast >= electionTimeout {
				mu.Lock()
				startElection()
				electionStartTime = time.Now()
				mu.Unlock()
				
			}
			// Get new random timeout for next cycle and sleep a little
			electionTimeout = getRandomElectionTimeout()
			time.Sleep(10 * time.Millisecond)
			
		case "Candidate":
			// Candidate: Timeout if election takes too long
			if time.Since(electionStartTime) >= electionTimeout {
				mu.Lock()
				startElection()
				electionStartTime = time.Now()
				mu.Unlock()
				
			}
			// Choose a new randomized timeout for next cycle
			electionTimeout = getRandomElectionTimeout()
			time.Sleep(10 * time.Millisecond)
			
		case "Leader":
			// Leader: Send heartbeats periodically
			sendHeartbeats()
			time.Sleep(heartbeatInterval)
			
		case "Failed":
			// Do nothing if server is in failed state
			time.Sleep(100 * time.Millisecond)
			
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// Get a random election timeout within the specified min and max timeouts.
func getRandomElectionTimeout() time.Duration {
	return minElectionTimeout + time.Duration(rand.Int63n(int64(maxElectionTimeout-minElectionTimeout)))
}

// startElection sets state to Candidate, increments term, votes for self 
// and ask the others to vote for me aswell (RequestVote RPC).
func startElection() {
	serverState.State = "Candidate"
	serverState.CurrentTerm++
	serverState.VotedFor = serverState.SelfID
	candidateVotes = 1 // 1 vote from self
	lastHeartbeat = time.Now() // Reset heartbeat timer
	electionStartTime = time.Now() // Reset election start time
	log.Printf("Starting election for term %d", serverState.CurrentTerm)
	
	// Send RequestVote to all peers
	candidateID := serverState.SelfID
	
	for _, peer := range serverState.Peers {
		if strings.TrimSpace(peer) == serverState.SelfID {
			continue
		}
		go sendRequestVote(peer, candidateID)
	}
}

// Send a RequestVote RPC to a peer.
func sendRequestVote(peer string, candidateID string) {
	addr, err := net.ResolveUDPAddr("udp", peer)
	if err != nil {
		log.Printf("Failed to resolve peer address %s: %v", peer, err)
		return
	}

	// Get last log info
	var lastLogIndex uint64 = 0
	var lastLogTerm uint64 = 0
	if len(serverState.LogEntries) > 0 {
		lastEntry := &serverState.LogEntries[len(serverState.LogEntries)-1]
		lastLogIndex = lastEntry.Index
		lastLogTerm = lastEntry.Term
	}
	term := serverState.CurrentTerm

	req := &miniraft.Raft{
		Message: &miniraft.Raft_RequestVoteRequest{
			RequestVoteRequest: &miniraft.RequestVoteRequest{
				Term:          term,
				LastLogIndex:  lastLogIndex,
				LastLogTerm:   lastLogTerm,
				CandidateName: candidateID,
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

// Process incoming RequestVote RPC.
func handleRequestVote(req *miniraft.RequestVoteRequest, addr *net.UDPAddr) {
	mu.Lock()
	defer mu.Unlock()

	voteGranted := false
	
	// Step down if the term is higher
	if req.Term > serverState.CurrentTerm {
		serverState.CurrentTerm = req.Term
		serverState.State = "Follower"
		serverState.VotedFor = ""
	}

	// Reject if candidate's term is less than self
	if req.Term < serverState.CurrentTerm {
		sendRequestVoteResponse(addr, false, serverState.CurrentTerm)
		return
	}
	
	if serverState.VotedFor == "" || serverState.VotedFor == req.CandidateName {
		// Check if candidate's log is at least as up-to-date as self
		lastLogIndex := uint64(0)
		lastLogTerm := uint64(0)
		
		if len(serverState.LogEntries) > 0 {
			lastEntry := &serverState.LogEntries[len(serverState.LogEntries)-1]
			lastLogIndex = lastEntry.Index
			lastLogTerm = lastEntry.Term
		}
		
		// Grant vote if candidate's log is at least as up-to-date as ours
		// The Raft paper section 5.4.1
		if req.LastLogTerm > lastLogTerm || 
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
			serverState.VotedFor = req.CandidateName
			voteGranted = true
			lastHeartbeat = time.Now() // Reset election timer if vote is given	
		}
	}
	
	sendRequestVoteResponse(addr, voteGranted, serverState.CurrentTerm)
}

// Respond to RequestVote RPC with a vote or not.
func sendRequestVoteResponse(addr *net.UDPAddr, voteGranted bool, term uint64) {
	resp := &miniraft.Raft{
		Message: &miniraft.Raft_RequestVoteResponse{
			RequestVoteResponse: &miniraft.RequestVoteResponse{
				Term:        term,
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

// Process vote response
func handleRequestVoteResponse(resp *miniraft.RequestVoteResponse) {
	mu.Lock()
	defer mu.Unlock()

	if serverState.State != "Candidate" || resp.Term != serverState.CurrentTerm {
		return
	}

	// If term in response is greater then update term and set state to follower
	if resp.Term > serverState.CurrentTerm {
		serverState.CurrentTerm = resp.Term
		serverState.State = "Follower"
		serverState.VotedFor = ""
		return
	}
	
	// Count votes
	if resp.VoteGranted {
		candidateVotes++
		log.Printf("Candidate %s received vote, total votes: %d/%d", 
			serverState.SelfID, candidateVotes, len(serverState.Peers))
		
		// Check if more than half voted for self
		if candidateVotes > len(serverState.Peers)/2 {
			log.Printf("Server %s elected as Leader for term %d", 
				serverState.SelfID, serverState.CurrentTerm)
			
			// Set state to leader
			serverState.State = "Leader"
			
			// Reset the indexes
			for peer := range serverState.NextIndex {
				lastLogIndex := uint64(1)
				if len(serverState.LogEntries) > 0 {
					lastLogIndex = serverState.LogEntries[len(serverState.LogEntries)-1].Index + 1
				}
				serverState.NextIndex[peer] = lastLogIndex
				serverState.MatchIndex[peer] = 0
			}
			
			// Start sending heartbeats
			go sendHeartbeats()
		}
	}
}

// sendHeartbeats sends an AppendEntries (heartbeat) RPC to all peers.
func sendHeartbeats() {
	mu.Lock()
	// Double check leader status
	if serverState.State != "Leader" {
		mu.Unlock()
		return
	}
	
	term := serverState.CurrentTerm
	leaderID := serverState.SelfID
	commitIndex := serverState.CommitIndex
	mu.Unlock()

	for _, peer := range serverState.Peers {
		if strings.TrimSpace(peer) == leaderID {
			continue
		}
		go sendAppendEntries(peer, term, leaderID, commitIndex)
	}
}

// Send AppendEntries RPC to a peer
func sendAppendEntries(peer string, term uint64, leaderID string, commitIndex uint64) {
	mu.Lock()
	// Find what is the next log index for this peer
	nextIdx := serverState.NextIndex[peer]
	prevLogIndex := nextIdx - 1
	prevLogTerm := uint64(0)
	
	// Find the term of the previous log entry
	if prevLogIndex > 0 && int(prevLogIndex-1) < len(serverState.LogEntries) {
		prevLogTerm = serverState.LogEntries[prevLogIndex-1].Term
	}
	
	// Get entries to send (empty for heartbeat)
	var entries []*miniraft.LogEntry
	// For actual log replication (not just heartbeat)
	if nextIdx <= uint64(len(serverState.LogEntries)) {
		// Send log entries starting at nextIndex (The Raft paper section 5.3)
		for i := nextIdx - 1; i < uint64(len(serverState.LogEntries)); i++ {
			entry := &serverState.LogEntries[i]
			entries = append(entries, &miniraft.LogEntry{
				Index:       entry.Index,
				Term:        entry.Term,
				CommandName: entry.CommandName,
			})
		}
	}
	mu.Unlock()
	
	// Create the AppendEntries request
	req := &miniraft.Raft{
		Message: &miniraft.Raft_AppendEntriesRequest{
			AppendEntriesRequest: &miniraft.AppendEntriesRequest{
				Term:         term,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: commitIndex,
				LeaderId:     leaderID,
				Entries:      entries,
			},
		},
	}
	
	// Send the request
	addr, err := net.ResolveUDPAddr("udp", peer)
	if err != nil {
		log.Printf("Failed to resolve peer address %s: %v", peer, err)
		return
	}
	
	data, err := proto.Marshal(req)
	if err != nil {
		log.Printf("Error marshaling AppendEntries: %v", err)
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
		log.Printf("Error sending AppendEntries to %s: %v", peer, err)
	}
}

// Process an AppendEntries RPC.
func handleAppendEntries(req *miniraft.AppendEntriesRequest, addr *net.UDPAddr) {
	mu.Lock()
	
	// Create response (default to failure)
	success := false
	responseTerm := serverState.CurrentTerm
	
	// Reject if leaders term is less than self
	if req.Term < serverState.CurrentTerm {
		mu.Unlock()
		sendAppendEntriesResponse(addr, false, serverState.CurrentTerm)
		return
	}
	
	// If term is greater or equal, update state to follower
	if req.Term >= serverState.CurrentTerm {
		if req.Term > serverState.CurrentTerm || serverState.State != "Follower" {
			serverState.CurrentTerm = req.Term
			serverState.State = "Follower"
			serverState.VotedFor = ""
		}
		lastHeartbeat = time.Now()
	}
	
	// Check if the leader is up-to-date
	if len(req.Entries) > 0 {
		logOK := false
		
		if req.PrevLogIndex == 0 {
			logOK = true
		} else if int(req.PrevLogIndex-1) < len(serverState.LogEntries) {
			// Check if terms match at prevLogIndex
			if serverState.LogEntries[req.PrevLogIndex-1].Term == req.PrevLogTerm {
				logOK = true
			}
		}
		
		if !logOK {
			// Log inconsistency, reject
			// The Raft paper section 5.3
			mu.Unlock()
			sendAppendEntriesResponse(addr, false, serverState.CurrentTerm)
			return
		}
		
		// Process log entries
		for i, entry := range req.Entries {
			entryIndex := req.PrevLogIndex + uint64(i) + 1
			
			// If existing entry conflicts with new one (same index, different terms)
			if int(entryIndex-1) < len(serverState.LogEntries) {
				if serverState.LogEntries[entryIndex-1].Term != entry.Term {
					// Delete it and all following entries
					serverState.LogEntries = serverState.LogEntries[:entryIndex-1]
				} else {
					// Skip entry if already present with same term
					continue
				}
			}
			
			// Append new entry if we reach here
			if int(entryIndex-1) >= len(serverState.LogEntries) {
				serverState.LogEntries = append(serverState.LogEntries, miniraft.LogEntry{
					Index:       entry.Index,
					Term:        entry.Term,
					CommandName: entry.CommandName,
				})
			}
		}
		
		// Success for log entries
		success = true
	} else {
		// Empty AppendEntries is just a heartbeat
		success = true
	}
	
	// Update commit index if leaders is higher
	if req.LeaderCommit > serverState.CommitIndex {
		lastLogIndex := uint64(0)
		if len(serverState.LogEntries) > 0 {
			lastLogIndex = serverState.LogEntries[len(serverState.LogEntries)-1].Index
		}
		
		// Set commit index
		if req.LeaderCommit < lastLogIndex {
			serverState.CommitIndex = req.LeaderCommit
		} else {
			serverState.CommitIndex = lastLogIndex
		}
		
		// Apply committed entries
		applyCommittedEntries()
	}
	
	mu.Unlock()
	
	// Send response
	sendAppendEntriesResponse(addr, success, responseTerm)
}

// Response to AppendEntries
func sendAppendEntriesResponse(addr *net.UDPAddr, success bool, term uint64) {
	resp := &miniraft.Raft{
		Message: &miniraft.Raft_AppendEntriesResponse{
			AppendEntriesResponse: &miniraft.AppendEntriesResponse{
				Term:    term,
				Success: success,
			},
		},
	}
	
	data, err := proto.Marshal(resp)
	if err != nil {
		log.Printf("Error marshaling AppendEntriesResponse: %v", err)
		return
	}
	
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Printf("Error dialing for AppendEntriesResponse: %v", err)
		return
	}
	defer conn.Close()
	
	_, err = conn.Write(data)
	if err != nil {
		log.Printf("Error sending AppendEntriesResponse: %v", err)
	}
}

// Process responses to AppendEntries RPC
func handleAppendEntriesResponse(resp *miniraft.AppendEntriesResponse, addr *net.UDPAddr) {
	mu.Lock()
	defer mu.Unlock()
	
	// Ignore if not a leader anymore
	if serverState.State != "Leader" {
		return
	}
	
	// If term in response is greater than self - update term and set state to follower
	if resp.Term > serverState.CurrentTerm {
		serverState.CurrentTerm = resp.Term
		serverState.State = "Follower"
		serverState.VotedFor = ""
		return
	}
	
	// Find the peer that responded
	peerAddr := addr.String()
	var peerID string
	for _, peer := range serverState.Peers {
		if strings.HasPrefix(peer, peerAddr) || strings.HasSuffix(peer, peerAddr) {
			peerID = peer
			break
		}
	}
	
	if peerID == "" {
		log.Printf("AppendEntries response: Could not identify peer from %s", addr.String())
		return
	}
	
	// If append was successful, update nextIndex and matchIndex
	if resp.Success {
		// Determine the index of the last log entry sent to this peer
		nextIndex := serverState.NextIndex[peerID]
		lastSentIndex := nextIndex - 1
		for i := lastSentIndex + 1; i <= uint64(len(serverState.LogEntries)); i++ {
			if int(i-1) < len(serverState.LogEntries) {
				lastSentIndex = i
			}
		}
		
		// Update indexes for peer
		if lastSentIndex >= serverState.NextIndex[peerID] {
			serverState.NextIndex[peerID] = lastSentIndex + 1
			serverState.MatchIndex[peerID] = lastSentIndex
			
			// Commit new entries
			updateCommitIndex()
		}
	} else {
		// If failed due to log inconsistency, decrement nextIndex and retry
		if serverState.NextIndex[peerID] > 1 {
			serverState.NextIndex[peerID]--
			// Will be retried on next heartbeat automatically
		}
	}
}

// Check if there are new entries that can be committed
func updateCommitIndex() {
	// Leader only commits entries from its current term
	// Section 5.4.2 in Raft paper
	for N := serverState.CommitIndex + 1; N <= uint64(len(serverState.LogEntries)); N++ {
		// Check if this is from current term
		if int(N-1) < len(serverState.LogEntries) && 
		   serverState.LogEntries[N-1].Term == serverState.CurrentTerm {
			
			// Count replications
			replicationCount := 1 // Count self
			for peer, matchIdx := range serverState.MatchIndex {
				if peer != serverState.SelfID && matchIdx >= N {
					replicationCount++
				}
			}
			
			// If majority, commit entry
			if replicationCount > len(serverState.Peers)/2 {
				serverState.CommitIndex = N
			} else {
				break
			}
		}
	}
	
	// Apply committed entries
	applyCommittedEntries()
}

// Apply commited entries to state
func applyCommittedEntries() {
	// Apply all entries between lastApplied and commitIndex
	for i := serverState.LastApplied + 1; i <= serverState.CommitIndex; i++ {
		if int(i-1) < len(serverState.LogEntries) {
			entry := &serverState.LogEntries[i-1]
			
			log.Printf("Applying command: %s (index %d, term %d)", 
				entry.CommandName, entry.Index, entry.Term)
			
			// Update lastApplied
			serverState.LastApplied = i
		}
	}
}

// Process client commands
func handleClientCommand(command string) {
	mu.Lock()
	defer mu.Unlock()
	
	log.Printf("Received client command: %s", command)
	
	// Create a new log entry
	newIndex := uint64(1)
	if len(serverState.LogEntries) > 0 {
		newIndex = serverState.LogEntries[len(serverState.LogEntries)-1].Index + 1
	}
	
	newEntry := miniraft.LogEntry{
		Index:       newIndex,
		Term:        serverState.CurrentTerm,
		CommandName: command,
	}
	
	// Append to leader's log
	serverState.LogEntries = append(serverState.LogEntries, newEntry)
	log.Printf("Leader %s: Added new command to log at index %d, term %d", 
		serverState.SelfID, newIndex, serverState.CurrentTerm)
		
	// The entry will be replicated to followers in the next AppendEntries cycle
	// Force an immediate AppendEntries to replicate faster
	go sendHeartbeats()
}
