package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/mkyas/miniraft"

	// We were unable to find a command in the miniraft package to marshal/unmarshal messages so we use the protobuf library
	"google.golang.org/protobuf/proto"
)

func main() {
	// Check command-line argument: server address to connect to
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run raftclient.go server-host:server-port")
		os.Exit(1)
	}
	serverAddr := os.Args[1]

	// Establish a UDP connection to the server
	conn, err := connectToServer(serverAddr)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	// Read commands from stdin and send to server
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Println("Error reading input:", err)
			continue
		}
		input = strings.TrimSpace(input)

		// Exit command terminates the client
		if input == "exit" {
			break
		}

		// Send the command to the server
		sendCommand(conn, input, serverAddr)
	}
}

// connectToServer resolves the server address and creates a UDP connection
func connectToServer(serverAddr string) (*net.UDPConn, error) {
	addr, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// sendCommand marshals the command into a Raft message and sends it over UDP
func sendCommand(conn *net.UDPConn, command string, serverAddr string) {
	// Create a Raft message with the CommandName field set
	msg := &miniraft.Raft{
		Message: &miniraft.Raft_CommandName{
			CommandName: command,
		},
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		log.Printf("Error marshaling command: %v", err)
		return
	}

	// Send the marshaled message as a UDP packet to the server
	_, err = conn.Write(data)
	if err != nil {
		log.Printf("Error sending command: %v", err)
	}
}
