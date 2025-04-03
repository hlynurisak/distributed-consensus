package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

func main() {
	// Check command-line argument: server address to connect to.
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run client.go server-host:server-port")
		os.Exit(1)
	}
	serverAddr := os.Args[1]

	// Establish a UDP connection to the server.
	conn, err := connectToServer(serverAddr)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	// Read commands from standard input and send them to the server.
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter command: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Println("Error reading input:", err)
			continue
		}
		input = strings.TrimSpace(input)

		// Exit command terminates the client.
		if input == "exit" {
			break
		}

		// TODO: Validate that the command contains only letters, digits, dashes, and underscores.

		// Send the command to the server.
		sendCommand(conn, input, serverAddr)
	}
}

// connectToServer resolves the server address and creates a UDP connection.
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

// sendCommand marshals the command into a Raft message and sends it over UDP.
func sendCommand(conn *net.UDPConn, command string, serverAddr string) {
	// TODO: Create a Raft message setting the CommandName field.
	// Use the miniraft package to marshal the message into bytes.
	// Send the UDP packet to the server.
}
