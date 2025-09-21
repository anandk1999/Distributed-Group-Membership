package network

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func send(a string, message string, ctx context.Context) {
	results := make(chan string)

	go func() {
		var d net.Dialer
		conn, err := d.DialContext(ctx, "udp", a)
		if err != nil {
			return
		}
		defer conn.Close()

		_, err = conn.Write([]byte(message))
		if err != nil {
			//write something to conn
			return
		}
	}()

	return
}

// FIGURE OUT HOW TO HANDLE FOR GOSSIP VS. SWIM
func receive() <-chan "RESULT TYPE" {
	go func() {
		addr, err := net.ResolveUDPAddr("udp", ":8080")
		if err != nil {
			fmt.Printf("Error resolving UDP address: %v\n", err)
			return
		}

		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			fmt.Printf("Error listening on UDP: %v\n", err)
			return
		}
		defer conn.Close()

		fmt.Println("UDP server listening on :8080")

		buffer := make([]byte, 1024)
		for {
			n, remoteAddr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				fmt.Printf("Error reading from UDP: %v\n", err)
				continue // Continue listening for the next message
			}

			message := string(buffer[:n])
			fmt.Printf("Received %d bytes from %s: %s\n", n, remoteAddr, message)

			// Optionally, send a response back to the client
			response := []byte("ACK: " + message)
			_, err = conn.WriteToUDP(response, remoteAddr)
			if err != nil {
				fmt.Printf("Error writing to UDP: %v\n", err)
			}
		}
	}()

	return 
}

func main() {
	// Read hosts.txt and build address list (host:8080 on each line)
	file, err := os.Open("../hosts.txt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open hosts.txt: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	var addresses []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		host := strings.TrimSpace(scanner.Text())
		if host == "" {
			continue
		}
		addresses = append(addresses, host+":8080")
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to read hosts.txt: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	send(addresses[0], "", ctx)
	receive()
}
