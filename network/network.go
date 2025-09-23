package network

import (
	"encoding/json"
	"fmt"
	"log"
	. "mp2-g02/types"
	"net"
	"sync"
	"time"
)

// GetLocalIP returns the local IP address of the machine
func GetLocalIP() string {
	// conn, err := net.Dial("udp", "8.8.8.8:80")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer conn.Close()

	// localAddr := conn.LocalAddr().(*net.UDPAddr)
	// return localAddr.IP.String()
	return "fa25-cs425-0201.cs.illinois.edu"
}

type NetworkLayer struct {
	conn         *net.UDPConn
	dropRate     float32
	messageQueue chan ReceivedMessage
	handlers     map[MessageType]func(Message, *net.UDPAddr)
	closed       chan bool
	mutex        sync.RWMutex
}

type ReceivedMessage struct {
	Message Message
	From    *net.UDPAddr
}

func NewNetworkLayer() *NetworkLayer {
	return &NetworkLayer{
		dropRate:     0.0,
		messageQueue: make(chan ReceivedMessage, 1000),
		handlers:     make(map[MessageType]func(Message, *net.UDPAddr)),
		closed:       make(chan bool),
	}
}

func (n *NetworkLayer) Start(port int) error {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Printf("Error in trying to start in network.go: %v", err)
		return err
	}
	log.Printf("Starting UDP server on %s", addr.String())

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	n.conn = conn
	n.conn.SetReadBuffer(1048576) // 1MB buffer

	go n.receiveLoop()
	go n.processQueue()

	return nil
}

func (n *NetworkLayer) receiveLoop() {
	buffer := make([]byte, 65536)

	for {
		select {
		case <-n.closed:
			return
		default:
			n.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			bytesRead, addr, err := n.conn.ReadFromUDP(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				if err.Error() != "use of closed network connection" {
					log.Printf("Error reading UDP: %v", err)
				}
				continue
			}

			// Simulate message drop at receiver
			// `n.mutex.RLock()
			// dropRate := n.dropRate
			// n.mutex.RUnlock()

			// if rand.Float32() < dropRate {
			// 	continue
			// }`

			var msg Message
			if err := json.Unmarshal(buffer[:bytesRead], &msg); err != nil {
				log.Printf("Error unmarshaling message: %v", err)
				continue
			}

			select {
			case n.messageQueue <- ReceivedMessage{Message: msg, From: addr}:
			default:
				log.Println("Message queue full, dropping message")
			}
		}
	}
}

func (n *NetworkLayer) processQueue() {
	for {
		select {
		case <-n.closed:
			return
		case received := <-n.messageQueue:
			n.mutex.RLock()
			handler, exists := n.handlers[received.Message.Type]
			n.mutex.RUnlock()

			if exists {
				handler(received.Message, received.From)
			}
		}
	}
}

func (n *NetworkLayer) Send(msg Message, target string) error {
	msg.Timestamp = time.Now().Unix()
	fmt.Println(target)
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return err
	}

	_, err = n.conn.WriteToUDP(data, addr)
	if err == nil {
	}
	return err
}

func (n *NetworkLayer) RegisterHandler(msgType MessageType, handler func(Message, *net.UDPAddr)) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.handlers[msgType] = handler
}

func (n *NetworkLayer) SetDropRate(rate float32) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.dropRate = rate
}

func (n *NetworkLayer) Stop() {
	close(n.closed)
	if n.conn != nil {
		n.conn.Close()
	}
}
