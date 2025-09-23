package main

import (
	"flag"
	"fmt"
	"log"
	. "mp2-g02/gossip"
	. "mp2-g02/membership"
	. "mp2-g02/network"
	. "mp2-g02/suspicion"
	. "mp2-g02/types"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Controller struct {
	mode          DetectionMode
	gossipManager *GossipManager
	membership    *MembershipList
	network       *NetworkLayer
	suspicionMgr  *SuspicionManager
}

func NewController(config Config) (*Controller, error) {
	// Create network layer
	network := NewNetworkLayer()

	// Create membership list
	membership := NewMembershipList(config.NodeID)

	// Create suspicion manager
	suspicionMgr := NewSuspicionManager(membership, network)

	// Create gossip manager
	gossipManager := NewGossipManager(membership, network, suspicionMgr)

	controller := &Controller{
		mode:          config.Mode,
		gossipManager: gossipManager,
		membership:    membership,
		network:       network,
		suspicionMgr:  suspicionMgr,
	}

	return controller, nil
}

func (c *Controller) Start() error {
	// Start network layer
	if err := c.network.Start(c.membership.LocalNode.Port); err != nil {
		return err
	}

	// Start based on mode
	switch c.mode {
	case GossipMode:
		c.gossipManager.Start()
	case PingAckMode:
		// TODO: Implement ping-ack mode
		log.Println("Ping-Ack mode not yet implemented")
	}

	log.Printf("Controller started in %v mode", c.mode)
	return nil
}

// JoinGroup joins the distributed group via introducer
func (c *Controller) JoinGroup(introducerAddr string) error {
	if c.mode == GossipMode {
		return c.gossipManager.JoinGroup(introducerAddr)
	}
	return nil
}

func (c *Controller) Stop() {
	c.gossipManager.Stop()
	c.network.Stop()
	log.Println("Controller stopped")
}

func (c *Controller) SetDropRate(rate float32) {
	c.network.SetDropRate(rate)
}

// StartCLI starts a command line interface for the controller
func StartCLI(controller *Controller) {
	// Enhanced CLI to provide basic functionality
	log.Println("CLI started - Controller is running")
	log.Printf("Node ID: %s", controller.membership.LocalNode)
	log.Printf("Listening on port: %d", controller.membership.LocalNode.Port)

	// Periodically log membership status
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		members := controller.membership.GetAllMembers()
		log.Printf("Current membership size: %d", len(members))
		for _, member := range members {
			log.Printf("  Member: %s, Status: %s, Incarnation: %d",
				member.ID, member.Status, member.Incarnation)
		}
	}
}

func (c *Controller) SwitchMode(mode DetectionMode) {
	c.mode = mode

	switch mode {
	case GossipMode:
		// c.pingAckManager.Stop()
		c.gossipManager.Start()
	case PingAckMode:
		c.gossipManager.Stop()
		// c.pingAckManager.Start()
	}
}

func main() {
	// Parse command line arguments
	var (
		port         = flag.Int("port", 8080, "UDP port to listen on")
		introducerIP = flag.String("introducer", "", "Introducer IP:Port")
		isIntroducer = flag.Bool("is-introducer", false, "Act as introducer")
		mode         = flag.String("mode", "gossip", "Detection mode: gossip or pingack")
	)
	flag.Parse()

	// Get local IP
	localIP := GetLocalIP()
	nodeID := NodeID{
		IP:        localIP,
		Port:      *port,
		Timestamp: time.Now().Unix(),
	}

	// Create controller
	config := Config{
		NodeID:         nodeID,
		IntroducerAddr: *introducerIP,
		IsIntroducer:   *isIntroducer,
		Mode:           ParseMode(*mode),
	}

	controller, err := NewController(config)
	if err != nil {
		log.Fatalf("Failed to create controller: %v", err)
	}

	// Start the system
	if err := controller.Start(); err != nil {
		log.Fatalf("Failed to start controller: %v", err)
	}

	// Join the group if not introducer
	if !*isIntroducer && *introducerIP != "" {
		if err := controller.JoinGroup(*introducerIP); err != nil {
			log.Printf("Failed to join group: %v", err)
		}
	}

	// Start CLI handler in a goroutine
	go StartCLI(controller)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down...")
	controller.Stop()
}
