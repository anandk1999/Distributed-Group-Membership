package main

import (
	"context"
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

	// Create suspicion manager with options and callbacks
	opts := Options{
		SuspicionTimeout:   2 * time.Second,
		CheckInterval:      200 * time.Millisecond,
		RequireReports:     1,
		ConfirmedRetention: 30 * time.Second,
		OnSuspect: func(target NodeID, inc int32, reporters []NodeID) {
			// Update local membership to Suspected under lock
			membership.Lock()
			if m, ok := membership.Members[target.String()]; ok {
				m.Status = Suspected
				m.Incarnation = inc
				m.SuspicionStart = time.Now()
			}
			// collect recipients snapshot
			recips := make([]NodeID, 0, len(membership.Members))
			for _, mm := range membership.Members {
				if mm.ID.String() == membership.LocalNode.String() || mm.ID.String() == target.String() {
					continue
				}
				recips = append(recips, mm.ID)
			}
			membership.Unlock()

			// Build SUSPECT message and send outside any locks
			msg := Message{
				Type:        Suspect,
				Sender:      membership.LocalNode,
				Target:      target,
				Incarnation: inc,
			}
			for _, r := range recips {
				network.Send(msg, r.Address())
			}
			log.Printf("[OnSuspect] %s inc=%d reporters=%v", target, inc, reporters)
		},
		OnConfirm: func(target NodeID, inc int32) {
			// update local membership
			membership.Lock()
			if m, ok := membership.Members[target.String()]; ok {
				m.Status = Failed
				m.Incarnation = inc
			}
			// collect recipients snapshot
			recips := make([]NodeID, 0, len(membership.Members))
			for _, mm := range membership.Members {
				if mm.ID.String() == membership.LocalNode.String() || mm.ID.String() == target.String() {
					continue
				}
				recips = append(recips, mm.ID)
			}
			membership.Unlock()

			// Build CONFIRM message and broadcast
			msg := Message{
				Type:        Confirm,
				Sender:      membership.LocalNode,
				Target:      target,
				Incarnation: inc,
			}
			for _, r := range recips {
				network.Send(msg, r.Address())
			}
			log.Printf("[OnConfirm] %s inc=%d", target, inc)
		},
		OnClear: func(target NodeID, inc int32) {
			membership.Lock()
			if m, ok := membership.Members[target.String()]; ok {
				m.Status = Alive
				if inc >= m.Incarnation {
					m.Incarnation = inc
				}
				m.LastHeartbeat = time.Now()
				m.SuspicionStart = time.Time{}
			}
			// collect recipients snapshot
			recips := make([]NodeID, 0, len(membership.Members))
			for _, mm := range membership.Members {
				if mm.ID.String() == membership.LocalNode.String() || mm.ID.String() == target.String() {
					continue
				}
				recips = append(recips, mm.ID)
			}
			membership.Unlock()

			// Optionally broadcast an ALIVE message so others can clear suspicion quickly
			msg := Message{
				Type:        AliveMsg,
				Sender:      membership.LocalNode,
				Incarnation: inc,
			}
			for _, r := range recips {
				network.Send(msg, r.Address())
			}
			log.Printf("[OnClear] %s inc=%d", target, inc)
		},
	}

	// We'll set callbacks after creating the manager, but we need the manager instance first.
	suspicionMgr := NewSuspicionManager(membership, network, opts)

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

	// Start suspicion manager with a background context
	c.suspicionMgr.Start(context.Background())

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
	// Stop managers & network
	c.gossipManager.Stop()
	c.suspicionMgr.Stop()
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
		aliveCount := 0
		suspectedCount := 0
		failedCount := 0

		log.Printf("=== MEMBERSHIP STATUS ===")
		log.Printf("Total members: %d", len(members))

		for _, member := range members {
			timeSinceHeartbeat := time.Since(member.LastHeartbeat)
			statusInfo := ""

			switch member.Status {
			case Alive:
				aliveCount++
				if timeSinceHeartbeat > 5*time.Second {
					statusInfo = fmt.Sprintf(" (⚠️ stale: %v)", timeSinceHeartbeat)
				}
			case Suspected:
				suspectedCount++
				timeSinceSuspicion := time.Since(member.SuspicionStart)
				statusInfo = fmt.Sprintf(" (🔍 suspected for: %v)", timeSinceSuspicion)
			case Failed:
				failedCount++
				statusInfo = " (💀 failed)"
			}

			log.Printf("  %s | %s | Inc:%d | LastHB:%v ago%s",
				member.ID, member.Status, member.Incarnation,
				timeSinceHeartbeat.Truncate(time.Millisecond), statusInfo)
		}

		log.Printf("Summary: %d alive, %d suspected, %d failed", aliveCount, suspectedCount, failedCount)

		// Log recent updates being propagated
		recentUpdates := controller.membership.GetRecentUpdates(10)
		if len(recentUpdates) > 0 {
			log.Printf("Recent updates (piggybacking): %d", len(recentUpdates))
			for _, update := range recentUpdates {
				log.Printf("  📤 %s -> %s (Inc:%d)", update.NodeID, update.Status, update.Incarnation)
			}
		}
		log.Printf("========================")
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
		} else {
			log.Printf("Joined the group")
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
