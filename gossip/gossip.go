package gossip

import (
	"log"
	. "mp2-g02/membership"
	. "mp2-g02/network"
	. "mp2-g02/suspicion"
	. "mp2-g02/types"
	"net"
	"time"
)

type GossipManager struct {
	membership      *MembershipList
	network         *NetworkLayer
	heartbeatRate   time.Duration
	fanout          int
	suspicionTime   time.Duration
	failureTime     time.Duration
	stopped         chan bool
	suspicionMgr    *SuspicionManager
	enableSuspicion bool
}

func NewGossipManager(ml *MembershipList, net *NetworkLayer, suspicionMgr *SuspicionManager) *GossipManager {
	return &GossipManager{
		membership:      ml,
		network:         net,
		heartbeatRate:   250 * time.Millisecond,
		fanout:          3,
		suspicionTime:   2 * time.Second,
		failureTime:     1 * time.Second,
		stopped:         make(chan bool),
		suspicionMgr:    suspicionMgr,
		enableSuspicion: true,
	}
}

func (g *GossipManager) Start() {
	// Register handlers
	g.network.RegisterHandler(Heartbeat, g.handleHeartbeat)
	g.network.RegisterHandler(Join, g.handleJoin)
	g.network.RegisterHandler(JoinResponse, g.handleJoinResponse)
	g.network.RegisterHandler(AliveMsg, g.handleAliveMessage)
	g.network.RegisterHandler(Suspect, g.handleSuspectMessage)

	go g.heartbeatLoop()
	go g.failureDetectionLoop()

	log.Println("Gossip manager started")
}

func (g *GossipManager) Stop() {
	close(g.stopped)
}

func (g *GossipManager) SetSuspicion(enable bool) {
	g.enableSuspicion = enable
}

func (g *GossipManager) heartbeatLoop() {
	ticker := time.NewTicker(g.heartbeatRate)
	defer ticker.Stop()

	for {
		select {
		case <-g.stopped:
			return
		case <-ticker.C:
			targets := g.membership.GetRandomMembers(g.fanout, nil)

			// Get recent updates to piggyback (max 5)
			updates := g.membership.GetRecentUpdates(5)

			msg := Message{
				Type:        Heartbeat,
				Sender:      g.membership.LocalNode,
				Incarnation: g.membership.Incarnation,
				Members:     updates,
			}

			for _, target := range targets {
				if err := g.network.Send(msg, target.ID.Address()); err != nil {
					log.Printf("Failed to send heartbeat to %s: %v", target.ID, err)
				}
			}
		}
	}
}

func (g *GossipManager) handleHeartbeat(msg Message, from *net.UDPAddr) {
	g.membership.Lock()
	defer g.membership.Unlock()

	// Update sender's heartbeat
	senderKey := msg.Sender.String()
	sender, exists := g.membership.Members[senderKey]

	if !exists {
		// New member discovered
		newMember := &Member{
			ID:            msg.Sender,
			Incarnation:   msg.Incarnation,
			Status:        Alive,
			LastHeartbeat: time.Now(),
		}
		g.membership.Members[senderKey] = newMember
	} else {
		// Update existing member
		if msg.Incarnation > sender.Incarnation {
			sender.Incarnation = msg.Incarnation
			sender.Status = Alive
			sender.LastHeartbeat = time.Now()
			sender.SuspicionStart = time.Time{}
		} else if msg.Incarnation == sender.Incarnation {
			sender.LastHeartbeat = time.Now()
		}
	}

	// Process piggybacked updates
	for _, update := range msg.Members {
		g.processUpdate(update)
	}
}

func (g *GossipManager) processUpdate(update MemberUpdate) {
	memberKey := update.NodeID.String()

	// Don't process updates about self
	if memberKey == g.membership.LocalNode.String() {
		if update.Status == Suspected && g.enableSuspicion {
			// Refute suspicion about self
			g.suspicionMgr.ProcessSuspicion(update.NodeID, update.Incarnation)
		}
		return
	}

	member, exists := g.membership.Members[memberKey]

	if !exists && update.Status == Alive {
		// New member
		newMember := &Member{
			ID:            update.NodeID,
			Incarnation:   update.Incarnation,
			Status:        update.Status,
			LastHeartbeat: time.Now(),
		}
		g.membership.Members[memberKey] = newMember
	} else if exists {
		// Update existing member based on incarnation
		if update.Incarnation > member.Incarnation {
			member.Incarnation = update.Incarnation
			member.Status = update.Status

			if update.Status == Alive {
				member.LastHeartbeat = time.Now()
				member.SuspicionStart = time.Time{}
			} else if update.Status == Suspected && g.enableSuspicion {
				member.SuspicionStart = time.Now()
			}
		}
	}
}

func (g *GossipManager) failureDetectionLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-g.stopped:
			return
		case <-ticker.C:
			g.checkFailures()
		}
	}
}

func (g *GossipManager) checkFailures() {
	g.membership.Lock()
	defer g.membership.Unlock()

	now := time.Now()
	toRemove := []string{}

	for id, member := range g.membership.Members {
		// Skip self
		if id == g.membership.LocalNode.String() {
			continue
		}

		switch member.Status {
		case Alive:
			if now.Sub(member.LastHeartbeat) > g.suspicionTime {
				if g.enableSuspicion {
					member.Status = Suspected
					member.SuspicionStart = now
					// Broadcast suspicion
					g.suspicionMgr.BroadcastSuspicion(member)
				} else {
					// Direct failure without suspicion
					toRemove = append(toRemove, id)
				}
			}
		case Suspected:
			if now.Sub(member.SuspicionStart) > g.failureTime {
				toRemove = append(toRemove, id)
			}
		}
	}

	// Remove failed members
	for _, id := range toRemove {
		member := g.membership.Members[id]
		delete(g.membership.Members, id)

		// Notify hooks
		for _, hook := range g.membership.UpdateHooks {
			go hook(member, FailureDetected)
		}
	}
}

func (g *GossipManager) handleJoin(msg Message, from *net.UDPAddr) {
	g.membership.Lock()
	defer g.membership.Unlock()

	// Add new member to membership list
	newMember := &Member{
		ID:            msg.Sender,
		Incarnation:   msg.Incarnation,
		Status:        Alive,
		LastHeartbeat: time.Now(),
	}

	memberKey := msg.Sender.String()
	g.membership.Members[memberKey] = newMember

	// Send membership list as response
	members := make([]MemberUpdate, 0, len(g.membership.Members))
	for _, member := range g.membership.Members {
		members = append(members, MemberUpdate{
			NodeID:      member.ID,
			Incarnation: member.Incarnation,
			Status:      member.Status,
			Timestamp:   time.Now(),
		})
	}

	response := Message{
		Type:        JoinResponse,
		Sender:      g.membership.LocalNode,
		Incarnation: g.membership.Incarnation,
		Members:     members,
	}

	g.network.Send(response, msg.Sender.Address())
	log.Printf("New member joined: %s", msg.Sender)
}

func (g *GossipManager) handleJoinResponse(msg Message, from *net.UDPAddr) {
	g.membership.Lock()
	defer g.membership.Unlock()

	// Process all members from the response
	for _, update := range msg.Members {
		memberKey := update.NodeID.String()

		// Skip self
		if memberKey == g.membership.LocalNode.String() {
			continue
		}

		member, exists := g.membership.Members[memberKey]
		if !exists {
			// New member
			newMember := &Member{
				ID:            update.NodeID,
				Incarnation:   update.Incarnation,
				Status:        update.Status,
				LastHeartbeat: time.Now(),
			}
			g.membership.Members[memberKey] = newMember
		} else if update.Incarnation > member.Incarnation {
			// Update existing member
			member.Incarnation = update.Incarnation
			member.Status = update.Status
			member.LastHeartbeat = time.Now()
		}
	}

	log.Printf("Received membership list from %s, now have %d members", msg.Sender, len(g.membership.Members))
}

// JoinGroup sends a join request to the introducer
func (g *GossipManager) JoinGroup(introducerAddr string) error {
	if introducerAddr == "" {
		return nil // This node is the introducer
	}

	joinMsg := Message{
		Type:        Join,
		Sender:      g.membership.LocalNode,
		Incarnation: g.membership.Incarnation,
	}

	return g.network.Send(joinMsg, introducerAddr)
}

func (g *GossipManager) handleAliveMessage(msg Message, from *net.UDPAddr) {
	g.membership.Lock()
	defer g.membership.Unlock()

	memberKey := msg.Sender.String()
	member, exists := g.membership.Members[memberKey]

	if exists && msg.Incarnation > member.Incarnation {
		member.Incarnation = msg.Incarnation
		member.Status = Alive
		member.LastHeartbeat = time.Now()
		member.SuspicionStart = time.Time{}
		log.Printf("Member %s refuted suspicion with incarnation %d", msg.Sender, msg.Incarnation)
	}
}

func (g *GossipManager) handleSuspectMessage(msg Message, from *net.UDPAddr) {
	g.membership.Lock()
	defer g.membership.Unlock()

	targetKey := msg.Target.String()

	// Don't process suspicion about self
	if targetKey == g.membership.LocalNode.String() {
		// Counter suspicion about self
		if g.enableSuspicion {
			g.suspicionMgr.ProcessSuspicion(msg.Target, msg.Incarnation)
		}
		return
	}

	member, exists := g.membership.Members[targetKey]
	if exists && msg.Incarnation >= member.Incarnation {
		if member.Status == Alive {
			member.Status = Suspected
			member.Incarnation = msg.Incarnation
			member.SuspicionStart = time.Now()
			log.Printf("Member %s suspected with incarnation %d", msg.Target, msg.Incarnation)
		}
	}
}
