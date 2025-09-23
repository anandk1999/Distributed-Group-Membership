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

	// We'll collect piggyback updates to process after unlocking so we can pass the reporter.
	piggybacks := make([]MemberUpdate, 0, len(msg.Members))
	for _, update := range msg.Members {
		piggybacks = append(piggybacks, update)
	}
	g.membership.Unlock()

	// Process piggybacked updates — reporter is the heartbeat message sender
	for _, update := range piggybacks {
		g.processUpdate(update, msg.Sender)
	}
}

func (g *GossipManager) processUpdate(update MemberUpdate, reporter NodeID) {
	memberKey := update.NodeID.String()

	// Don't process updates about self, except for suspicion refutation
	if memberKey == g.membership.LocalNode.String() {
		if update.Status == Suspected && g.enableSuspicion {
			// Another node suspects us — let suspicion manager handle self-refutation.
			// Pass the reporter who claimed the suspicion.
			g.suspicionMgr.ProcessSuspicion(reporter, update.NodeID, update.Incarnation)
		} else if update.Status == Alive {
			// if someone reports us Alive with a higher incarnation, accept it
			g.suspicionMgr.ClearSuspect(update.NodeID, update.Incarnation)
		}
		return
	}

	// Update membership entries — do quick membership updates while holding lock only for the minimal duration
	g.membership.Lock()
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
		// trigger join hooks (do it async)
		for _, hook := range g.membership.UpdateHooks {
			go hook(newMember, Joined)
		}
		g.membership.Unlock()
		return
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
	g.membership.Unlock()

	// If the piggyback is a Suspect message, report that suspicion to the suspicion manager
	if update.Status == Suspected && g.enableSuspicion {
		g.suspicionMgr.ProcessSuspicion(reporter, update.NodeID, update.Incarnation)
	}
	// If piggyback says Alive, clear any tracked suspicion
	if update.Status == Alive {
		g.suspicionMgr.ClearSuspect(update.NodeID, update.Incarnation)
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
	// We'll collect suspects to report after unlocking, and members to remove to notify hooks.
	now := time.Now()
	type suspectInfo struct {
		id   string
		node NodeID
		inc  int32
	}
	var suspects []suspectInfo
	var toRemove [](*Member)

	g.membership.Lock()
	for id, member := range g.membership.Members {
		// Skip self
		if id == g.membership.LocalNode.String() {
			continue
		}

		switch member.Status {
		case Alive:
			if now.Sub(member.LastHeartbeat) > g.suspicionTime {
				if g.enableSuspicion {
					// mark suspected locally and start suspicion tracking
					member.Status = Suspected
					member.SuspicionStart = now
					suspects = append(suspects, suspectInfo{id: id, node: member.ID, inc: member.Incarnation})
				} else {
					// direct failure without suspicion: schedule removal
					toRemove = append(toRemove, cloneMember(member))
					delete(g.membership.Members, id)
				}
			}
		case Suspected:
			if now.Sub(member.SuspicionStart) > g.failureTime {
				// finalize failure: remove and schedule hooks
				toRemove = append(toRemove, cloneMember(member))
				delete(g.membership.Members, id)
			}
		}
	}
	g.membership.Unlock()

	// Report collected suspects to suspicion manager (we are the reporter)
	for _, s := range suspects {
		// Our local node is the reporter
		g.suspicionMgr.ProcessSuspicion(g.membership.LocalNode, s.node, s.inc)
	}

	// Notify hooks for removed members (do this outside membership lock)
	for _, m := range toRemove {
		for _, hook := range g.membership.UpdateHooks {
			go hook(m, FailureDetected)
		}
	}
}

// cloneMember makes a shallow copy so we can safely hand objects to hooks after deletion.
func cloneMember(m *Member) *Member {
	if m == nil {
		return nil
	}
	c := *m
	return &c
}

func (g *GossipManager) handleJoin(msg Message, from *net.UDPAddr) {
	g.membership.Lock()

	// Add new member to membership list
	newMember := &Member{
		ID:            msg.Sender,
		Incarnation:   msg.Incarnation,
		Status:        Alive,
		LastHeartbeat: time.Now(),
	}

	memberKey := msg.Sender.String()
	g.membership.Members[memberKey] = newMember

	// Trigger hooks for new member (do asynchronously)
	for _, hook := range g.membership.UpdateHooks {
		go hook(newMember, Joined)
	}

	// Build membership list for response
	members := make([]MemberUpdate, 0, len(g.membership.Members))
	for _, member := range g.membership.Members {
		members = append(members, MemberUpdate{
			NodeID:      member.ID,
			Incarnation: member.Incarnation,
			Status:      member.Status,
			Timestamp:   time.Now(),
		})
	}
	g.membership.Unlock()

	response := Message{
		Type:        JoinResponse,
		Sender:      g.membership.LocalNode,
		Incarnation: g.membership.Incarnation,
		Members:     members,
	}

	// send response outside locks
	g.network.Send(response, msg.Sender.Address())
	log.Printf("New member joined: %s", msg.Sender)
}

func (g *GossipManager) handleJoinResponse(msg Message, from *net.UDPAddr) {
	// We'll collect clears for any Alive updates and apply them after updating membership.
	clears := []MemberUpdate{}

	g.membership.Lock()
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

			// Trigger hooks for new member
			for _, hook := range g.membership.UpdateHooks {
				go hook(newMember, Joined)
			}
			if update.Status == Alive {
				clears = append(clears, update)
			}
		} else if update.Incarnation > member.Incarnation {
			// Update existing member
			member.Incarnation = update.Incarnation
			member.Status = update.Status
			member.LastHeartbeat = time.Now()

			// Trigger hooks for status change
			for _, hook := range g.membership.UpdateHooks {
				go hook(member, StatusChanged)
			}
			if update.Status == Alive {
				clears = append(clears, update)
			}
		}
	}
	g.membership.Unlock()

	// Clear any tracked suspicions that are refuted by Alive reports
	for _, u := range clears {
		g.suspicionMgr.ClearSuspect(u.NodeID, u.Incarnation)
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
	// Update membership and then clear any tracked suspicion outside the lock.
	g.membership.Lock()
	memberKey := msg.Sender.String()
	member, exists := g.membership.Members[memberKey]

	if exists && msg.Incarnation >= member.Incarnation {
		member.Incarnation = msg.Incarnation
		member.Status = Alive
		member.LastHeartbeat = time.Now()
		member.SuspicionStart = time.Time{}
	}
	g.membership.Unlock()

	// Clear tracked suspicion (if any) outside membership lock
	g.suspicionMgr.ClearSuspect(msg.Sender, msg.Incarnation)
	if exists {
		log.Printf("Member %s refuted suspicion with incarnation %d", msg.Sender, msg.Incarnation)
	}
}

func (g *GossipManager) handleSuspectMessage(msg Message, from *net.UDPAddr) {
	// When we receive an explicit Suspect message from some peer,
	// pass it to the suspicion manager with the sender as the reporter.
	// The suspicion manager's OnSuspect callback will update membership state (via callback).
	if !g.enableSuspicion {
		return
	}
	g.suspicionMgr.ProcessSuspicion(msg.Sender, msg.Target, msg.Incarnation)
}
