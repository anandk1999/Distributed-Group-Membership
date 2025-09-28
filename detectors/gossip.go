package detectors

import (
	"log"
	"mp2-g02/utils"
	"net"
	"sync"
	"time"
)

type GossipManager struct {
	membership      *utils.MembershipList
	network         *utils.NetworkLayer
	suspicionMgr    *utils.SuspicionManager
	fanout          int
	stopped         chan bool
	enableSuspicion bool
	mu              sync.Mutex
	active          bool
	// Timeout configuration
	timeouts utils.TimeoutConfig
}

func NewGossipManager(ml *utils.MembershipList, net *utils.NetworkLayer, suspicionMgr *utils.SuspicionManager) *GossipManager {
	return &GossipManager{
		membership:      ml,
		network:         net,
		suspicionMgr:    suspicionMgr,
		fanout:          3, // Send to 3 random nodes
		stopped:         make(chan bool),
		enableSuspicion: false,
		active:          true,
		timeouts:        utils.DefaultTimeoutConfig(),
	}
}

func NewGossipManagerWithTimeouts(ml *utils.MembershipList, net *utils.NetworkLayer, suspicionMgr *utils.SuspicionManager, timeouts utils.TimeoutConfig) *GossipManager {
	return &GossipManager{
		membership:      ml,
		network:         net,
		suspicionMgr:    suspicionMgr,
		fanout:          3,
		stopped:         make(chan bool),
		enableSuspicion: false,
		active:          true,
		timeouts:        timeouts,
	}
}

func (g *GossipManager) Start() {
	// Register message handlers
	g.network.RegisterHandler(utils.Heartbeat, g.handleHeartbeat)
	g.network.RegisterHandler(utils.Suspect, g.handleSuspectMessage)
	g.network.RegisterHandler(utils.AliveMsg, g.handleAliveMessage)
	g.network.RegisterHandler(utils.Confirm, g.handleConfirmMessage)
	g.network.RegisterHandler(utils.Join, g.handleJoin)
	g.network.RegisterHandler(utils.JoinResponse, g.handleJoinResponse)
	g.network.RegisterHandler(utils.Leave, g.handleLeave)

	// Start gossip loop
	go g.gossipLoop()

	// Start failure detection loop
	go g.failureDetectionLoop()

	log.Println("Gossip manager started")
}

func (g *GossipManager) Stop() {
	close(g.stopped)
}

func (g *GossipManager) SetSuspicion(enable bool) {
	g.enableSuspicion = enable
}

func (g *GossipManager) SuspicionEnabled() bool {
	return g.enableSuspicion
}

func (g *GossipManager) gossipLoop() {
	ticker := time.NewTicker(g.timeouts.GossipPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-g.stopped:
			return
		case <-ticker.C:
			g.performGossipRound()
		}
	}
}

func (g *GossipManager) performGossipRound() {
	// Get random members to gossip to
	targets := g.membership.GetRandomMembers(g.fanout, []string{g.membership.LocalNode.String()})
	if len(targets) == 0 {
		return
	}

	// Get recent membership updates to piggyback
	updates := g.membership.GetRecentUpdates(10)

	// Create gossip message
	msg := utils.Message{
		Type:        utils.Heartbeat,
		Sender:      g.membership.LocalNode,
		Incarnation: g.membership.Incarnation,
		Members:     updates,
		Timestamp:   time.Now().Unix(),
	}

	// Send to selected targets
	for _, target := range targets {
		if err := g.network.Send(msg, target.ID.Address()); err != nil {
			log.Printf("Failed to send gossip to %s: %v", target.ID, err)
		}
	}

	log.Printf("Gossiped to %d members", len(targets))
}

func (g *GossipManager) failureDetectionLoop() {
	ticker := time.NewTicker(g.timeouts.CheckInterval)
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
	now := time.Now()
	var toRemove []*utils.Member
	var toSuspect []string
	var failureUpdates []*utils.Member

	g.membership.Lock()
	// First pass - collect all changes without modifying the map
	for id, member := range g.membership.Members {
		// Skip self
		if id == g.membership.LocalNode.String() {
			continue
		}

		switch member.Status {
		case utils.Alive:
			if now.Sub(member.LastHeartbeat) > g.timeouts.SuspicionTimeout {
				if g.enableSuspicion {
					member.Status = utils.Suspected
					member.SuspicionStart = now
					toSuspect = append(toSuspect, cloneGossipMember(member).ID.String())
				} else {
					// Direct failure without suspicion
					toRemove = append(toRemove, cloneGossipMember(member))
					failedUpdate := &utils.Member{ID: member.ID, Status: utils.Failed, Incarnation: member.Incarnation}
					failureUpdates = append(failureUpdates, failedUpdate)
					log.Printf("GOSSIP: Declared %s as FAILED (no heartbeat for %v)", member.ID, now.Sub(member.LastHeartbeat))
				}
			}
		case utils.Suspected:
			if now.Sub(member.SuspicionStart) > g.timeouts.FailureTimeout {
				toRemove = append(toRemove, cloneGossipMember(member))
				failedUpdate := &utils.Member{ID: member.ID, Status: utils.Failed, Incarnation: member.Incarnation}
				failureUpdates = append(failureUpdates, failedUpdate)
				log.Printf("GOSSIP: Declared %s as FAILED (no heartbeat for %v)", member.ID, now.Sub(member.LastHeartbeat))
			}
		}
	}

	// Second pass - apply all changes atomically (collect updates to add outside lock)
	var suspectedMembers []*utils.Member
	for _, id := range toSuspect {
		if member, exists := g.membership.Members[id]; exists {
			member.Status = utils.Suspected
			member.SuspicionStart = now
			suspectedMembers = append(suspectedMembers, member)
		}
	}

	for _, member := range toRemove {
		delete(g.membership.Members, member.ID.String())
	}

	g.membership.Unlock()

	// Add updates outside the lock to avoid deadlock
	for _, member := range suspectedMembers {
		g.membership.AddRecentUpdateSafe(member)
	}

	for _, update := range failureUpdates {
		g.membership.AddRecentUpdateSafe(update)
	}

	// Log removed members
	for _, m := range toRemove {
		log.Printf("GOSSIP: Removed failed member %s", m.ID)
	}
}

func cloneGossipMember(m *utils.Member) *utils.Member {
	if m == nil {
		return nil
	}
	c := *m
	return &c
}

// Message handlers for gossip protocol

func (g *GossipManager) handleHeartbeat(msg utils.Message, from *net.UDPAddr) {
	if !g.active {
		return
	}

	// Update sender's information
	g.membership.Lock()
	senderKey := msg.Sender.String()
	sender, exists := g.membership.Members[senderKey]

	if !exists {
		// New member
		newMember := &utils.Member{
			ID:            msg.Sender,
			Incarnation:   msg.Incarnation,
			Status:        utils.Alive,
			LastHeartbeat: time.Now(),
		}
		g.membership.Members[senderKey] = newMember
		g.membership.AddRecentUpdate(newMember)
		log.Printf("GOSSIP: New member joined %s (inc: %d)", msg.Sender, msg.Incarnation)
	} else {
		// Update existing member
		updated := false
		if msg.Incarnation > sender.Incarnation {
			// Update status of sender in own membership list
			oldStatus := sender.Status
			sender.Incarnation = msg.Incarnation
			sender.Status = utils.Alive
			sender.LastHeartbeat = time.Now()
			sender.SuspicionStart = time.Time{}
			updated = true
			log.Printf("GOSSIP: Member %s rejoined with higher incarnation %d (was %d, status: %s)", msg.Sender, msg.Incarnation, sender.Incarnation, oldStatus)
		} else if msg.Incarnation == sender.Incarnation {
			// Same incarnation - just update heartbeat
			sender.LastHeartbeat = time.Now()
			if sender.Status == utils.Suspected {
				sender.Status = utils.Alive
				sender.SuspicionStart = time.Time{}
				updated = true
				log.Printf("GOSSIP: Member %s refuted suspicion (inc: %d)", msg.Sender, msg.Incarnation)
			}
		} else {
			// Lower incarnation - ignore (stale message)
			log.Printf("GOSSIP: Ignoring stale message from %s (inc: %d < %d)", msg.Sender, msg.Incarnation, sender.Incarnation)
		}
		if updated {
			g.membership.AddRecentUpdate(sender)
		}
	}

	// Process piggybacked updates
	piggybacks := make([]utils.MemberUpdate, 0, len(msg.Members))
	piggybacks = append(piggybacks, msg.Members...)
	g.membership.Unlock()

	// Process each piggybacked update
	for _, update := range piggybacks {
		g.processUpdate(update, msg.Sender)
	}
}

func (g *GossipManager) processUpdate(update utils.MemberUpdate, reporter utils.NodeID) {
	memberKey := update.NodeID.String()

	// Handle self-refutation
	if memberKey == g.membership.LocalNode.String() {
		if update.Status == utils.Suspected && g.enableSuspicion {
			log.Printf("GOSSIP: Received suspicion about self from %s (inc: %d)", reporter, update.Incarnation)
			g.suspicionMgr.ProcessSuspicion(reporter, update.NodeID, update.Incarnation)
		} else if update.Status == utils.Alive {
			log.Printf("GOSSIP: Received alive message about self from %s (inc: %d)", reporter, update.Incarnation)
			g.suspicionMgr.ClearSuspect(update.NodeID, update.Incarnation)
		}
		return
	}

	g.membership.Lock()
	member, exists := g.membership.Members[memberKey]

	if !exists {
		// New member from piggyback
		newMember := &utils.Member{
			ID:            update.NodeID,
			Incarnation:   update.Incarnation,
			Status:        update.Status,
			LastHeartbeat: time.Now(),
		}
		if update.Status == utils.Suspected {
			newMember.SuspicionStart = time.Now()
		}
		g.membership.Members[memberKey] = newMember
		g.membership.AddRecentUpdate(newMember)
		log.Printf("GOSSIP: Learned about new member %s (inc: %d, status: %s) from %s", update.NodeID, update.Incarnation, update.Status, reporter)
		g.membership.Unlock()
		return
	}

	// Update existing member
	updated := false
	if update.Incarnation > member.Incarnation {
		// Higher incarnation - accept update
		oldStatus := member.Status
		member.Incarnation = update.Incarnation
		member.Status = update.Status
		if update.Status == utils.Alive {
			member.LastHeartbeat = time.Now()
			member.SuspicionStart = time.Time{}
		} else if update.Status == utils.Suspected && g.enableSuspicion {
			member.SuspicionStart = time.Now()
		}
		updated = true
		// Only log status changes that aren't suspect/failed (those are handled by callbacks)
		if update.Status != utils.Suspected && update.Status != utils.Failed {
			log.Printf("GOSSIP: Updated member %s (inc: %d -> %d, status: %s -> %s) from %s", update.NodeID, member.Incarnation, update.Incarnation, oldStatus, update.Status, reporter)
		}
	} else if update.Incarnation == member.Incarnation {
		// Same incarnation - check for status changes
		if update.Status == utils.Alive && member.Status == utils.Suspected {
			member.Status = utils.Alive
			member.LastHeartbeat = time.Now()
			member.SuspicionStart = time.Time{}
			updated = true
			log.Printf("GOSSIP: Member %s cleared suspicion (inc: %d) from %s", update.NodeID, update.Incarnation, reporter)
		} else if update.Status == utils.Suspected && member.Status == utils.Alive {
			member.Status = utils.Suspected
			member.SuspicionStart = time.Now()
			updated = true
			// Note: Suspicion processing will handle logging via callbacks
		}
	} else {
		// Lower incarnation - ignore
		log.Printf("GOSSIP: Ignoring stale update for %s (inc: %d < %d) from %s", update.NodeID, update.Incarnation, member.Incarnation, reporter)
	}

	if updated {
		g.membership.AddRecentUpdate(member)
	}
	g.membership.Unlock()

	// Handle suspicion processing
	if update.Status == utils.Suspected && g.enableSuspicion {
		g.suspicionMgr.ProcessSuspicion(reporter, update.NodeID, update.Incarnation)
	}
	if update.Status == utils.Alive {
		g.suspicionMgr.ClearSuspect(update.NodeID, update.Incarnation)
	}
}

func (g *GossipManager) handleSuspectMessage(msg utils.Message, from *net.UDPAddr) {
	if !g.enableSuspicion || !g.active {
		return
	}
	g.suspicionMgr.ProcessSuspicion(msg.Sender, msg.Target, msg.Incarnation)
}

func (g *GossipManager) handleAliveMessage(msg utils.Message, from *net.UDPAddr) {
	if !g.active {
		return
	}
	g.membership.Lock()
	memberKey := msg.Sender.String()
	member, exists := g.membership.Members[memberKey]

	if exists && msg.Incarnation >= member.Incarnation {
		if member.Status != utils.Alive || msg.Incarnation > member.Incarnation {
			member.Incarnation = msg.Incarnation
			member.Status = utils.Alive
			member.LastHeartbeat = time.Now()
			member.SuspicionStart = time.Time{}
			g.membership.AddRecentUpdate(member)
		}
	}
	g.membership.Unlock()

	g.suspicionMgr.ClearSuspect(msg.Sender, msg.Incarnation)
	if exists {
		log.Printf("Member %s refuted suspicion with incarnation %d", msg.Sender, msg.Incarnation)
	}
}

func (g *GossipManager) handleConfirmMessage(msg utils.Message, from *net.UDPAddr) {
	if !g.active {
		return
	}
	g.membership.Lock()
	memberKey := msg.Target.String()
	if member, exists := g.membership.Members[memberKey]; exists {
		if msg.Incarnation >= member.Incarnation {
			member.Status = utils.Failed
			member.Incarnation = msg.Incarnation
			g.membership.AddRecentUpdate(member)
		}
	}
	g.membership.Unlock()
}

func (g *GossipManager) handleJoin(msg utils.Message, from *net.UDPAddr) {
	if !g.active {
		return
	}
	g.membership.Lock()

	// Check for and remove any old entries with the same IP:Port but different timestamp
	joinerAddress := msg.Sender.Address()
	var oldEntriesToRemove []string
	for key, member := range g.membership.Members {
		if member.ID.Address() == joinerAddress && member.ID.Timestamp != msg.Sender.Timestamp {
			oldEntriesToRemove = append(oldEntriesToRemove, key)
		}
	}

	// Remove old entries before adding the new one (collect updates for later)
	var oldFailedUpdates []*utils.Member
	for _, key := range oldEntriesToRemove {
		if oldMember, exists := g.membership.Members[key]; exists {
			log.Printf("Removing old entry for rejoining node %s (old timestamp: %d, new timestamp: %d)",
				joinerAddress, oldMember.ID.Timestamp, msg.Sender.Timestamp)
			delete(g.membership.Members, key)
			// Add a failed update for the old entry to propagate the removal
			failedUpdate := &utils.Member{ID: oldMember.ID, Status: utils.Failed, Incarnation: oldMember.Incarnation}
			oldFailedUpdates = append(oldFailedUpdates, failedUpdate)
		}
	}

	newMember := &utils.Member{
		ID:            msg.Sender,
		Incarnation:   msg.Incarnation,
		Status:        utils.Alive,
		LastHeartbeat: time.Now(),
	}
	memberKey := msg.Sender.String()
	g.membership.Members[memberKey] = newMember

	// Send membership list back
	members := make([]utils.MemberUpdate, 0, len(g.membership.Members))
	for _, member := range g.membership.Members {
		members = append(members, utils.MemberUpdate{
			NodeID:      member.ID,
			Incarnation: member.Incarnation,
			Status:      member.Status,
			Timestamp:   time.Now(),
		})
	}
	g.membership.Unlock()

	// Add updates outside the lock to avoid deadlock
	for _, failedUpdate := range oldFailedUpdates {
		g.membership.AddRecentUpdateSafe(failedUpdate)
	}
	g.membership.AddRecentUpdateSafe(newMember)

	response := utils.Message{
		Type:        utils.JoinResponse,
		Sender:      g.membership.LocalNode,
		Incarnation: g.membership.Incarnation,
		Members:     members,
	}

	g.network.Send(response, msg.Sender.Address())
	log.Printf("New member joined: %s", msg.Sender)
}

func (g *GossipManager) handleJoinResponse(msg utils.Message, from *net.UDPAddr) {
	if !g.active {
		return
	}
	clears := []utils.MemberUpdate{}

	g.membership.Lock()
	for _, update := range msg.Members {
		memberKey := update.NodeID.String()

		if memberKey == g.membership.LocalNode.String() {
			continue
		}

		member, exists := g.membership.Members[memberKey]
		if !exists {
			newMember := &utils.Member{
				ID:            update.NodeID,
				Incarnation:   update.Incarnation,
				Status:        update.Status,
				LastHeartbeat: time.Now(),
			}
			if update.Status == utils.Suspected {
				newMember.SuspicionStart = time.Now()
			}
			g.membership.Members[memberKey] = newMember
			g.membership.AddRecentUpdate(newMember)
			if update.Status == utils.Alive {
				clears = append(clears, update)
			}
		} else if update.Incarnation > member.Incarnation {
			member.Incarnation = update.Incarnation
			member.Status = update.Status
			member.LastHeartbeat = time.Now()
			if update.Status == utils.Suspected {
				member.SuspicionStart = time.Now()
			} else if update.Status == utils.Alive {
				member.SuspicionStart = time.Time{}
			}
			g.membership.AddRecentUpdate(member)
			if update.Status == utils.Alive {
				clears = append(clears, update)
			}
		}
	}
	g.membership.Unlock()

	for _, u := range clears {
		g.suspicionMgr.ClearSuspect(u.NodeID, u.Incarnation)
	}

	log.Printf("Received membership list from %s, now have %d members", msg.Sender, len(g.membership.Members))
}

func (g *GossipManager) handleLeave(msg utils.Message, from *net.UDPAddr) {
	g.membership.Lock()
	key := msg.Sender.String()
	if m, ok := g.membership.Members[key]; ok {
		delete(g.membership.Members, key)
		// Record an update to piggyback removal
		failedUpdate := &utils.Member{ID: m.ID, Status: utils.Failed, Incarnation: m.Incarnation}
		g.membership.AddRecentUpdate(failedUpdate)
		log.Printf("Member left voluntarily: %s", msg.Sender)
	}
	g.membership.Unlock()
}

func (g *GossipManager) JoinGroup(introducerAddr string) error {
	if introducerAddr == "" {
		return nil
	}

	// Reactivate the node when joining
	g.active = true

	// Refresh local node with new timestamp and incarnation
	g.membership.RefreshLocalNode()

	joinMsg := utils.Message{
		Type:        utils.Join,
		Sender:      g.membership.LocalNode,
		Incarnation: g.membership.Incarnation,
	}
	return g.network.Send(joinMsg, introducerAddr)
}

func (g *GossipManager) LeaveGroup() {
	// Broadcast Leave to all known peers
	g.membership.RLock()
	addrs := make([]string, 0, len(g.membership.Members))
	for _, m := range g.membership.Members {
		if m.ID.String() == g.membership.LocalNode.String() {
			continue
		}
		addrs = append(addrs, m.ID.Address())
	}
	self := g.membership.LocalNode
	g.membership.RUnlock()

	msg := utils.Message{Type: utils.Leave, Sender: self, Incarnation: g.membership.Incarnation}
	for _, addr := range addrs {
		_ = g.network.Send(msg, addr)
	}

	g.active = false
	log.Printf("Voluntarily left the group")
}
