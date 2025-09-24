package pingack

import (
	"fmt"
	"log"
	. "mp2-g02/membership"
	. "mp2-g02/network"
	. "mp2-g02/suspicion"
	. "mp2-g02/types"
	"net"
	"time"
)

type pendingPing struct {
	target       NodeID
	seqNum       uint64
	startTime    time.Time
	directAck    bool
	indirectAcks map[string]bool
	ackReceived  chan bool
}

type PingAckManager struct {
	membership      *MembershipList
	network         *NetworkLayer
	protocolPeriod  time.Duration
	ackTimeout      time.Duration
	k               int
	suspicionTime   time.Duration
	failureTime     time.Duration
	stopped         chan bool
	suspicionMgr    *SuspicionManager
	seqNum          uint64
	pendingAcks     map[uint64]*pendingPing
	pendingIndirect map[string]*struct {
		requester NodeID
		ch        chan bool
	}
	enableSuspicion bool
}

func NewPingAckManager(ml *MembershipList, net *NetworkLayer, suspicionMgr *SuspicionManager) *PingAckManager {
	return &PingAckManager{
		membership:     ml,
		network:        net,
		protocolPeriod: 2 * time.Second,
		ackTimeout:     500 * time.Millisecond,
		k:              1,
		suspicionTime:  2 * time.Second,
		failureTime:    1 * time.Second,
		stopped:        make(chan bool),
		suspicionMgr:   suspicionMgr,
		seqNum:         0,
		pendingAcks:    make(map[uint64]*pendingPing),
		pendingIndirect: make(map[string]*struct {
			requester NodeID
			ch        chan bool
		}),
		enableSuspicion: true,
	}
}

func (p *PingAckManager) Start() {
	// Register handlers
	p.network.RegisterHandler(Ping, p.handlePing)
	p.network.RegisterHandler(Ack, p.handleAck)
	p.network.RegisterHandler(IndirectPing, p.handleIndirectPing) // ping-req handler
	p.network.RegisterHandler(IndirectAck, p.handleIndirectAck)   // indirect ack handler
	p.network.RegisterHandler(Join, p.handleJoin)
	p.network.RegisterHandler(JoinResponse, p.handleJoinResponse)
	p.network.RegisterHandler(AliveMsg, p.handleAliveMessage)
	p.network.RegisterHandler(Suspect, p.handleSuspectMessage)

	go p.pingLoop()
	go p.failureDetectionLoop()

	log.Println("PingAck manager started")

}

func (p *PingAckManager) Stop() {
	close(p.stopped)
}

func (p *PingAckManager) SetSuspicion(enable bool) {
	p.enableSuspicion = enable
}

func (p *PingAckManager) pingLoop() {
	ticker := time.NewTicker(p.protocolPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopped:
			return
		case <-ticker.C:
			p.performSWIMProtocolPeriod()
		}
	}
}

func (p *PingAckManager) performSWIMProtocolPeriod() {
	targets := p.membership.GetRandomMembers(1, []string{p.membership.LocalNode.String()})
	if len(targets) == 0 {
		return
	}

	target := targets[0]
	p.seqNum++
	seqNum := p.seqNum

	log.Printf("SWIM Protocol Period %d: Pinging %s", seqNum, target.ID)

	updates := p.membership.GetRecentUpdates(5)

	pingMsg := Message{
		Type:        Ping,
		Sender:      p.membership.LocalNode,
		Target:      target.ID,
		Incarnation: p.membership.Incarnation,
		SeqNum:      seqNum,
		Members:     updates,
	}

	pending := &pendingPing{
		target:       target.ID,
		seqNum:       seqNum,
		startTime:    time.Now(),
		directAck:    false,
		indirectAcks: make(map[string]bool),
		ackReceived:  make(chan bool, 1),
	}

	p.pendingAcks[seqNum] = pending

	if err := p.network.Send(pingMsg, target.ID.Address()); err != nil {
		log.Printf("Failed to send direct ping to %s: %v", target.ID, err)
	}

	directAckTimer := time.NewTimer(p.ackTimeout)
	defer directAckTimer.Stop()

	select {
	case <-pending.ackReceived:
		log.Printf("Direct ACK received from %s (seq %d)", target.ID, seqNum)
		delete(p.pendingAcks, seqNum)
		return

	case <-directAckTimer.C:
		log.Printf("Direct ACK timeout for %s (seq %d), starting indirect probing", target.ID, seqNum)
	}

	indirectMembers := p.membership.GetRandomMembers(
		p.k,
		[]string{p.membership.LocalNode.String(), target.ID.String()},
	)

	if len(indirectMembers) == 0 {
		log.Printf("No members available for indirect probing of %s", target.ID)
		delete(p.pendingAcks, seqNum)
		p.declareFailure(target.ID)
		return
	}

	for _, indirectMember := range indirectMembers {
		pingReqMsg := Message{
			Type:        IndirectPing,
			Sender:      p.membership.LocalNode,
			Target:      target.ID,
			Incarnation: p.membership.Incarnation,
			SeqNum:      seqNum,
			Members:     updates,
		}

		if err := p.network.Send(pingReqMsg, indirectMember.ID.Address()); err != nil {
			log.Printf("Failed to send ping-req to %s: %v", indirectMember.ID, err)
		} else {
			log.Printf("Sent ping-req to %s for target %s (seq %d)", indirectMember.ID, target.ID, seqNum)
		}
	}

	remainingTimeout := p.protocolPeriod - p.ackTimeout - 100*time.Millisecond
	indirectAckTimer := time.NewTimer(remainingTimeout)
	defer indirectAckTimer.Stop()

	select {
	case <-pending.ackReceived:
		log.Printf("Indirect ACK received for %s (seq %d)", target.ID, seqNum)
		delete(p.pendingAcks, seqNum)
		return

	case <-indirectAckTimer.C:
		log.Printf("No ACKs received for %s (seq %d), declaring failure", target.ID, seqNum)
		delete(p.pendingAcks, seqNum)
		p.declareFailure(target.ID)
	}
}

// declareFailure marks a member as failed and removes from membership
func (p *PingAckManager) declareFailure(nodeID NodeID) {
	p.membership.Lock()
	defer p.membership.Unlock()

	memberKey := nodeID.String()
	if member, exists := p.membership.Members[memberKey]; exists {
		member.Status = Failed
		p.membership.AddRecentUpdate(member)
		delete(p.membership.Members, memberKey)

		log.Printf("Declared %s as FAILED", nodeID)

		// Trigger failure hooks
		for _, hook := range p.membership.UpdateHooks {
			go hook(member, FailureDetected)
		}
	}
}

func (p *PingAckManager) processUpdate(update MemberUpdate, reporter NodeID) {
	memberKey := update.NodeID.String()

	// Don't process updates about self, except for suspicion refutation
	if memberKey == p.membership.LocalNode.String() {
		if update.Status == Suspected && p.enableSuspicion {
			// Another node suspects us — let suspicion manager handle self-refutation.
			// Pass the reporter who claimed the suspicion.
			p.suspicionMgr.ProcessSuspicion(reporter, update.NodeID, update.Incarnation)
		} else if update.Status == Alive {
			// if someone reports us Alive with a higher incarnation, accept it
			p.suspicionMgr.ClearSuspect(update.NodeID, update.Incarnation)
		}
		return
	}

	// Update membership entries — do quick membership updates while holding lock only for the minimal duration
	p.membership.Lock()
	member, exists := p.membership.Members[memberKey]

	if !exists {
		// New member discovered - accept it regardless of status
		// This prevents missing members when they're first heard about as suspected
		newMember := &Member{
			ID:            update.NodeID,
			Incarnation:   update.Incarnation,
			Status:        update.Status,
			LastHeartbeat: time.Now(),
		}

		// Set suspicion start time if initially suspected
		if update.Status == Suspected {
			newMember.SuspicionStart = time.Now()
		}

		p.membership.Members[memberKey] = newMember
		p.membership.AddRecentUpdate(newMember)

		// trigger join hooks (do it async)
		for _, hook := range p.membership.UpdateHooks {
			go hook(newMember, Joined)
		}
		p.membership.Unlock()
		return
	} else if exists {
		// Update existing member based on incarnation
		if update.Incarnation > member.Incarnation {
			member.Incarnation = update.Incarnation
			member.Status = update.Status

			if update.Status == Alive {
				member.LastHeartbeat = time.Now()
				member.SuspicionStart = time.Time{}
			} else if update.Status == Suspected && p.enableSuspicion {
				member.SuspicionStart = time.Now()
			}

			// Add to recent updates to propagate this change
			p.membership.AddRecentUpdate(member)
		}
	}
	p.membership.Unlock()

	// If the piggyback is a Suspect message, report that suspicion to the suspicion manager
	if update.Status == Suspected && p.enableSuspicion {
		p.suspicionMgr.ProcessSuspicion(reporter, update.NodeID, update.Incarnation)
	}
	// If piggyback says Alive, clear any tracked suspicion
	if update.Status == Alive {
		p.suspicionMgr.ClearSuspect(update.NodeID, update.Incarnation)
	}
}

func (p *PingAckManager) failureDetectionLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopped:
			return
		case <-ticker.C:
			p.checkFailures()
		}
	}
}

func (p *PingAckManager) checkFailures() {
	// We'll collect suspects to report after unlocking, and members to remove to notify hooks.
	now := time.Now()
	type suspectInfo struct {
		id   string
		node NodeID
		inc  int32
	}
	var suspects []suspectInfo
	var toRemove [](*Member)

	p.membership.Lock()
	for id, member := range p.membership.Members {
		// Skip self
		if id == p.membership.LocalNode.String() {
			continue
		}

		switch member.Status {
		case Alive:
			if now.Sub(member.LastHeartbeat) > p.suspicionTime {
				if p.enableSuspicion {
					// mark suspected locally and start suspicion tracking
					member.Status = Suspected
					member.SuspicionStart = now
					suspects = append(suspects, suspectInfo{id: id, node: member.ID, inc: member.Incarnation})
				} else {
					// direct failure without suspicion: schedule removal
					toRemove = append(toRemove, cloneMember(member))
					delete(p.membership.Members, id)
				}
			}
		case Suspected:
			if now.Sub(member.SuspicionStart) > p.failureTime {
				// finalize failure: remove and schedule hooks
				toRemove = append(toRemove, cloneMember(member))
				delete(p.membership.Members, id)
			}
		}
	}
	p.membership.Unlock()

	// Report collected suspects to suspicion manager (we are the reporter)
	for _, s := range suspects {
		// Our local node is the reporter
		p.suspicionMgr.ProcessSuspicion(p.membership.LocalNode, s.node, s.inc)
	}

	// Notify hooks for removed members (do this outside membership lock)
	for _, m := range toRemove {
		for _, hook := range p.membership.UpdateHooks {
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

// JoinGroup sends a join request to the introducer
func (p *PingAckManager) JoinGroup(introducerAddr string) error {
	if introducerAddr == "" {
		return nil // This node is the introducer
	}

	joinMsg := Message{
		Type:        Join,
		Sender:      p.membership.LocalNode,
		Incarnation: p.membership.Incarnation,
	}

	return p.network.Send(joinMsg, introducerAddr)
}

func (p *PingAckManager) handlePing(msg Message, from *net.UDPAddr) {
	// Send ACK back immediately
	ack := Message{
		Type:        Ack,
		Sender:      p.membership.LocalNode,
		Target:      msg.Sender,
		SeqNum:      msg.SeqNum,
		Incarnation: p.membership.Incarnation,
	}

	if err := p.network.Send(ack, msg.Sender.Address()); err != nil {
		log.Printf("Failed to send ACK to %s: %v", msg.Sender, err)
	}

	log.Printf("Received PING from %s (seq %d), sent ACK", msg.Sender, msg.SeqNum)

	p.membership.Lock()
	// Update sender's heartbeat
	senderKey := msg.Sender.String()
	sender, exists := p.membership.Members[senderKey]

	if !exists {
		// New member discovered
		newMember := &Member{
			ID:            msg.Sender,
			Incarnation:   msg.Incarnation,
			Status:        Alive,
			LastHeartbeat: time.Now(),
		}
		p.membership.Members[senderKey] = newMember

		// Add to recent updates for gossip propagation
		p.membership.AddRecentUpdate(newMember)

		// Trigger join hooks
		for _, hook := range p.membership.UpdateHooks {
			go hook(newMember, Joined)
		}
	} else {
		// Update existing member
		updated := false
		if msg.Incarnation > sender.Incarnation {
			sender.Incarnation = msg.Incarnation
			sender.Status = Alive
			sender.LastHeartbeat = time.Now()
			sender.SuspicionStart = time.Time{}
			updated = true
		} else if msg.Incarnation == sender.Incarnation {
			sender.LastHeartbeat = time.Now()
		}

		// Add to recent updates if we made a significant change
		if updated {
			p.membership.AddRecentUpdate(sender)
		}
	}

	// We'll collect piggyback updates to process after unlocking so we can pass the reporter.
	piggybacks := make([]MemberUpdate, 0, len(msg.Members))
	piggybacks = append(piggybacks, msg.Members...)
	p.membership.Unlock()

	// Debug: log received updates
	if len(piggybacks) > 0 {
		log.Printf("Received heartbeat from %s with %d piggybacked updates", msg.Sender, len(piggybacks))
		for _, update := range piggybacks {
			log.Printf("  <- %s: %s (Inc:%d)", update.NodeID, update.Status, update.Incarnation)
		}
	}

	// Process piggybacked updates — reporter is the heartbeat message sender
	for _, update := range piggybacks {
		p.processUpdate(update, msg.Sender)
	}
}

// handleAck processes ACK messages (direct responses to pings)
func (p *PingAckManager) handleAck(msg Message, from *net.UDPAddr) {
	log.Printf("Received ACK from %s (seq %d)", msg.Sender, msg.SeqNum)

	// 1) Refresh membership heartbeat for ACK sender
	p.membership.Lock()
	senderKey := msg.Sender.String()
	if m, ok := p.membership.Members[senderKey]; ok {
		updated := false
		if msg.Incarnation > m.Incarnation {
			m.Incarnation = msg.Incarnation
			m.Status = Alive
			m.LastHeartbeat = time.Now()
			m.SuspicionStart = time.Time{}
			updated = true
		} else if msg.Incarnation == m.Incarnation {
			m.LastHeartbeat = time.Now()
		}
		if updated {
			p.membership.AddRecentUpdate(m)
		}
	} else {
		// discover new member on ACK path (unlikely but safe)
		newM := &Member{ID: msg.Sender, Incarnation: msg.Incarnation, Status: Alive, LastHeartbeat: time.Now()}
		p.membership.Members[senderKey] = newM
		p.membership.AddRecentUpdate(newM)
		for _, hook := range p.membership.UpdateHooks {
			go hook(newM, Joined)
		}
	}
	p.membership.Unlock()

	// Clear any tracked suspicion for this sender
	p.suspicionMgr.ClearSuspect(msg.Sender, msg.Incarnation)

	// 2) If this ACK corresponds to our own outstanding direct ping, signal it
	if pending, ok := p.pendingAcks[msg.SeqNum]; ok && pending.target.Equals(msg.Sender) {
		pending.directAck = true
		select {
		case pending.ackReceived <- true:
		default:
		}
	}

	// 3) If this ACK completes an indirect probe we are relaying, notify waiter
	indKey := fmt.Sprintf("%d|%s", msg.SeqNum, msg.Sender.String())
	if waiter, ok := p.pendingIndirect[indKey]; ok {
		select {
		case waiter.ch <- true:
		default:
		}
	}
}

// handleIndirectPing processes ping-req messages (indirect ping requests)
func (p *PingAckManager) handleIndirectPing(msg Message, from *net.UDPAddr) {
	log.Printf("Received ping-req from %s for target %s (seq %d)", msg.Sender, msg.Target, msg.SeqNum)

	// Register a short-lived waiter so when we get ACK from target we can forward IndirectAck to requester
	key := fmt.Sprintf("%d|%s", msg.SeqNum, msg.Target.String())
	w := &struct {
		requester NodeID
		ch        chan bool
	}{requester: msg.Sender, ch: make(chan bool, 1)}
	p.pendingIndirect[key] = w

	// Send a direct PING to the target with the same seq number and piggyback recent updates
	updates := p.membership.GetRecentUpdates(5)
	ping := Message{
		Type:        Ping,
		Sender:      p.membership.LocalNode,
		Target:      msg.Target,
		Incarnation: p.membership.Incarnation,
		SeqNum:      msg.SeqNum,
		Members:     updates,
	}
	if err := p.network.Send(ping, msg.Target.Address()); err != nil {
		log.Printf("Failed to send proxy PING to %s: %v", msg.Target, err)
	}

	// Wait for ACK up to ackTimeout, then forward IndirectAck if received
	go func(waitKey string, waiter *struct {
		requester NodeID
		ch        chan bool
	}) {
		t := time.NewTimer(p.ackTimeout)
		defer t.Stop()
		defer delete(p.pendingIndirect, waitKey)
		select {
		case <-waiter.ch:
			indAck := Message{
				Type:        IndirectAck,
				Sender:      p.membership.LocalNode, // proxy
				Target:      msg.Target,             // the original target we probed
				Incarnation: p.membership.Incarnation,
				SeqNum:      msg.SeqNum,
			}
			if err := p.network.Send(indAck, waiter.requester.Address()); err != nil {
				log.Printf("Failed to forward indirect ACK to %s: %v", waiter.requester, err)
			} else {
				log.Printf("Forwarded indirect ACK for %s (seq %d) to %s", msg.Target, msg.SeqNum, waiter.requester)
			}
		case <-t.C:
			// no ack observed within timeout; silently give up
			log.Printf("Proxy PING timeout for %s (seq %d); no indirect ACK sent", msg.Target, msg.SeqNum)
		}
	}(key, w)
}

// handleIndirectAck processes indirect ACK messages
func (p *PingAckManager) handleIndirectAck(msg Message, from *net.UDPAddr) {
	log.Printf("Received indirect ACK from %s for target %s (seq %d)", msg.Sender, msg.Target, msg.SeqNum)

	// Satisfy any pending probe for this seq/target
	if pending, ok := p.pendingAcks[msg.SeqNum]; ok && pending.target.Equals(msg.Target) {
		pending.indirectAcks[msg.Sender.String()] = true
		select {
		case pending.ackReceived <- true:
		default:
		}
	}
}

func (p *PingAckManager) handleJoin(msg Message, from *net.UDPAddr) {
	p.membership.Lock()

	// Add new member to membership list
	newMember := &Member{
		ID:            msg.Sender,
		Incarnation:   msg.Incarnation,
		Status:        Alive,
		LastHeartbeat: time.Now(),
	}

	memberKey := msg.Sender.String()
	p.membership.Members[memberKey] = newMember
	p.membership.AddRecentUpdate(newMember)

	// Trigger hooks for new member (do asynchronously)
	for _, hook := range p.membership.UpdateHooks {
		go hook(newMember, Joined)
	}

	// Build membership list for response
	members := make([]MemberUpdate, 0, len(p.membership.Members))
	for _, member := range p.membership.Members {
		members = append(members, MemberUpdate{
			NodeID:      member.ID,
			Incarnation: member.Incarnation,
			Status:      member.Status,
			Timestamp:   time.Now(),
		})
	}
	p.membership.Unlock()

	response := Message{
		Type:        JoinResponse,
		Sender:      p.membership.LocalNode,
		Incarnation: p.membership.Incarnation,
		Members:     members,
	}

	// send response outside locks
	p.network.Send(response, msg.Sender.Address())
	log.Printf("New member joined: %s", msg.Sender)
}

func (p *PingAckManager) handleJoinResponse(msg Message, from *net.UDPAddr) {
	// We'll collect clears for any Alive updates and apply them after updating membership.
	clears := []MemberUpdate{}

	p.membership.Lock()
	// Process all members from the response
	for _, update := range msg.Members {
		memberKey := update.NodeID.String()

		// Skip self
		if memberKey == p.membership.LocalNode.String() {
			continue
		}

		member, exists := p.membership.Members[memberKey]
		if !exists {
			// New member
			newMember := &Member{
				ID:            update.NodeID,
				Incarnation:   update.Incarnation,
				Status:        update.Status,
				LastHeartbeat: time.Now(),
			}

			if update.Status == Suspected {
				newMember.SuspicionStart = time.Now()
			}

			p.membership.Members[memberKey] = newMember
			p.membership.AddRecentUpdate(newMember)

			// Trigger hooks for new member
			for _, hook := range p.membership.UpdateHooks {
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

			if update.Status == Suspected {
				member.SuspicionStart = time.Now()
			} else if update.Status == Alive {
				member.SuspicionStart = time.Time{}
			}

			p.membership.AddRecentUpdate(member)

			// Trigger hooks for status change
			for _, hook := range p.membership.UpdateHooks {
				go hook(member, StatusChanged)
			}
			if update.Status == Alive {
				clears = append(clears, update)
			}
		}
	}
	p.membership.Unlock()

	// Clear any tracked suspicions that are refuted by Alive reports
	for _, u := range clears {
		p.suspicionMgr.ClearSuspect(u.NodeID, u.Incarnation)
	}

	log.Printf("Received membership list from %s, now have %d members", msg.Sender, len(p.membership.Members))
}

func (p *PingAckManager) handleAliveMessage(msg Message, from *net.UDPAddr) {
	// Update membership and then clear any tracked suspicion outside the lock.
	p.membership.Lock()
	memberKey := msg.Sender.String()
	member, exists := p.membership.Members[memberKey]

	if exists && msg.Incarnation >= member.Incarnation {
		member.Incarnation = msg.Incarnation
		member.Status = Alive
		member.LastHeartbeat = time.Now()
		member.SuspicionStart = time.Time{}
	}
	p.membership.Unlock()

	// Clear tracked suspicion (if any) outside membership lock
	p.suspicionMgr.ClearSuspect(msg.Sender, msg.Incarnation)
	if exists {
		log.Printf("Member %s refuted suspicion with incarnation %d", msg.Sender, msg.Incarnation)
	}
}

func (p *PingAckManager) handleSuspectMessage(msg Message, from *net.UDPAddr) {
	// When we receive an explicit Suspect message from some peer,
	// pass it to the suspicion manager with the sender as the reporter.
	// The suspicion manager's OnSuspect callback will update membership state (via callback).
	if !p.enableSuspicion {
		return
	}
	p.suspicionMgr.ProcessSuspicion(msg.Sender, msg.Target, msg.Incarnation)
}
