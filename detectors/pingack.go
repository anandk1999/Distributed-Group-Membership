package detectors

import (
	"context"
	"fmt"
	"log"
	"mp2-g02/utils"
	"net"
	"sync"
	"time"
)

type pendingPing struct {
	target       utils.NodeID
	seqNum       uint64
	startTime    time.Time
	directAck    bool
	indirectAcks map[string]bool
	ackReceived  chan bool
}

type indirectPingWaiter struct {
	requester   utils.NodeID
	ch          chan int32
	ctx         context.Context
	cancel      context.CancelFunc
	createdTime time.Time
}

type PingAckManager struct {
	membership      *utils.MembershipList
	network         *utils.NetworkLayer
	k               int
	stopped         chan bool
	suspicionMgr    *utils.SuspicionManager
	seqNum          uint64
	pendingAcks     map[uint64]*pendingPing
	pendingIndirect map[string]*indirectPingWaiter
	enableSuspicion bool
	mu              sync.Mutex
	active          bool
	// Context for cleanup
	ctx    context.Context
	cancel context.CancelFunc
	// Timeout configuration
	timeouts utils.TimeoutConfig
}

func NewPingAckManager(ml *utils.MembershipList, net *utils.NetworkLayer, suspicionMgr *utils.SuspicionManager) *PingAckManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &PingAckManager{
		membership:      ml,
		network:         net,
		k:               5,
		stopped:         make(chan bool),
		suspicionMgr:    suspicionMgr,
		seqNum:          0,
		pendingAcks:     make(map[uint64]*pendingPing),
		pendingIndirect: make(map[string]*indirectPingWaiter),
		enableSuspicion: false,
		active:          true,
		ctx:             ctx,
		cancel:          cancel,
		timeouts:        utils.DefaultTimeoutConfig(),
	}
}

func NewPingAckManagerWithTimeouts(ml *utils.MembershipList, net *utils.NetworkLayer, suspicionMgr *utils.SuspicionManager, timeouts utils.TimeoutConfig) *PingAckManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &PingAckManager{
		membership:      ml,
		network:         net,
		k:               5,
		stopped:         make(chan bool),
		suspicionMgr:    suspicionMgr,
		seqNum:          0,
		pendingAcks:     make(map[uint64]*pendingPing),
		pendingIndirect: make(map[string]*indirectPingWaiter),
		enableSuspicion: false,
		active:          true,
		ctx:             ctx,
		cancel:          cancel,
		timeouts:        timeouts,
	}
}

func (p *PingAckManager) Start() {
	p.network.RegisterHandler(utils.Ping, p.handlePing)
	p.network.RegisterHandler(utils.Ack, p.handleAck)
	p.network.RegisterHandler(utils.IndirectPing, p.handleIndirectPing)
	p.network.RegisterHandler(utils.IndirectAck, p.handleIndirectAck)
	p.network.RegisterHandler(utils.Join, p.handleJoin)
	p.network.RegisterHandler(utils.JoinResponse, p.handleJoinResponse)
	p.network.RegisterHandler(utils.AliveMsg, p.handleAliveMessage)
	p.network.RegisterHandler(utils.Suspect, p.handleSuspectMessage)
	p.network.RegisterHandler(utils.Leave, p.handleLeave)

	go p.pingLoop()
	go p.failureDetectionLoop()
	go p.cleanupLoop()

	log.Println("PingAck manager started")
}

func (p *PingAckManager) Stop() {
	p.cancel() // Cancel context to cleanup goroutines
	close(p.stopped)
}

func (p *PingAckManager) SetSuspicion(enable bool) {
	p.enableSuspicion = enable
}

func (p *PingAckManager) SuspicionEnabled() bool {
	return p.enableSuspicion
}

func (p *PingAckManager) pingLoop() {
	ticker := time.NewTicker(p.timeouts.ProtocolPeriod)
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
	p.mu.Lock()
	p.seqNum++
	seqNum := p.seqNum
	p.mu.Unlock()

	log.Printf("SWIM Protocol Period %d: Pinging %s", seqNum, target.ID)

	updates := p.membership.GetRecentUpdates(5)

	pingMsg := utils.Message{
		Type:        utils.Ping,
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

	p.mu.Lock()
	p.pendingAcks[seqNum] = pending
	p.mu.Unlock()

	if err := p.network.Send(pingMsg, target.ID.Address()); err != nil {
		log.Printf("Failed to send direct ping to %s: %v", target.ID, err)
	}

	directAckTimer := time.NewTimer(p.timeouts.AckTimeout)
	defer directAckTimer.Stop()

	select {
	case <-pending.ackReceived:
		// log.Printf("Direct ACK received from %s (seq %d)", target.ID, seqNum)
		p.mu.Lock()
		delete(p.pendingAcks, seqNum)
		p.mu.Unlock()
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
		p.mu.Lock()
		delete(p.pendingAcks, seqNum)
		p.mu.Unlock()
		p.declareSuspicion(target.ID)
		return
	}

	for _, indirectMember := range indirectMembers {
		pingReqMsg := utils.Message{
			Type:        utils.IndirectPing,
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

	remainingTimeout := p.timeouts.ProtocolPeriod - p.timeouts.AckTimeout
	indirectAckTimer := time.NewTimer(remainingTimeout)
	defer indirectAckTimer.Stop()

	select {
	case <-pending.ackReceived:
		p.mu.Lock()
		delete(p.pendingAcks, seqNum)
		p.mu.Unlock()
		return

	case <-indirectAckTimer.C:
		p.mu.Lock()
		delete(p.pendingAcks, seqNum)
		p.mu.Unlock()
		p.declareSuspicion(target.ID)
	}
}

func (p *PingAckManager) declareSuspicion(nodeID utils.NodeID) {
	if p.enableSuspicion {
		p.membership.Lock()
		memberKey := nodeID.String()
		member, exists := p.membership.Members[memberKey]
		if !exists {
			// Create an entry as suspected so the rest of the system can converge
			member = &utils.Member{ID: nodeID, Status: utils.Suspected, LastHeartbeat: time.Now(), SuspicionStart: time.Now()}
			p.membership.Members[memberKey] = member
			p.membership.AddRecentUpdate(member)
		} else {
			// Transition to suspected if not already
			if member.Status != utils.Suspected {
				member.Status = utils.Suspected
				member.SuspicionStart = time.Now()
				p.membership.AddRecentUpdate(member)
			}
		}
		p.membership.Unlock()

		// Report our suspicion
		p.suspicionMgr.ProcessSuspicion(p.membership.LocalNode, nodeID, member.Incarnation)
		log.Printf("Declared %s as SUSPECTED (no ACKs)", nodeID)
		return
	}

	// If suspicion is disabled, we remove immediately
	p.membership.Lock()
	defer p.membership.Unlock()
	memberKey := nodeID.String()
	if member, exists := p.membership.Members[memberKey]; exists {
		failedUpdate := &utils.Member{ID: member.ID, Status: utils.Failed, Incarnation: member.Incarnation}
		p.membership.AddRecentUpdate(failedUpdate)
		delete(p.membership.Members, memberKey)
		log.Printf("Declared %s as FAILED", nodeID)
	}
}

func (p *PingAckManager) processUpdate(update utils.MemberUpdate, reporter utils.NodeID) {
	memberKey := update.NodeID.String()

	if memberKey == p.membership.LocalNode.String() {
		if update.Status == utils.Suspected && p.enableSuspicion {
			p.suspicionMgr.ProcessSuspicion(reporter, update.NodeID, update.Incarnation)
		} else if update.Status == utils.Alive {
			p.suspicionMgr.ClearSuspect(update.NodeID, update.Incarnation)
		}
		return
	}

	p.membership.Lock()
	member, exists := p.membership.Members[memberKey]

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

		p.membership.Members[memberKey] = newMember
		p.membership.AddRecentUpdate(newMember)
		p.membership.Unlock()
		return
	} else {
		if update.Incarnation > member.Incarnation {
			member.Incarnation = update.Incarnation
			member.Status = update.Status
			if update.Status == utils.Alive {
				member.LastHeartbeat = time.Now()
				member.SuspicionStart = time.Time{}
			} else if update.Status == utils.Suspected && p.enableSuspicion {
				member.SuspicionStart = time.Now()
			}
			p.membership.AddRecentUpdate(member)
		} else if update.Incarnation == member.Incarnation {
			if update.Status == utils.Alive && member.Status == utils.Suspected {
				member.Status = utils.Alive
				member.LastHeartbeat = time.Now()
				member.SuspicionStart = time.Time{}
				p.membership.AddRecentUpdate(member)
			} else if update.Status == utils.Suspected && member.Status == utils.Alive {
				member.Status = utils.Suspected
				member.SuspicionStart = time.Now()
				p.membership.AddRecentUpdate(member)
			}
		}
	}
	p.membership.Unlock()

	if update.Status == utils.Suspected && p.enableSuspicion {
		p.suspicionMgr.ProcessSuspicion(reporter, update.NodeID, update.Incarnation)
	}
	if update.Status == utils.Alive {
		p.suspicionMgr.ClearSuspect(update.NodeID, update.Incarnation)
	}
}

func (p *PingAckManager) failureDetectionLoop() {
	ticker := time.NewTicker(p.timeouts.CheckInterval)
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

func (p *PingAckManager) cleanupLoop() {
	// Cleanup orphaned entries every 5 seconds
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopped:
			return
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.cleanupOrphanedEntries()
		}
	}
}

func (p *PingAckManager) cleanupOrphanedEntries() {
	now := time.Now()
	var toCleanup []string

	p.mu.Lock()
	// Collect entries that are older than 2 * AckTimeout (should be enough for any reasonable operation)
	maxAge := 2 * p.timeouts.AckTimeout

	for key, waiter := range p.pendingIndirect {
		select {
		case <-waiter.ctx.Done():
			// Context was cancelled, entry should be cleaned up
			toCleanup = append(toCleanup, key)
		default:
			// Check if this entry is too old (fallback cleanup)
			if now.Sub(waiter.createdTime) > maxAge {
				toCleanup = append(toCleanup, key)
			}
		}
	}

	// Remove the orphaned entries
	for _, key := range toCleanup {
		if waiter, exists := p.pendingIndirect[key]; exists {
			waiter.cancel() // Ensure context is cancelled
			delete(p.pendingIndirect, key)
		}
	}
	p.mu.Unlock()

	if len(toCleanup) > 0 {
		log.Printf("Cleaned up %d orphaned pendingIndirect entries", len(toCleanup))
	}
}

func (p *PingAckManager) checkFailures() {
	now := time.Now()
	var toRemove []*utils.Member
	var failureUpdates []*utils.Member

	p.membership.Lock()
	// First pass - collect all changes without modifying the map
	for id, member := range p.membership.Members {
		if id == p.membership.LocalNode.String() {
			continue
		}

		if member.Status == utils.Suspected {
			if now.Sub(member.SuspicionStart) > p.timeouts.SuspicionTimeout {
				toRemove = append(toRemove, cloneMember(member))
				failedUpdate := &utils.Member{ID: member.ID, Status: utils.Failed, Incarnation: member.Incarnation}
				failureUpdates = append(failureUpdates, failedUpdate)
			}
		}
	}

	// Second pass - apply all changes atomically (but add updates outside lock)
	for _, member := range toRemove {
		delete(p.membership.Members, member.ID.String())
	}

	p.membership.Unlock()

	// Add updates outside the lock to avoid deadlock
	for _, update := range failureUpdates {
		p.membership.AddRecentUpdateSafe(update)
	}

	for _, m := range toRemove {
		log.Printf("Declared %s as FAILED", m.ID)
	}
}

func cloneMember(m *utils.Member) *utils.Member {
	if m == nil {
		return nil
	}
	c := *m
	return &c
}

func (p *PingAckManager) handlePing(msg utils.Message, from *net.UDPAddr) {
	if !p.active {
		return
	}
	ack := utils.Message{
		Type:        utils.Ack,
		Sender:      p.membership.LocalNode,
		Target:      msg.Sender,
		SeqNum:      msg.SeqNum,
		Incarnation: p.membership.Incarnation,
	}
	if err := p.network.Send(ack, msg.Sender.Address()); err != nil {
		log.Printf("Failed to send ACK to %s: %v", msg.Sender, err)
	}

	// log.Printf("Received PING from %s (seq %d), sent ACK", msg.Sender, msg.SeqNum)

	p.membership.Lock()
	senderKey := msg.Sender.String()
	sender, exists := p.membership.Members[senderKey]

	if !exists {
		newMember := &utils.Member{
			ID:            msg.Sender,
			Incarnation:   msg.Incarnation,
			Status:        utils.Alive,
			LastHeartbeat: time.Now(),
		}
		p.membership.Members[senderKey] = newMember
		p.membership.AddRecentUpdate(newMember)
	} else {
		updated := false
		if msg.Incarnation > sender.Incarnation {
			sender.Incarnation = msg.Incarnation
			sender.Status = utils.Alive
			sender.LastHeartbeat = time.Now()
			sender.SuspicionStart = time.Time{}
			updated = true
		} else if msg.Incarnation == sender.Incarnation {
			sender.LastHeartbeat = time.Now()
			if sender.Status == utils.Suspected {
				sender.Status = utils.Alive
				sender.SuspicionStart = time.Time{}
				updated = true
			}
		}
		if updated {
			p.membership.AddRecentUpdate(sender)
		}
	}

	piggybacks := make([]utils.MemberUpdate, 0, len(msg.Members))
	piggybacks = append(piggybacks, msg.Members...)
	p.membership.Unlock()

	for _, update := range piggybacks {
		p.processUpdate(update, msg.Sender)
	}
}

func (p *PingAckManager) handleAck(msg utils.Message, from *net.UDPAddr) {
	if !p.active {
		return
	}
	p.membership.Lock()
	senderKey := msg.Sender.String()
	if m, ok := p.membership.Members[senderKey]; ok {
		updated := false
		if msg.Incarnation > m.Incarnation {
			m.Incarnation = msg.Incarnation
			m.Status = utils.Alive
			m.LastHeartbeat = time.Now()
			m.SuspicionStart = time.Time{}
			updated = true
		} else if msg.Incarnation == m.Incarnation {
			m.LastHeartbeat = time.Now()
			if m.Status == utils.Suspected {
				m.Status = utils.Alive
				m.SuspicionStart = time.Time{}
				updated = true
			}
		}
		if updated {
			p.membership.AddRecentUpdate(m)
		}
	} else {
		newM := &utils.Member{ID: msg.Sender, Incarnation: msg.Incarnation, Status: utils.Alive, LastHeartbeat: time.Now()}
		p.membership.Members[senderKey] = newM
		p.membership.AddRecentUpdate(newM)
	}
	p.membership.Unlock()

	p.suspicionMgr.ClearSuspect(msg.Sender, msg.Incarnation)

	p.mu.Lock()
	if pending, ok := p.pendingAcks[msg.SeqNum]; ok && pending.target.Equals(msg.Sender) {
		pending.directAck = true
		select {
		case pending.ackReceived <- true:
		default:
		}
	}
	p.mu.Unlock()

	indKey := fmt.Sprintf("%d|%s", msg.SeqNum, msg.Sender.String())
	p.mu.Lock()
	waiter, ok := p.pendingIndirect[indKey]
	p.mu.Unlock()
	if ok {
		select {
		case waiter.ch <- msg.Incarnation:
		default:
		}
	}
}

func (p *PingAckManager) handleIndirectPing(msg utils.Message, from *net.UDPAddr) {
	if !p.active {
		return
	}
	log.Printf("Received ping-req from %s for target %s (seq %d)", msg.Sender, msg.Target, msg.SeqNum)

	p.membership.Lock()
	reqKey := msg.Sender.String()
	if m, ok := p.membership.Members[reqKey]; ok {
		if msg.Incarnation >= m.Incarnation {
			m.LastHeartbeat = time.Now()
		}
	}
	piggybacks := make([]utils.MemberUpdate, 0, len(msg.Members))
	piggybacks = append(piggybacks, msg.Members...)
	p.membership.Unlock()

	key := fmt.Sprintf("%d|%s", msg.SeqNum, msg.Target.String())
	ctx, cancel := context.WithCancel(p.ctx)
	w := &indirectPingWaiter{
		requester:   msg.Sender,
		ch:          make(chan int32, 1),
		ctx:         ctx,
		cancel:      cancel,
		createdTime: time.Now(),
	}
	p.mu.Lock()
	p.pendingIndirect[key] = w
	p.mu.Unlock()

	for _, u := range piggybacks {
		p.processUpdate(u, msg.Sender)
	}

	updates := p.membership.GetRecentUpdates(5)
	ping := utils.Message{
		Type:        utils.Ping,
		Sender:      p.membership.LocalNode,
		Target:      msg.Target,
		Incarnation: p.membership.Incarnation,
		SeqNum:      msg.SeqNum,
		Members:     updates,
	}
	if err := p.network.Send(ping, msg.Target.Address()); err != nil {
		log.Printf("Failed to send proxy PING to %s: %v", msg.Target, err)
	}

	go func(waitKey string, waiter *indirectPingWaiter, cancelCtx context.Context) {
		t := time.NewTimer(p.timeouts.AckTimeout)
		defer t.Stop()
		defer func() {
			waiter.cancel() // Cancel the context when done
			p.mu.Lock()
			delete(p.pendingIndirect, waitKey)
			p.mu.Unlock()
		}()
		select {
		case receivedIncarnation := <-waiter.ch:
			indAck := utils.Message{
				Type:        utils.IndirectAck,
				Sender:      p.membership.LocalNode,
				Target:      msg.Target,
				Incarnation: receivedIncarnation,
				SeqNum:      msg.SeqNum,
			}
			if err := p.network.Send(indAck, waiter.requester.Address()); err != nil {
				log.Printf("Failed to forward indirect ACK to %s: %v", waiter.requester, err)
			} else {
				log.Printf("Forwarded indirect ACK for %s (inc: %d, seq: %d) to %s", msg.Target, receivedIncarnation, msg.SeqNum, waiter.requester)
			}
		case <-t.C:
			log.Printf("Proxy PING timeout for %s (seq %d); no indirect ACK sent", msg.Target, msg.SeqNum)
		case <-cancelCtx.Done():
			log.Printf("Proxy PING for %s (seq %d) cancelled", msg.Target, msg.SeqNum)
		}
	}(key, w, ctx)
}

func (p *PingAckManager) handleIndirectAck(msg utils.Message, from *net.UDPAddr) {
	if !p.active {
		return
	}
	log.Printf("Received indirect ACK from %s for target %s (seq %d, inc %d)", msg.Sender, msg.Target, msg.SeqNum, msg.Incarnation)

	p.mu.Lock()
	if pending, ok := p.pendingAcks[msg.SeqNum]; ok && pending.target.Equals(msg.Target) {
		pending.indirectAcks[msg.Sender.String()] = true
		select {
		case pending.ackReceived <- true:
		default:
		}
	}
	p.mu.Unlock()

	p.membership.Lock()
	targetKey := msg.Target.String()
	if m, ok := p.membership.Members[targetKey]; ok {
		if msg.Incarnation >= m.Incarnation {
			m.Incarnation = msg.Incarnation
			m.LastHeartbeat = time.Now()
			if m.Status != utils.Alive {
				m.Status = utils.Alive
				m.SuspicionStart = time.Time{}
				p.membership.AddRecentUpdate(m)
			}
		}
	}
	p.membership.Unlock()

	p.suspicionMgr.ClearSuspect(msg.Target, msg.Incarnation)
}

func (p *PingAckManager) handleJoin(msg utils.Message, from *net.UDPAddr) {
	if !p.active {
		return
	}
	p.membership.Lock()

	// Check for and remove any old entries with the same IP:Port but different timestamp
	joinerAddress := msg.Sender.Address()
	var oldEntriesToRemove []string
	for key, member := range p.membership.Members {
		if member.ID.Address() == joinerAddress && member.ID.Timestamp != msg.Sender.Timestamp {
			oldEntriesToRemove = append(oldEntriesToRemove, key)
		}
	}

	// Remove old entries before adding the new one (collect updates for later)
	var oldFailedUpdates []*utils.Member
	for _, key := range oldEntriesToRemove {
		if oldMember, exists := p.membership.Members[key]; exists {
			log.Printf("Removing old entry for rejoining node %s (old timestamp: %d, new timestamp: %d)",
				joinerAddress, oldMember.ID.Timestamp, msg.Sender.Timestamp)
			delete(p.membership.Members, key)
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
	p.membership.Members[memberKey] = newMember

	members := make([]utils.MemberUpdate, 0, len(p.membership.Members))
	for _, member := range p.membership.Members {
		members = append(members, utils.MemberUpdate{
			NodeID:      member.ID,
			Incarnation: member.Incarnation,
			Status:      member.Status,
			Timestamp:   time.Now(),
		})
	}
	p.membership.Unlock()

	// Add updates outside the lock to avoid deadlock
	for _, failedUpdate := range oldFailedUpdates {
		p.membership.AddRecentUpdateSafe(failedUpdate)
	}
	p.membership.AddRecentUpdateSafe(newMember)

	response := utils.Message{
		Type:        utils.JoinResponse,
		Sender:      p.membership.LocalNode,
		Incarnation: p.membership.Incarnation,
		Members:     members,
	}

	p.network.Send(response, msg.Sender.Address())
	log.Printf("New member joined: %s", msg.Sender)
}

func (p *PingAckManager) handleJoinResponse(msg utils.Message, from *net.UDPAddr) {
	if !p.active {
		return
	}
	clears := []utils.MemberUpdate{}

	p.membership.Lock()
	for _, update := range msg.Members {
		memberKey := update.NodeID.String()

		if memberKey == p.membership.LocalNode.String() {
			continue
		}

		member, exists := p.membership.Members[memberKey]
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
			p.membership.Members[memberKey] = newMember
			p.membership.AddRecentUpdate(newMember)
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
			p.membership.AddRecentUpdate(member)
			if update.Status == utils.Alive {
				clears = append(clears, update)
			}
		}
	}
	p.membership.Unlock()

	for _, u := range clears {
		p.suspicionMgr.ClearSuspect(u.NodeID, u.Incarnation)
	}

	log.Printf("Received membership list from %s, now have %d members", msg.Sender, len(p.membership.Members))
}

func (p *PingAckManager) handleAliveMessage(msg utils.Message, from *net.UDPAddr) {
	if !p.active {
		return
	}
	p.membership.Lock()
	memberKey := msg.Sender.String()
	member, exists := p.membership.Members[memberKey]

	if exists && msg.Incarnation >= member.Incarnation {
		if member.Status != utils.Alive || msg.Incarnation > member.Incarnation {
			member.Incarnation = msg.Incarnation
			member.Status = utils.Alive
			member.LastHeartbeat = time.Now()
			member.SuspicionStart = time.Time{}
			p.membership.AddRecentUpdate(member)
		}
	}
	p.membership.Unlock()

	p.suspicionMgr.ClearSuspect(msg.Sender, msg.Incarnation)
	if exists {
		log.Printf("Member %s refuted suspicion with incarnation %d", msg.Sender, msg.Incarnation)
	}
}

func (p *PingAckManager) handleSuspectMessage(msg utils.Message, from *net.UDPAddr) {
	if !p.enableSuspicion {
		return
	}
	if !p.active {
		return
	}
	p.suspicionMgr.ProcessSuspicion(msg.Sender, msg.Target, msg.Incarnation)
}

func (p *PingAckManager) handleLeave(msg utils.Message, from *net.UDPAddr) {
	p.membership.Lock()
	key := msg.Sender.String()
	if m, ok := p.membership.Members[key]; ok {
		delete(p.membership.Members, key)
		// record an update to piggyback removal
		failedUpdate := &utils.Member{ID: m.ID, Status: utils.Failed, Incarnation: m.Incarnation}
		p.membership.AddRecentUpdate(failedUpdate)
		log.Printf("Member left voluntarily: %s", msg.Sender)
	}
	p.membership.Unlock()
}

func (p *PingAckManager) JoinGroup(introducerAddr string) error {
	if introducerAddr == "" {
		return nil
	}

	// Reactivate the node when joining (handles rejoin after leave)
	p.active = true

	// Refresh local node with new timestamp and incarnation (for rejoining)
	p.membership.RefreshLocalNode()

	joinMsg := utils.Message{
		Type:        utils.Join,
		Sender:      p.membership.LocalNode,
		Incarnation: p.membership.Incarnation,
	}
	return p.network.Send(joinMsg, introducerAddr)
}

func (p *PingAckManager) LeaveGroup() {
	// broadcast Leave to all known peers
	p.membership.RLock()
	addrs := make([]string, 0, len(p.membership.Members))
	for _, m := range p.membership.Members {
		if m.ID.String() == p.membership.LocalNode.String() {
			continue
		}
		addrs = append(addrs, m.ID.Address())
	}
	self := p.membership.LocalNode
	p.membership.RUnlock()

	msg := utils.Message{Type: utils.Leave, Sender: self, Incarnation: p.membership.Incarnation}
	for _, addr := range addrs {
		_ = p.network.Send(msg, addr)
	}

	p.active = false
	log.Printf("Voluntarily left the group")
}
