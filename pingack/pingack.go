package pingack

import (
	"fmt"
	"log"
	"mp2-g02/membership"
	"mp2-g02/network"
	"mp2-g02/suspicion"
	"mp2-g02/types"
	"net"
	"sync"
	"time"
)

type pendingPing struct {
	target       types.NodeID
	seqNum       uint64
	startTime    time.Time
	directAck    bool
	indirectAcks map[string]bool
	ackReceived  chan bool
}

type PingAckManager struct {
	membership      *membership.MembershipList
	network         *network.NetworkLayer
	protocolPeriod  time.Duration
	ackTimeout      time.Duration
	k               int
	suspicionTime   time.Duration
	failureTime     time.Duration
	stopped         chan bool
	suspicionMgr    *suspicion.SuspicionManager
	seqNum          uint64
	pendingAcks     map[uint64]*pendingPing
	pendingIndirect map[string]*struct {
		requester types.NodeID
		ch        chan int32
	}
	enableSuspicion bool
	mu              sync.Mutex
	active          bool
}

func NewPingAckManager(ml *membership.MembershipList, net *network.NetworkLayer, suspicionMgr *suspicion.SuspicionManager) *PingAckManager {
	return &PingAckManager{
		membership:     ml,
		network:        net,
		protocolPeriod: 2 * time.Second,
		ackTimeout:     500 * time.Millisecond,
		k:              5,
		suspicionTime:  1 * time.Second,
		failureTime:    2 * time.Second,
		stopped:        make(chan bool),
		suspicionMgr:   suspicionMgr,
		seqNum:         0,
		pendingAcks:    make(map[uint64]*pendingPing),
		pendingIndirect: make(map[string]*struct {
			requester types.NodeID
			ch        chan int32
		}),
		enableSuspicion: true,
		active:          true,
	}
}

func (p *PingAckManager) Start() {
	p.network.RegisterHandler(types.Ping, p.handlePing)
	p.network.RegisterHandler(types.Ack, p.handleAck)
	p.network.RegisterHandler(types.IndirectPing, p.handleIndirectPing)
	p.network.RegisterHandler(types.IndirectAck, p.handleIndirectAck)
	p.network.RegisterHandler(types.Join, p.handleJoin)
	p.network.RegisterHandler(types.JoinResponse, p.handleJoinResponse)
	p.network.RegisterHandler(types.AliveMsg, p.handleAliveMessage)
	p.network.RegisterHandler(types.Suspect, p.handleSuspectMessage)
	p.network.RegisterHandler(types.Leave, p.handleLeave)

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

func (p *PingAckManager) SuspicionEnabled() bool {
	return p.enableSuspicion
}

func (p *PingAckManager) pingLoop() {
	ticker := time.NewTicker(p.protocolPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopped:
			return
		case <-ticker.C:
			if p.active {
				p.performSWIMProtocolPeriod()
			}
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

	pingMsg := types.Message{
		Type:        types.Ping,
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

	directAckTimer := time.NewTimer(p.ackTimeout)
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
		pingReqMsg := types.Message{
			Type:        types.IndirectPing,
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

	remainingTimeout := p.protocolPeriod - p.ackTimeout
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

func (p *PingAckManager) declareSuspicion(nodeID types.NodeID) {
	if p.enableSuspicion {
		p.membership.Lock()
		memberKey := nodeID.String()
		member, exists := p.membership.Members[memberKey]
		if !exists {
			// Create an entry as suspected so the rest of the system can converge
			member = &types.Member{ID: nodeID, Status: types.Suspected, LastHeartbeat: time.Now(), SuspicionStart: time.Now()}
			p.membership.Members[memberKey] = member
			p.membership.AddRecentUpdate(member)
		} else {
			// Transition to suspected if not already
			if member.Status != types.Suspected {
				member.Status = types.Suspected
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
		failedUpdate := &types.Member{ID: member.ID, Status: types.Failed, Incarnation: member.Incarnation}
		p.membership.AddRecentUpdate(failedUpdate)
		delete(p.membership.Members, memberKey)
		log.Printf("Declared %s as FAILED", nodeID)
	}
}

func (p *PingAckManager) processUpdate(update types.MemberUpdate, reporter types.NodeID) {
	memberKey := update.NodeID.String()

	if memberKey == p.membership.LocalNode.String() {
		if update.Status == types.Suspected && p.enableSuspicion {
			p.suspicionMgr.ProcessSuspicion(reporter, update.NodeID, update.Incarnation)
		} else if update.Status == types.Alive {
			p.suspicionMgr.ClearSuspect(update.NodeID, update.Incarnation)
		}
		return
	}

	p.membership.Lock()
	member, exists := p.membership.Members[memberKey]

	if !exists {
		newMember := &types.Member{
			ID:            update.NodeID,
			Incarnation:   update.Incarnation,
			Status:        update.Status,
			LastHeartbeat: time.Now(),
		}

		if update.Status == types.Suspected {
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
			if update.Status == types.Alive {
				member.LastHeartbeat = time.Now()
				member.SuspicionStart = time.Time{}
			} else if update.Status == types.Suspected && p.enableSuspicion {
				member.SuspicionStart = time.Now()
			}
			p.membership.AddRecentUpdate(member)
		} else if update.Incarnation == member.Incarnation {
			if update.Status == types.Alive && member.Status == types.Suspected {
				member.Status = types.Alive
				member.LastHeartbeat = time.Now()
				member.SuspicionStart = time.Time{}
				p.membership.AddRecentUpdate(member)
			} else if update.Status == types.Suspected && member.Status == types.Alive {
				member.Status = types.Suspected
				member.SuspicionStart = time.Now()
				p.membership.AddRecentUpdate(member)
			}
		}
	}
	p.membership.Unlock()

	if update.Status == types.Suspected && p.enableSuspicion {
		p.suspicionMgr.ProcessSuspicion(reporter, update.NodeID, update.Incarnation)
	}
	if update.Status == types.Alive {
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
	now := time.Now()
	var toRemove []*types.Member

	p.membership.Lock()
	for id, member := range p.membership.Members {
		if id == p.membership.LocalNode.String() {
			continue
		}

		if member.Status == types.Suspected {
			if now.Sub(member.SuspicionStart) > p.failureTime {
				toRemove = append(toRemove, cloneMember(member))
				delete(p.membership.Members, id)
				failedUpdate := &types.Member{ID: member.ID, Status: types.Failed, Incarnation: member.Incarnation}
				p.membership.AddRecentUpdate(failedUpdate)
			}
		}
	}
	p.membership.Unlock()

	for _, m := range toRemove {
		log.Printf("Declared %s as FAILED", m.ID)
	}
}

func cloneMember(m *types.Member) *types.Member {
	if m == nil {
		return nil
	}
	c := *m
	return &c
}

func (p *PingAckManager) JoinGroup(introducerAddr string) error {
	if introducerAddr == "" {
		return nil
	}
	joinMsg := types.Message{
		Type:        types.Join,
		Sender:      p.membership.LocalNode,
		Incarnation: p.membership.Incarnation,
	}
	return p.network.Send(joinMsg, introducerAddr)
}

func (p *PingAckManager) handlePing(msg types.Message, from *net.UDPAddr) {
	if !p.active {
		return
	}
	ack := types.Message{
		Type:        types.Ack,
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
		newMember := &types.Member{
			ID:            msg.Sender,
			Incarnation:   msg.Incarnation,
			Status:        types.Alive,
			LastHeartbeat: time.Now(),
		}
		p.membership.Members[senderKey] = newMember
		p.membership.AddRecentUpdate(newMember)
	} else {
		updated := false
		if msg.Incarnation > sender.Incarnation {
			sender.Incarnation = msg.Incarnation
			sender.Status = types.Alive
			sender.LastHeartbeat = time.Now()
			sender.SuspicionStart = time.Time{}
			updated = true
		} else if msg.Incarnation == sender.Incarnation {
			sender.LastHeartbeat = time.Now()
			if sender.Status == types.Suspected {
				sender.Status = types.Alive
				sender.SuspicionStart = time.Time{}
				updated = true
			}
		}
		if updated {
			p.membership.AddRecentUpdate(sender)
		}
	}

	piggybacks := make([]types.MemberUpdate, 0, len(msg.Members))
	piggybacks = append(piggybacks, msg.Members...)
	p.membership.Unlock()

	for _, update := range piggybacks {
		p.processUpdate(update, msg.Sender)
	}
}

func (p *PingAckManager) handleAck(msg types.Message, from *net.UDPAddr) {
	if !p.active {
		return
	}
	p.membership.Lock()
	senderKey := msg.Sender.String()
	if m, ok := p.membership.Members[senderKey]; ok {
		updated := false
		if msg.Incarnation > m.Incarnation {
			m.Incarnation = msg.Incarnation
			m.Status = types.Alive
			m.LastHeartbeat = time.Now()
			m.SuspicionStart = time.Time{}
			updated = true
		} else if msg.Incarnation == m.Incarnation {
			m.LastHeartbeat = time.Now()
			if m.Status == types.Suspected {
				m.Status = types.Alive
				m.SuspicionStart = time.Time{}
				updated = true
			}
		}
		if updated {
			p.membership.AddRecentUpdate(m)
		}
	} else {
		newM := &types.Member{ID: msg.Sender, Incarnation: msg.Incarnation, Status: types.Alive, LastHeartbeat: time.Now()}
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

func (p *PingAckManager) handleIndirectPing(msg types.Message, from *net.UDPAddr) {
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
	piggybacks := make([]types.MemberUpdate, 0, len(msg.Members))
	piggybacks = append(piggybacks, msg.Members...)
	p.membership.Unlock()

	key := fmt.Sprintf("%d|%s", msg.SeqNum, msg.Target.String())
	w := &struct {
		requester types.NodeID
		ch        chan int32
	}{requester: msg.Sender, ch: make(chan int32, 1)}
	p.mu.Lock()
	p.pendingIndirect[key] = w
	p.mu.Unlock()

	for _, u := range piggybacks {
		p.processUpdate(u, msg.Sender)
	}

	updates := p.membership.GetRecentUpdates(5)
	ping := types.Message{
		Type:        types.Ping,
		Sender:      p.membership.LocalNode,
		Target:      msg.Target,
		Incarnation: p.membership.Incarnation,
		SeqNum:      msg.SeqNum,
		Members:     updates,
	}
	if err := p.network.Send(ping, msg.Target.Address()); err != nil {
		log.Printf("Failed to send proxy PING to %s: %v", msg.Target, err)
	}

	go func(waitKey string, waiter *struct {
		requester types.NodeID
		ch        chan int32
	}) {
		t := time.NewTimer(p.ackTimeout)
		defer t.Stop()
		defer func() {
			p.mu.Lock()
			delete(p.pendingIndirect, waitKey)
			p.mu.Unlock()
		}()
		select {
		case receivedIncarnation := <-waiter.ch:
			indAck := types.Message{
				Type:        types.IndirectAck,
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
		}
	}(key, w)
}

func (p *PingAckManager) handleIndirectAck(msg types.Message, from *net.UDPAddr) {
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
			if m.Status != types.Alive {
				m.Status = types.Alive
				m.SuspicionStart = time.Time{}
				p.membership.AddRecentUpdate(m)
			}
		}
	}
	p.membership.Unlock()

	p.suspicionMgr.ClearSuspect(msg.Target, msg.Incarnation)
}

func (p *PingAckManager) handleJoin(msg types.Message, from *net.UDPAddr) {
	if !p.active {
		return
	}
	p.membership.Lock()

	newMember := &types.Member{
		ID:            msg.Sender,
		Incarnation:   msg.Incarnation,
		Status:        types.Alive,
		LastHeartbeat: time.Now(),
	}
	memberKey := msg.Sender.String()
	p.membership.Members[memberKey] = newMember
	p.membership.AddRecentUpdate(newMember)

	members := make([]types.MemberUpdate, 0, len(p.membership.Members))
	for _, member := range p.membership.Members {
		members = append(members, types.MemberUpdate{
			NodeID:      member.ID,
			Incarnation: member.Incarnation,
			Status:      member.Status,
			Timestamp:   time.Now(),
		})
	}
	p.membership.Unlock()

	response := types.Message{
		Type:        types.JoinResponse,
		Sender:      p.membership.LocalNode,
		Incarnation: p.membership.Incarnation,
		Members:     members,
	}

	p.network.Send(response, msg.Sender.Address())
	log.Printf("New member joined: %s", msg.Sender)
}

func (p *PingAckManager) handleJoinResponse(msg types.Message, from *net.UDPAddr) {
	if !p.active {
		return
	}
	clears := []types.MemberUpdate{}

	p.membership.Lock()
	for _, update := range msg.Members {
		memberKey := update.NodeID.String()

		if memberKey == p.membership.LocalNode.String() {
			continue
		}

		member, exists := p.membership.Members[memberKey]
		if !exists {
			newMember := &types.Member{
				ID:            update.NodeID,
				Incarnation:   update.Incarnation,
				Status:        update.Status,
				LastHeartbeat: time.Now(),
			}
			if update.Status == types.Suspected {
				newMember.SuspicionStart = time.Now()
			}
			p.membership.Members[memberKey] = newMember
			p.membership.AddRecentUpdate(newMember)
			if update.Status == types.Alive {
				clears = append(clears, update)
			}
		} else if update.Incarnation > member.Incarnation {
			member.Incarnation = update.Incarnation
			member.Status = update.Status
			member.LastHeartbeat = time.Now()
			if update.Status == types.Suspected {
				member.SuspicionStart = time.Now()
			} else if update.Status == types.Alive {
				member.SuspicionStart = time.Time{}
			}
			p.membership.AddRecentUpdate(member)
			if update.Status == types.Alive {
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

func (p *PingAckManager) handleAliveMessage(msg types.Message, from *net.UDPAddr) {
	if !p.active {
		return
	}
	p.membership.Lock()
	memberKey := msg.Sender.String()
	member, exists := p.membership.Members[memberKey]

	if exists && msg.Incarnation >= member.Incarnation {
		if member.Status != types.Alive || msg.Incarnation > member.Incarnation {
			member.Incarnation = msg.Incarnation
			member.Status = types.Alive
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

func (p *PingAckManager) handleSuspectMessage(msg types.Message, from *net.UDPAddr) {
	if !p.enableSuspicion {
		return
	}
	if !p.active {
		return
	}
	p.suspicionMgr.ProcessSuspicion(msg.Sender, msg.Target, msg.Incarnation)
}

func (p *PingAckManager) handleLeave(msg types.Message, from *net.UDPAddr) {
	p.membership.Lock()
	key := msg.Sender.String()
	if m, ok := p.membership.Members[key]; ok {
		delete(p.membership.Members, key)
		// record an update to piggyback removal
		failedUpdate := &types.Member{ID: m.ID, Status: types.Failed, Incarnation: m.Incarnation}
		p.membership.AddRecentUpdate(failedUpdate)
		log.Printf("Member left voluntarily: %s", msg.Sender)
	}
	p.membership.Unlock()
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

	msg := types.Message{Type: types.Leave, Sender: self, Incarnation: p.membership.Incarnation}
	for _, addr := range addrs {
		_ = p.network.Send(msg, addr)
	}

	p.active = false
	log.Printf("Voluntarily left the group")
}
