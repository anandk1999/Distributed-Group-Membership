package suspicion

import (
	"context"
	"mp2-g02/membership"
	"mp2-g02/network"
	"mp2-g02/types"
	"sync"
	"time"
)

type Options struct {
	SuspicionTimeout   time.Duration
	CheckInterval      time.Duration
	RequireReports     int
	ConfirmedRetention time.Duration

	OnSuspect func(target types.NodeID, inc int32, reporters []types.NodeID)
	OnConfirm func(target types.NodeID, inc int32)
	OnClear   func(target types.NodeID, inc int32)
}
type suspectEntry struct {
	target       types.NodeID
	incarnation  int32
	reporters    map[types.NodeID]time.Time
	firstReport  time.Time
	confirmed    bool
	confirmedAt  time.Time
	cleanupAfter time.Time
}
type SuspicionManager struct {
	mu         sync.Mutex
	membership *membership.MembershipList
	network    *network.NetworkLayer
	opts       Options
	entries    map[string]*suspectEntry
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	started    bool
}

func NewSuspicionManager(m *membership.MembershipList, n *network.NetworkLayer, opts Options) *SuspicionManager {
	if opts.SuspicionTimeout == 0 {
		opts.SuspicionTimeout = 2500 * time.Millisecond
	}
	if opts.CheckInterval == 0 {
		opts.CheckInterval = 200 * time.Millisecond
	}
	if opts.RequireReports <= 0 {
		opts.RequireReports = 1
	}
	// default confirmed retention 30s if not set
	if opts.ConfirmedRetention == 0 {
		opts.ConfirmedRetention = 30 * time.Second
	}

	return &SuspicionManager{
		membership: m,
		network:    n,
		opts:       opts,
		entries:    make(map[string]*suspectEntry),
	}
}

func (sm *SuspicionManager) Start(parent context.Context) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.started {
		return
	}
	sm.ctx, sm.cancel = context.WithCancel(parent)
	sm.wg.Add(1)
	go sm.loop()
	sm.started = true
}

func (sm *SuspicionManager) Stop() {
	sm.mu.Lock()
	if !sm.started {
		sm.mu.Unlock()
		return
	}
	sm.cancel()
	sm.mu.Unlock()
	sm.wg.Wait()
	sm.mu.Lock()
	sm.started = false
	sm.mu.Unlock()
}

func (sm *SuspicionManager) ProcessSuspicion(reporter types.NodeID, target types.NodeID, inc int32) {
	now := time.Now()
	key := target.String()

	if target.String() == sm.membership.LocalNode.String() {
		sm.handleSelfRefutation(inc)
		return
	}

	sm.mu.Lock()
	e, ok := sm.entries[key]
	if ok {
		if inc < e.incarnation {
			sm.mu.Unlock()
			return
		}
		if inc > e.incarnation {
			e = &suspectEntry{
				target:      target,
				incarnation: inc,
				reporters:   make(map[types.NodeID]time.Time),
			}
			sm.entries[key] = e
		}
	} else {
		sm.mu.Unlock()
		sm.membership.Lock()
		member := sm.membership.Members[target.String()]
		localInc := int32(0)
		if member != nil {
			localInc = member.Incarnation
		}
		sm.membership.Unlock()
		if inc < localInc {
			return
		}
		sm.mu.Lock()
		e = &suspectEntry{
			target:      target,
			incarnation: inc,
			reporters:   make(map[types.NodeID]time.Time),
		}
		sm.entries[key] = e
	}

	e.reporters[reporter] = now

	if len(e.reporters) >= sm.opts.RequireReports && e.firstReport.IsZero() {
		e.firstReport = now

		sm.mu.Unlock()
		sm.membership.Lock()
		m := sm.membership.Members[target.String()]
		if m != nil {
			m.Status = types.Suspected
			m.SuspicionStart = e.firstReport
		}
		sm.membership.Unlock()

		if sm.opts.OnSuspect != nil {
			go sm.opts.OnSuspect(target, inc, sm.reportersList(e))
		}
		return
	}
	sm.mu.Unlock()
}

func (sm *SuspicionManager) ClearSuspect(target types.NodeID, inc int32) {
	key := target.String()

	sm.mu.Lock()
	e, ok := sm.entries[key]
	if !ok {
		sm.mu.Unlock()
		sm.membership.Lock()
		m := sm.membership.Members[key]
		if m != nil {
			if inc >= m.Incarnation {
				m.Status = types.Alive
				m.Incarnation = inc
			}
		}
		sm.membership.Unlock()
		return
	}
	if inc < e.incarnation {
		sm.mu.Unlock()
		return
	}
	delete(sm.entries, key)
	sm.mu.Unlock()

	sm.membership.Lock()
	if m := sm.membership.Members[key]; m != nil {
		if inc >= m.Incarnation {
			m.Status = types.Alive
			m.Incarnation = inc
		}
	}
	sm.membership.Unlock()

	if sm.opts.OnClear != nil {
		go sm.opts.OnClear(target, inc)
	}
}

func (sm *SuspicionManager) handleSelfRefutation(incomingInc int32) {
	sm.membership.Lock()
	localInc := sm.membership.Incarnation
	newInc := localInc
	if incomingInc >= newInc {
		newInc = incomingInc + 1
	} else {
		newInc = localInc + 1
	}
	sm.membership.Incarnation = newInc
	sm.membership.Unlock()

	msg := types.Message{
		Type:        types.AliveMsg,
		Sender:      sm.membership.LocalNode,
		Incarnation: newInc,
	}

	sm.membership.Lock()
	recipients := make([]string, 0, len(sm.membership.Members))
	for _, m := range sm.membership.Members {
		if m.ID.String() == sm.membership.LocalNode.String() {
			continue
		}
		recipients = append(recipients, m.ID.Address())
	}
	sm.membership.Unlock()

	for _, addr := range recipients {
		sm.network.Send(msg, addr)
	}
}

func (sm *SuspicionManager) loop() {
	defer sm.wg.Done()

	t := time.NewTicker(sm.opts.CheckInterval)
	defer t.Stop()

	for {
		select {
		case <-sm.ctx.Done():
			return
		case now := <-t.C:
			sm.check(now)
		}
	}
}

func (sm *SuspicionManager) check(now time.Time) {
	type confirmAction struct {
		target types.NodeID
		inc    int32
	}

	var toConfirm []confirmAction
	var toCleanup []string

	sm.mu.Lock()
	for key, e := range sm.entries {
		if e.firstReport.IsZero() {
			continue
		}
		if e.confirmed {
			if sm.opts.ConfirmedRetention > 0 && !e.cleanupAfter.IsZero() && now.After(e.cleanupAfter) {
				toCleanup = append(toCleanup, key)
			}
			continue
		}
		if now.Sub(e.firstReport) >= sm.opts.SuspicionTimeout {
			e.confirmed = true
			e.confirmedAt = now
			if sm.opts.ConfirmedRetention > 0 {
				e.cleanupAfter = now.Add(sm.opts.ConfirmedRetention)
			}
			toConfirm = append(toConfirm, confirmAction{target: e.target, inc: e.incarnation})
		}
	}
	for _, k := range toCleanup {
		delete(sm.entries, k)
	}
	sm.mu.Unlock()

	for _, c := range toConfirm {
		sm.membership.Lock()
		if m := sm.membership.Members[c.target.String()]; m != nil {
			m.Status = types.Failed
		}
		sm.membership.Unlock()

		if sm.opts.OnConfirm != nil {
			go sm.opts.OnConfirm(c.target, c.inc)
		}
	}
}

func (sm *SuspicionManager) reportersList(e *suspectEntry) []types.NodeID {
	out := make([]types.NodeID, 0, len(e.reporters))
	for r := range e.reporters {
		out = append(out, r)
	}
	return out
}
