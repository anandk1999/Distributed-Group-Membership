package suspicion

import (
	"context"
	"fmt"
	"sync"
	"time"

	"mp2-g02/membership"
	"mp2-g02/network"
	"mp2-g02/types"
)

// Options controls suspicion manager behavior.
type Options struct {
	// How long to wait after firstReport before promoting suspect -> confirmed
	SuspicionTimeout time.Duration

	// Polling interval for checking deadlines
	CheckInterval time.Duration

	// How many distinct reporters are required to start the suspicion countdown
	RequireReports int

	// How long to retain confirmed entries in the local suspicion table
	ConfirmedRetention time.Duration

	// Callbacks (may be nil)
	OnSuspect func(target types.NodeID, inc int32, reporters []types.NodeID)
	OnConfirm func(target types.NodeID, inc int32)
	OnClear   func(target types.NodeID, inc int32)
}

// suspectEntry tracks reporters + state for a single node.
type suspectEntry struct {
	target      types.NodeID
	incarnation int32

	reporters   map[types.NodeID]time.Time // reporter.String() -> when reported
	firstReport time.Time                  // when the required # of reporters was reached

	confirmed    bool
	confirmedAt  time.Time
	cleanupAfter time.Time
}

// SuspicionManager provides a shared suspicion mechanism used by gossip and SWIM logic.
type SuspicionManager struct {
	mu sync.Mutex

	membership *membership.MembershipList
	network    *network.NetworkLayer
	opts       Options

	entries map[string]*suspectEntry // key: target.String()

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
}

// NewSuspicionManager constructs a manager with defaults for any zero option.
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

// Start begins the background loop; ctx is parent context (can be context.Background()).
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

// Stop cancels background loops and waits.
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

// ProcessSuspicion is called when we receive a SUSPECT message from `reporter` about `target` at `inc`.
// NOTE: reporter must be provided (the sender of the message) so we can require multiple reporters.
func (sm *SuspicionManager) ProcessSuspicion(reporter types.NodeID, target types.NodeID, inc int32) {
	now := time.Now()
	key := target.String()

	// special-case: if the target is ourselves, do local refutation by bumping our incarnation
	if target.String() == sm.membership.LocalNode.String() {
		sm.handleSelfRefutation(inc)
		return
	}

	sm.mu.Lock()
	e, ok := sm.entries[key]
	if ok {
		// ignore reports for an older incarnation than we already track/know
		if inc < e.incarnation {
			sm.mu.Unlock()
			return
		}
		// if inc > tracked incarnation, reset the entry
		if inc > e.incarnation {
			e = &suspectEntry{
				target:      target,
				incarnation: inc,
				reporters:   make(map[types.NodeID]time.Time),
			}
			sm.entries[key] = e
		}
	} else {
		// If membership list has a higher incarnation recorded, ignore strictly older reports
		sm.mu.Unlock()
		sm.membership.Lock()
		member := sm.membership.Members[target.String()]
		localInc := int32(0)
		if member != nil {
			localInc = member.Incarnation
		}
		sm.membership.Unlock()
		if inc < localInc {
			// report about an older incarnation than we know -> ignore
			return
		}
		sm.mu.Lock()
		// create entry
		e = &suspectEntry{
			target:      target,
			incarnation: inc,
			reporters:   make(map[types.NodeID]time.Time),
		}
		sm.entries[key] = e
	}

	// record reporter
	e.reporters[reporter] = now

	// if we have now reached the required number of distinct reporters and firstReport not set
	if len(e.reporters) >= sm.opts.RequireReports && e.firstReport.IsZero() {
		e.firstReport = now

		// update membership status to Suspected (under membership lock)
		sm.mu.Unlock()
		sm.membership.Lock()
		m := sm.membership.Members[target.String()]
		if m != nil {
			m.Status = types.Suspected
			m.SuspicionStart = e.firstReport
		}
		sm.membership.Unlock()

		// call callback (non-blocking / recover)
		go sm.safeOnSuspect(target, inc, sm.reportersList(e))
		return
	}
	sm.mu.Unlock()
}

// ClearSuspect clears any suspicion for `target` when we observe liveness or a higher incarnation.
func (sm *SuspicionManager) ClearSuspect(target types.NodeID, inc int32) {
	key := target.String()
	now := time.Now()

	sm.mu.Lock()
	e, ok := sm.entries[key]
	if !ok {
		sm.mu.Unlock()
		// nothing tracked; still update membership if necessary
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

	// only clear if incoming incarnation >= tracked incarnation (or is a higher incarnation)
	if inc < e.incarnation {
		sm.mu.Unlock()
		return
	}

	// remove entry
	delete(sm.entries, key)
	sm.mu.Unlock()

	// update membership under membership lock
	sm.membership.Lock()
	if m := sm.membership.Members[key]; m != nil {
		if inc >= m.Incarnation {
			m.Status = types.Alive
			m.Incarnation = inc
		}
	}
	sm.membership.Unlock()

	// callback
	go sm.safeOnClear(target, inc, now)
}

// handleSelfRefutation increments local incarnation (to be > incoming) and broadcasts an Alive message,
// but crucially we do the network send outside any heavy-held locks.
func (sm *SuspicionManager) handleSelfRefutation(incomingInc int32) {
	// read then set local incarnation under membership lock
	sm.membership.Lock()
	localInc := sm.membership.Incarnation
	// we must ensure our incarnation becomes strictly greater than incomingInc and localInc
	newInc := localInc
	if incomingInc >= newInc {
		newInc = incomingInc + 1
	} else {
		newInc = localInc + 1
	}
	sm.membership.Incarnation = newInc
	sm.membership.Unlock()

	// build Alive message and perform sends WITHOUT holding membership lock
	msg := types.Message{
		Type:        types.AliveMsg,
		Sender:      sm.membership.LocalNode,
		Incarnation: newInc,
	}

	// gather recipient addresses under membership lock quickly
	sm.membership.Lock()
	recips := make([]string, 0, len(sm.membership.Members))
	for _, m := range sm.membership.Members {
		// skip self
		if m.ID.String() == sm.membership.LocalNode.String() {
			continue
		}
		recips = append(recips, m.ID.Address())
	}
	sm.membership.Unlock()

	// send outside lock
	for _, addr := range recips {
		// ignoring send errors here; network.Send should be non-blocking in your implementation
		sm.network.Send(msg, addr)
	}
}

// loop runs periodically to promote suspects -> confirmed when their timeout elapses.
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

// check scans entries and promotes any timed-out suspect to confirmed.
func (sm *SuspicionManager) check(now time.Time) {
	// Collect confirmations to act on (do non-lock work later)
	type confirmAction struct {
		target types.NodeID
		inc    int32
	}

	var toConfirm []confirmAction
	var toCleanup []string

	sm.mu.Lock()
	for key, e := range sm.entries {
		// skip entries that haven't started (no firstReport yet)
		if e.firstReport.IsZero() {
			continue
		}
		// If already confirmed, schedule cleanup after retention
		if e.confirmed {
			if sm.opts.ConfirmedRetention > 0 && !e.cleanupAfter.IsZero() && now.After(e.cleanupAfter) {
				toCleanup = append(toCleanup, key)
			}
			continue
		}
		// check suspicion timeout
		if now.Sub(e.firstReport) >= sm.opts.SuspicionTimeout {
			// promote to confirmed
			e.confirmed = true
			e.confirmedAt = now
			if sm.opts.ConfirmedRetention > 0 {
				e.cleanupAfter = now.Add(sm.opts.ConfirmedRetention)
			}
			toConfirm = append(toConfirm, confirmAction{target: e.target, inc: e.incarnation})
		}
	}
	// delete cleanup candidates
	for _, k := range toCleanup {
		delete(sm.entries, k)
	}
	sm.mu.Unlock()

	// For each confirmation: update membership to Failed and call OnConfirm (also allow broadcasting outside locks)
	for _, c := range toConfirm {
		// update membership under its lock
		sm.membership.Lock()
		if m := sm.membership.Members[c.target.String()]; m != nil {
			m.Status = types.Failed
			// keep Incarnation as-is: it's the incarnation we confirmed on
		}
		sm.membership.Unlock()

		// callback to allow system to broadcast CONFIRM
		go sm.safeOnConfirm(c.target, c.inc)
	}
}

// reportersList returns a slice of NodeID reporters snapshot from entry (caller must hold sm.mu or be OK with concurrent reads).
func (sm *SuspicionManager) reportersList(e *suspectEntry) []types.NodeID {
	out := make([]types.NodeID, 0, len(e.reporters))
	for r := range e.reporters {
		out = append(out, r)
	}
	return out
}

// safeOnSuspect calls OnSuspect safely.
func (sm *SuspicionManager) safeOnSuspect(target types.NodeID, inc int32, reporters []types.NodeID) {
	if sm.opts.OnSuspect == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("panic in OnSuspect callback: %v\n", r)
		}
	}()
	sm.opts.OnSuspect(target, inc, reporters)
}

// safeOnConfirm calls OnConfirm safely.
func (sm *SuspicionManager) safeOnConfirm(target types.NodeID, inc int32) {
	if sm.opts.OnConfirm == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("panic in OnConfirm callback: %v\n", r)
		}
	}()
	sm.opts.OnConfirm(target, inc)
}

// safeOnClear calls OnClear safely.
func (sm *SuspicionManager) safeOnClear(target types.NodeID, inc int32, _ time.Time) {
	if sm.opts.OnClear == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("panic in OnClear callback: %v\n", r)
		}
	}()
	sm.opts.OnClear(target, inc)
}
