package suspicion

import (
	. "mp2-g02/membership"
	. "mp2-g02/network"
	. "mp2-g02/types"
	"time"
)

type SuspicionManager struct {
	membership   *MembershipList
	network      *NetworkLayer
	cleanupTime  time.Duration
	incarnations map[string]int32
}

func NewSuspicionManager(membership *MembershipList, network *NetworkLayer) *SuspicionManager {
	return &SuspicionManager{
		membership:   membership,
		network:      network,
		cleanupTime:  2 * time.Second,
		incarnations: make(map[string]int32),
	}
}

func (s *SuspicionManager) ProcessSuspicion(nodeID NodeID, incarnation int32) {
	s.membership.Lock()
	defer s.membership.Unlock()

	member := s.membership.Members[nodeID.String()]
	if member == nil {
		return
	}

	// Handle incarnation numbers
	if nodeID.String() == s.membership.LocalNode.String() {
		// Refute suspicion about self
		if incarnation >= s.membership.Incarnation {
			s.membership.Incarnation = incarnation + 1
			s.broadcastAlive()
		}
	} else {
		// Update member status based on incarnation
		if incarnation > member.Incarnation {
			member.Incarnation = incarnation
			member.Status = Suspected
			member.SuspicionStart = time.Now()
		}
	}
}

func (s *SuspicionManager) broadcastAlive() {
	msg := Message{
		Type:        AliveMsg,
		Sender:      s.membership.LocalNode,
		Incarnation: s.membership.Incarnation,
	}

	for _, member := range s.membership.Members {
		if member.ID.String() != s.membership.LocalNode.String() {
			s.network.Send(msg, member.ID.Address())
		}
	}
}

func (s *SuspicionManager) BroadcastSuspicion(member *Member) {
	msg := Message{
		Type:        Suspect,
		Sender:      s.membership.LocalNode,
		Target:      member.ID,
		Incarnation: member.Incarnation,
	}

	for _, m := range s.membership.Members {
		if m.ID.String() != s.membership.LocalNode.String() && m.ID.String() != member.ID.String() {
			s.network.Send(msg, m.ID.Address())
		}
	}
}

func (s *SuspicionManager) cleanupLoop() {
	ticker := time.NewTicker(s.cleanupTime)
	for range ticker.C {
		s.membership.Lock()
		now := time.Now()

		for id, member := range s.membership.Members {
			if member.Status == Suspected {
				if now.Sub(member.SuspicionStart) > s.cleanupTime {
					member.Status = Failed
					delete(s.membership.Members, id)
				}
			}
		}
		s.membership.Unlock()
	}
}
