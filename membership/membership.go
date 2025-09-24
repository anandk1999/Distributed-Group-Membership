package membership

import (
	"math/rand"
	"mp2-g02/types"
	. "mp2-g02/types"
	"sync"
	"time"
)

type MembershipList struct {
	sync.RWMutex
	LocalNode     NodeID
	Members       map[string]*Member
	Incarnation   int32
	UpdateHooks   []func(*Member, ChangeType)
	recentUpdates []MemberUpdate
	updateWindow  time.Duration
}

func NewMembershipList(localNode NodeID) *MembershipList {
	ml := &MembershipList{
		LocalNode:     localNode,
		Members:       make(map[string]*Member),
		Incarnation:   0,
		UpdateHooks:   []func(*Member, ChangeType){},
		recentUpdates: []MemberUpdate{},
		updateWindow:  5 * time.Second,
	}

	// Add self to membership
	ml.Members[localNode.String()] = &Member{
		ID:            localNode,
		Incarnation:   0,
		Status:        Alive,
		LastHeartbeat: time.Now(),
	}

	return ml
}

func (ml *MembershipList) AddMember(member *Member) {
	ml.Lock()
	defer ml.Unlock()

	existing, exists := ml.Members[member.ID.String()]
	if !exists || member.Incarnation > existing.Incarnation {
		ml.Members[member.ID.String()] = member
		ml.AddRecentUpdate(member)

		// Trigger hooks
		for _, hook := range ml.UpdateHooks {
			go hook(member, Joined)
		}
	}
}

func (ml *MembershipList) UpdateMember(nodeID string, status MemberStatus, incarnation int32) {
	ml.Lock()
	defer ml.Unlock()

	if member, exists := ml.Members[nodeID]; exists {
		if incarnation >= member.Incarnation {
			oldStatus := member.Status
			member.Status = status
			member.Incarnation = incarnation

			if status == Alive {
				member.LastHeartbeat = time.Now()
			} else if status == Suspected && oldStatus == Alive {
				member.SuspicionStart = time.Now()
			}

			ml.AddRecentUpdate(member)

			// Trigger hooks
			for _, hook := range ml.UpdateHooks {
				go hook(member, StatusChanged)
			}
		}
	}
}

func (ml *MembershipList) RemoveMember(nodeID string) {
	ml.Lock()
	defer ml.Unlock()

	if member, exists := ml.Members[nodeID]; exists {
		member.Status = Failed
		ml.AddRecentUpdate(member)
		delete(ml.Members, nodeID)

		// Trigger hooks
		for _, hook := range ml.UpdateHooks {
			go hook(member, FailureDetected)
		}
	}
}

func (ml *MembershipList) GetRandomMembers(n int, exclude []string) []*types.Member {
	ml.RLock()
	defer ml.RUnlock()

	var candidates []*types.Member
	excludeMap := make(map[string]bool)
	for _, e := range exclude {
		excludeMap[e] = true
	}

	for id, member := range ml.Members {
		if !excludeMap[id] && member.Status == types.Alive && id != ml.LocalNode.String() {
			candidates = append(candidates, member)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	if n > len(candidates) {
		n = len(candidates)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	return candidates[:n]
}

func (ml *MembershipList) GetRecentUpdates(limit int) []MemberUpdate {
	ml.Lock()
	defer ml.Unlock()

	// Clean old updates
	cutoff := time.Now().Add(-ml.updateWindow)
	var filtered []MemberUpdate
	for _, update := range ml.recentUpdates {
		if update.Timestamp.After(cutoff) {
			filtered = append(filtered, update)
		}
	}
	ml.recentUpdates = filtered

	if limit > len(filtered) {
		limit = len(filtered)
	}

	if len(filtered) == 0 {
		return nil
	}

	return filtered[:limit]
}

func (ml *MembershipList) AddRecentUpdate(member *Member) {
	update := MemberUpdate{
		NodeID:      member.ID,
		Incarnation: member.Incarnation,
		Status:      member.Status,
		Timestamp:   time.Now(),
	}
	ml.recentUpdates = append(ml.recentUpdates, update)
}

func (ml *MembershipList) GetAllMembers() []*Member {
	ml.RLock()
	defer ml.RUnlock()

	members := make([]*Member, 0, len(ml.Members))
	for _, member := range ml.Members {
		members = append(members, &Member{
			ID:            member.ID,
			Incarnation:   member.Incarnation,
			Status:        member.Status,
			LastHeartbeat: member.LastHeartbeat,
		})
	}
	return members
}

func (ml *MembershipList) IncrementIncarnation() {
	ml.Lock()
	defer ml.Unlock()
	ml.Incarnation++
}
