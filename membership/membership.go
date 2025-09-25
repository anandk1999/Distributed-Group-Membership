package membership

import (
	"math/rand"
	"mp2-g02/types"
	"sync"
	"time"
)

type MembershipList struct {
	sync.RWMutex
	LocalNode     types.NodeID
	Members       map[string]*types.Member
	Incarnation   int32
	recentUpdates []types.MemberUpdate
	updateWindow  time.Duration
}

func NewMembershipList(localNode types.NodeID) *MembershipList {
	ml := &MembershipList{
		LocalNode:     localNode,
		Members:       make(map[string]*types.Member),
		Incarnation:   0,
		recentUpdates: []types.MemberUpdate{},
		updateWindow:  5 * time.Second,
	}

	ml.Members[localNode.String()] = &types.Member{
		ID:            localNode,
		Incarnation:   0,
		Status:        types.Alive,
		LastHeartbeat: time.Now(),
	}

	return ml
}

func (ml *MembershipList) AddMember(member *types.Member) {
	ml.Lock()
	defer ml.Unlock()

	existing, exists := ml.Members[member.ID.String()]
	if !exists || member.Incarnation > existing.Incarnation {
		ml.Members[member.ID.String()] = member
		ml.AddRecentUpdate(member)
	}
}

func (ml *MembershipList) UpdateMember(nodeID string, status types.MemberStatus, incarnation int32) {
	ml.Lock()
	defer ml.Unlock()

	if member, exists := ml.Members[nodeID]; exists {
		if incarnation >= member.Incarnation {
			oldStatus := member.Status
			member.Status = status
			member.Incarnation = incarnation

			if status == types.Alive {
				member.LastHeartbeat = time.Now()
			} else if status == types.Suspected && oldStatus == types.Alive {
				member.SuspicionStart = time.Now()
			}

			ml.AddRecentUpdate(member)
		}
	}
}

func (ml *MembershipList) RemoveMember(nodeID string) {
	ml.Lock()
	defer ml.Unlock()

	if member, exists := ml.Members[nodeID]; exists {
		member.Status = types.Failed
		ml.AddRecentUpdate(member)
		delete(ml.Members, nodeID)
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

func (ml *MembershipList) GetRecentUpdates(limit int) []types.MemberUpdate {
	ml.Lock()
	defer ml.Unlock()

	cutoff := time.Now().Add(-ml.updateWindow)
	var filtered []types.MemberUpdate
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

func (ml *MembershipList) AddRecentUpdate(member *types.Member) {
	update := types.MemberUpdate{
		NodeID:      member.ID,
		Incarnation: member.Incarnation,
		Status:      member.Status,
		Timestamp:   time.Now(),
	}
	ml.recentUpdates = append(ml.recentUpdates, update)
}

func (ml *MembershipList) GetAllMembers() []*types.Member {
	ml.RLock()
	defer ml.RUnlock()

	members := make([]*types.Member, 0, len(ml.Members))
	for _, member := range ml.Members {
		members = append(members, &types.Member{
			ID:            member.ID,
			Incarnation:   member.Incarnation,
			Status:        member.Status,
			LastHeartbeat: member.LastHeartbeat,
		})
	}
	return members
}

func (ml *MembershipList) GetSuspectedMembers() []*types.Member {
	ml.RLock()
	defer ml.RUnlock()

	var suspects []*types.Member
	for _, m := range ml.Members {
		if m.Status == types.Suspected {
			suspects = append(suspects, &types.Member{
				ID:             m.ID,
				Incarnation:    m.Incarnation,
				Status:         m.Status,
				LastHeartbeat:  m.LastHeartbeat,
				SuspicionStart: m.SuspicionStart,
			})
		}
	}
	return suspects
}

func (ml *MembershipList) IncrementIncarnation() {
	ml.Lock()
	defer ml.Unlock()
	ml.Incarnation++
}

// RefreshLocalNode updates the local node with a new timestamp (for rejoining)
func (ml *MembershipList) RefreshLocalNode() {
	ml.Lock()
	defer ml.Unlock()

	// Create a new NodeID with fresh timestamp
	newNodeID := types.NodeID{
		IP:        ml.LocalNode.IP,
		Port:      ml.LocalNode.Port,
		Timestamp: time.Now().Unix(),
	}

	// Remove old entry from members map
	delete(ml.Members, ml.LocalNode.String())

	// Update local node reference
	ml.LocalNode = newNodeID

	// Increment incarnation for the rejoin
	ml.Incarnation++

	// Add the new local node to members with fresh info
	ml.Members[newNodeID.String()] = &types.Member{
		ID:            newNodeID,
		Incarnation:   ml.Incarnation,
		Status:        types.Alive,
		LastHeartbeat: time.Now(),
	}
}
