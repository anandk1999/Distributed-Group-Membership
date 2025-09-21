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
	UpdateHooks   []func(*types.Member, types.ChangeType)
	recentUpdates []types.MemberUpdate
	updateWindow  time.Duration
}

func NewMembershipList(localNode types.NodeID) *MembershipList {
	return &MembershipList{
		LocalNode:    localNode,
		Members:      make(map[string]*types.Member),
		Incarnation:  0,
		UpdateHooks:  []func(*types.Member, types.ChangeType){},
		updateWindow: 5 * time.Second,
	}
}

func (ml *MembershipList) AddMember(member *types.Member) {
	ml.Lock()
	defer ml.Unlock()

	ml.Members[member.ID.String()] = member

	// Trigger hooks
	for _, hook := range ml.UpdateHooks {
		hook(member, types.Joined)
	}
}

func (ml *MembershipList) RemoveMember(nodeID string) {
	ml.Lock()
	defer ml.Unlock()

	if member, exists := ml.Members[nodeID]; exists {
		delete(ml.Members, nodeID)

		// Trigger hooks
		for _, hook := range ml.UpdateHooks {
			hook(member, types.FailureDetected)
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

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	return candidates[:n]
}
