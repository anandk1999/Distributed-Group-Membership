package types

import (
	"fmt"
	"time"
)

type NodeID struct {
	IP        string `json:"ip"`
	Port      int    `json:"port"`
	Timestamp int64  `json:"timestamp"`
}

func (n NodeID) String() string {
	return fmt.Sprintf("%s:%d:%d", n.IP, n.Port, n.Timestamp)
}

func (n NodeID) Address() string {
	return fmt.Sprintf("%s:%d", n.IP, n.Port)
}

type MemberStatus int

const (
	Alive MemberStatus = iota
	Suspected
	Failed
)

type Member struct {
	ID             NodeID
	Incarnation    int32
	Status         MemberStatus
	LastHeartbeat  time.Time
	SuspicionStart time.Time
}

type ChangeType int

const (
	Joined ChangeType = iota
	Left
	FailureDetected
	StatusChanged
)

type MemberUpdate struct {
	NodeID      NodeID       `json:"node_id"`
	Incarnation int32        `json:"incarnation"`
	Status      MemberStatus `json:"status"`
}
