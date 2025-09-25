package utils

import (
	"fmt"
	"time"
)

// NodeID uniquely identifies a node
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

func (n NodeID) Equals(other NodeID) bool {
	return n.IP == other.IP && n.Port == other.Port && n.Timestamp == other.Timestamp
}

// MemberStatus represents the status of a member
type MemberStatus int

const (
	Alive MemberStatus = iota
	Suspected
	Failed
)

func (s MemberStatus) String() string {
	switch s {
	case Alive:
		return "Alive"
	case Suspected:
		return "Suspected"
	case Failed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// Member represents a group member
type Member struct {
	ID             NodeID
	Incarnation    int32
	Status         MemberStatus
	LastHeartbeat  time.Time
	SuspicionStart time.Time
}

// ChangeType represents types of membership changes
type ChangeType int

const (
	Joined ChangeType = iota
	Left
	FailureDetected
	StatusChanged
)

// MessageType represents different message types
type MessageType int

const (
	Heartbeat MessageType = iota
	Ping
	Ack
	IndirectPing
	IndirectAck
	Join
	JoinResponse
	Leave
	Suspect
	AliveMsg
	Confirm
)

// Message represents a network message
type Message struct {
	Type        MessageType    `json:"type"`
	Sender      NodeID         `json:"sender"`
	Target      NodeID         `json:"target,omitempty"`
	Incarnation int32          `json:"incarnation"`
	SeqNum      uint64         `json:"seq_num,omitempty"`
	Members     []MemberUpdate `json:"members,omitempty"`
	Timestamp   int64          `json:"timestamp"`
}

// MemberUpdate represents a membership update
type MemberUpdate struct {
	NodeID      NodeID       `json:"node_id"`
	Incarnation int32        `json:"incarnation"`
	Status      MemberStatus `json:"status"`
	Timestamp   time.Time    `json:"timestamp"`
}

// DetectionMode represents the failure detection mode
type DetectionMode int

const (
	GossipMode DetectionMode = iota
	PingAckMode
)

func ParseMode(mode string) DetectionMode {
	if mode == "pingack" {
		return PingAckMode
	}
	return GossipMode
}

// Config represents configuration for initializing a node
type Config struct {
	NodeID         NodeID
	IntroducerAddr string
	IsIntroducer   bool
	Mode           DetectionMode
}
