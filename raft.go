package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

const DebugCM = 1

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

func (s CMState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		return "Unknown"
	}
}

type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type ConsensusModule struct {
	mu sync.Mutex

	id      int
	peerIds []int

	server *Server

	currentTerm int
	votedFor    int

	state              CMState
	electionResetEvent time.Time
}

func NewConsensusModule(id int, peerIds []int, server *Server, ready <-chan interface{}) *ConsensusModule {
	cm := ConsensusModule{
		id:       id,
		peerIds:  peerIds,
		server:   server,
		state:    Follower,
		votedFor: -1,
	}

	go func() {
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()

	return &cm
}

func (cm *ConsensusModule) electionTimeout() time.Duration {
	// Election timeout is between 150-300ms
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (cm *ConsensusModule) runElectionTimer() {
	electionTimeout := cm.electionTimeout()
	cm.mu.Lock()
	startedTerm := cm.currentTerm
	cm.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C

		cm.mu.Lock()
		if cm.state != Follower && cm.state != Candidate {
			// We're the leader, bail out of the election process
			cm.mu.Unlock()
			return
		}

		if startedTerm != cm.currentTerm {
			// New leader detected, bail out of the election process
			cm.mu.Unlock()
			return
		}

		if elapsedTime := time.Since(cm.electionResetEvent); elapsedTime >= electionTimeout {
			// Haven't received a heartbeat from the leader after the fixed timeout,
			// initiate an election in the cluster.
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	// The term of the current election
	currentTerm := cm.currentTerm
	// Reset election timer because we're starting a new one
	cm.electionResetEvent = time.Now()
	// Vote for self
	cm.votedFor = cm.id

	votesReceived := 1

	// Request votes from the other peers
	for _, peerId := range cm.peerIds {
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        currentTerm,
				CandidateId: cm.id,
			}

			var reply RequestVoteReply

			if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()

				if reply.Term > currentTerm {
					// The term is outdated, bail out of the election
					cm.becomeFollower(reply.Term)
					return
				}

				if reply.Term == currentTerm && reply.VoteGranted {
					votesReceived++
					if votesReceived*2 > len(cm.peerIds)+1 {
						// If quorum is reached, we've won the election
						cm.becomeLeader()
						return
					}
				}
			}

		}(peerId)
	}

	go cm.runElectionTimer()
}

func (cm *ConsensusModule) becomeFollower(newTerm int) {
	cm.state = Follower
	cm.currentTerm = newTerm
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()
	go cm.runElectionTimer()
}

func (cm *ConsensusModule) becomeLeader() {
	cm.state = Leader

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			cm.sendHeartbeat()
			<-ticker.C

			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

func (cm *ConsensusModule) sendHeartbeat() {
	cm.mu.Lock()
	if cm.state != Leader {
		cm.mu.Unlock()
		return
	}
	currentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerId := range cm.peerIds {
		args := AppendEntriesArgs{
			Term:     currentTerm,
			LeaderId: cm.id,
		}

		go func(peerId int) {
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()

				if reply.Term > currentTerm {
					// Term is outdated, degrade self to follower
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}(peerId)
	}
}

func (cm *ConsensusModule) dlog(format string, args ...interface{}) {
	if DebugCM == 0 {
		return
	}

	format = fmt.Sprintf("[%d] ", cm.id) + format
	log.Printf(format, args...)
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}

	if args.Term > cm.currentTerm {
		cm.becomeFollower(args.Term)
	}

	if args.Term == cm.currentTerm && (cm.votedFor == -1 || cm.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateId
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = cm.currentTerm
	return nil
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}

	if args.Term > cm.currentTerm {
		cm.becomeFollower(args.Term)
	}

	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}

		cm.electionResetEvent = time.Now()
		reply.Success = true
	}

	reply.Term = cm.currentTerm
	return nil
}

func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.state = Dead
}
