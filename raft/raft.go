// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	realElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	rf := Raft{}
	rf.id = c.ID
	rf.electionTimeout = c.ElectionTick
	rf.heartbeatTimeout = c.HeartbeatTick
	rf.Prs = make(map[uint64]*Progress)
	rf.votes = make(map[uint64]bool)
	for _, peer := range c.peers {
		prg := &Progress{
			Next:  0,
			Match: 0,
		}
		rf.Prs[peer] = prg
	}
	rf.electionElapsed = 0
	rf.heartbeatElapsed = 0
	rf.realElectionTimeout = randTime(rf.electionTimeout)
	rf.RaftLog = newLog(c.Storage)
	rf.State = StateFollower
	rf.Term = 0
	rf.Vote = 0
	return &rf
}

func (r *Raft) checkLeaderCommit() {
	if r.State != StateLeader {
		return
	}
	count := make(map[uint64]int)
	for peer := range r.Prs {
		N := r.Prs[peer].Match
		if N > r.RaftLog.committed {
			count[N] += 1
		}
	}
	for key, value := range count {
		if (value)*2 > len(r.Prs) && key > r.RaftLog.committed {
			r.RaftLog.committed = key
		}
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	for _, entry := range m.Entries {
		entry.Term = r.Term
		entry.Index = r.RaftLog.LastIndex() + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	nextIndex := r.Prs[to].Next
	entries := r.RaftLog.entries[nextIndex:]
	sendEntries := []*pb.Entry{}
	for _, entry := range entries {
		sendEntries = append(sendEntries, &entry)
	}
	lastLogTerm, _ := r.RaftLog.Term(nextIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: lastLogTerm,
		Entries: sendEntries,
		Index:   nextIndex - 1,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{}
	msg.MsgType = pb.MessageType_MsgHeartbeat
	msg.To = to
	msg.From = r.id
	msg.Term = r.Term
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) electionTick() {
	r.electionElapsed += 1
	if r.electionElapsed >= r.realElectionTimeout {
		r.realElectionTimeout = randTime(r.electionTimeout)
		r.startElection()
		r.electionElapsed = 0
	}
}

func (r *Raft) heartbeatTick() {
	r.heartbeatElapsed += 1
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		for peer := range r.Prs {
			if peer == r.id {
				continue
			}
			r.sendHeartbeat(peer)
		}
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.electionTick()
	case StateCandidate:
		r.electionTick()
	case StateLeader:
		r.heartbeatTick()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Vote = 0
	r.Lead = lead
	if term > r.Term {
		r.Term = term
	}
}

func (r *Raft) startElection() {
	if r.State == StateLeader {
		return
	}
	r.becomeCandidate()
	r.msgs = []pb.Message{}
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		msg := pb.Message{}
		msg.MsgType = pb.MessageType_MsgRequestVote
		msg.To = peer
		msg.From = r.id
		msg.Term = r.Term
		r.msgs = append(r.msgs, msg)
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.Vote = r.id
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.Vote = 0
	dummy := pb.Entry{}
	dummy.Term = r.Term
	dummy.Index = r.RaftLog.LastIndex() + 1
	dummy.Data = nil
	r.RaftLog.entries = append(r.RaftLog.entries, dummy)
	for peer := range r.Prs {
		r.Prs[peer].Next = dummy.Index
		r.Prs[peer].Match = 0
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case 0: //MsgHup
		r.startElection()
	case 1: //MsgBeat
		r.handleHeartbeat(m)
	case 2: //MsgPropose
		r.handlePropose(m)
	case 3: //MsgAppend
		r.handleAppendEntries(m)
	case 4: //MsgAppendResponse
		r.handleAppendEntriesResponse(m)
	case 5: //RequestVote
		r.handleRequestVote(m)

	case 6: //RequestVoteRespone
		r.handleRequestVoteResponse(m)

	case 7: //MsgSnapshot

	case 8: //MsgHeartbeat

	case 9: //MsgHeartbeat

	case 11: //MsgTransferLeader

	case 12: //MsgTimeout

	}
	return nil
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	if !m.Reject {
		match := m.Index
		next := match + 1
		r.Prs[m.From].Next = max(r.Prs[m.From].Next, next)
		r.Prs[m.From].Match = max(r.Prs[m.From].Match, match)
	} else if m.Reject {
		r.Prs[m.From].Next--
	}
	r.checkLeaderCommit()
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{}
	msg.Term = r.Term
	msg.From = r.id
	msg.To = m.From
	msg.MsgType = pb.MessageType_MsgAppendResponse
	if m.Term != None && m.Term < r.Term {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}
	r.electionElapsed = 0
	r.Vote = 0
	if r.State == StateCandidate {
		r.becomeFollower(m.Term, m.From)
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	for _, entry := range m.Entries {
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	msg.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.State == StateLeader {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.Vote = 0
		r.electionElapsed = 0
		for peer := range r.Prs {
			if peer == r.id {
				continue
			}
			msg := pb.Message{}
			msg.MsgType = pb.MessageType_MsgHeartbeat
			msg.From = r.id
			msg.To = peer
			r.msgs = append(r.msgs, msg)
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	msg := pb.Message{}
	msg.MsgType = pb.MessageType_MsgRequestVoteResponse
	msg.Term = r.Term
	msg.From = r.id
	msg.To = m.From
	if m.Term < r.Term || (r.Vote != 0 && r.Vote != m.From) {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}
	msg.Reject = false
	r.Vote = m.From
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	votes := 0
	if !m.Reject {
		r.votes[m.From] = true
	}
	if r.State == StateCandidate {
		for _, v := range r.votes {
			if v {
				votes += 1
			}
		}
		if (votes)*2 > len(r.Prs) {
			r.becomeLeader()
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
