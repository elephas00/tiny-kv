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
	"fmt"
	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	// TODO: Your Code Here (2A).

	// create new raft node as the result of this function.
	raft := &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             None,
		State:            StateFollower,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	// Initialize other fields...
	raft.initPeers(c.peers)
	raft.initVotes()
	raft.initRaftLog(c)

	return raft

}

func (r *Raft) initPeers(peers []uint64) {
	r.Prs = make(map[uint64]*Progress, len(peers))
	for _, peerID := range peers {
		r.Prs[peerID] = &Progress{Match: 0, Next: 1}
	}
}

func (r *Raft) initVotes() {
	r.votes = make(map[uint64]bool, len(r.Prs))
	for key := range r.Prs {
		r.votes[key] = false
	}
}

func (r *Raft) initRaftLog(config *Config) {
	raftLog := newLog(config.Storage)
	raftLog.applied = config.Applied
	r.RaftLog = raftLog
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// TODO: Your Code Here (2A).

	progress := r.Prs[to]
	prevLogIndex := progress.Match
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		message := "%s failed to access log index %d when send append entries, error %+v"
		panic(fmt.Sprintf(message, r.nodeIdentifier(), prevLogIndex, err))
	}
	entries := r.RaftLog.entries[progress.Next:]

	log.Infof("%s send append to %d, previous log index %d, previous log term %d.", r.nodeIdentifier(), to, prevLogIndex, prevLogTerm)

	ents := make([]*pb.Entry, len(entries))

	for i, entry := range entries {
		ents[i] = &pb.Entry{
			EntryType: entry.EntryType,
			Term:      entry.Term,
			Index:     entry.Index,
			Data:      entry.Data,
		}
	}

	appendMsg := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgAppend,
		Index:   prevLogIndex,
		LogTerm: prevLogTerm,
		Entries: ents,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, appendMsg)
	return false
}

func (r *Raft) sendVoteResponse(to uint64, reject bool) {
	requestVoteResponseMsg := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, requestVoteResponseMsg)
}

func (r *Raft) sendRequestVote(to uint64) {
	lastIndex := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(lastIndex)
	requestVoteMsg := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   lastIndex,
		LogTerm: logTerm,
		MsgType: pb.MessageType_MsgRequestVote,
	}
	r.msgs = append(r.msgs, requestVoteMsg)
}

func (r *Raft) sendAppendEntriesToPeers() {
	for peerId := range r.Prs {
		if peerId != r.id {
			r.sendAppend(peerId)
		}
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// TODO: Your Code Here (2A).
	heartbeatMsg := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeat,
	}
	r.msgs = append(r.msgs, heartbeatMsg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower:
		r.tickFollower()
	case StateCandidate:
		r.tickCandidate()
	case StateLeader:
		r.tickLeader()
	}
}

func (r *Raft) tickFollower() {
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickCandidate() {
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) tickLeader() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// TODO: Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
}

// sendVoteToCandidateItself makes the current candidate vote for itself.
func (r *Raft) sendVoteToCandidateItself() {
	voteResponse := pb.Message{
		From:    r.id,
		To:      r.id,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Reject:  false,
	}
	_ = r.Step(voteResponse) // Ignore error for simplicity
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// TODO: Your Code Here (2A).

	r.Term++
	r.State = StateCandidate
	for key := range r.votes {
		r.votes[key] = false
	}

	r.sendVoteToCandidateItself()
}

func (r *Raft) proposeNoopEntry() {
	noopEntry := pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
	}
	proposeMsg := pb.Message{
		From:    r.id,
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&noopEntry},
	}

	r.Step(proposeMsg)

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// TODO: Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State != StateCandidate {
		return
	}
	r.State = StateLeader
	// propose noop entry.
	r.proposeNoopEntry()
	// initialize leader data structure.
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// TODO: Your Code Here (2A).

	if !IsLocalMsg(m.MsgType) {
		r.updateTermFromMessage(m)
	}

	var err error
	switch r.State {
	case StateFollower:
		err = r.handleFollowerStep(m)
	case StateCandidate:
		err = r.handleCandidateStep(m)
	case StateLeader:
		err = r.handleLeaderStep(m)
	}
	return err
}

func (r *Raft) updateTermFromMessage(m pb.Message) error {
	if m.Term < r.Term {
		// reject this message.
	} else if m.Term == r.Term {
		// r.becomeFollower(m.Term, m.From)
		// if r.State == StateLeader {
		// 	return errors.New("there are two leader in cluster.")
		// }
		// TODO: check this logic
		// just do nothing now.
	} else if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	return nil
}

func (r *Raft) candidateIsMoreUpToDate(m pb.Message) bool {
	// rf.getLog(rf.getLastLogIndex()).Term > args.LastLogTerm || (rf.getLog(rf.getLastLogIndex()).Term == args.LastLogTerm && rf.getLastLogIndex() > args.LastLogIndex
	//return r.Term == m.Term && r.RaftLog.LastIndex() <= m.Index
	if r.Term > m.Term {
		return false
	}
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
	if lastLogTerm > m.LogTerm {
		return false
	}
	if lastLogTerm == m.LogTerm && lastLogIndex > m.Index {
		return false
	}
	return true
}

func (r *Raft) handleFollowerStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.electionElapsed = -randIntInRange(0, r.electionTimeout)
		r.sendRequestVoteToPeers()

	case pb.MessageType_MsgRequestVote:
		if r.Vote == None && r.candidateIsMoreUpToDate(m) {
			r.Vote = m.From
		}
		r.sendVoteResponse(m.From, r.Vote != m.From)

	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	}
	return nil
}

// randIntInRange returns a random integer between [a, b]
func randIntInRange(a, b int) int {
	if a == b {
		return a
	}
	return rand.Intn(b-a) + a // 返回 [a, b] 之间的随机整数
}

func (r *Raft) minimumQuorum() int {
	size := len(r.Prs)
	return size/2 + 1
}

func (r *Raft) checkVotes() {
	cnt := 0
	for _, vote := range r.votes {
		if vote {
			cnt++
		}
	}
	if cnt >= r.minimumQuorum() {
		r.becomeLeader()
	}
}

func (r *Raft) handleCandidateStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.electionElapsed = -randIntInRange(0, r.electionTimeout)
		r.sendRequestVoteToPeers()

	case pb.MessageType_MsgRequestVoteResponse:
		if !m.Reject {
			r.votes[m.From] = true
			r.checkVotes()
		}

	case pb.MessageType_MsgRequestVote:
		r.sendVoteResponse(m.From, true)

	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
	}
	return nil
}

func (r *Raft) nodeIdentifier() string {
	pattern := "node %d (term %d, state %+v)"
	return fmt.Sprintf(pattern, r.id, r.Term, r.State)
}

func (r *Raft) sendRequestVoteToPeers() {
	for peerId, _ := range r.Prs {
		if peerId != r.id {
			r.sendRequestVote(peerId)
		}
	}
}

func (r *Raft) sendHeartbeatToPeers() {
	for peerId, _ := range r.Prs {
		if peerId != r.id {
			r.sendHeartbeat(peerId)
		}
	}
}

func (r *Raft) updateCommit() {
	oldValue := r.RaftLog.committed
	for commit := r.RaftLog.LastIndex(); commit > oldValue; commit-- {
		term, _ := r.RaftLog.Term(commit)
		cnt := 0
		for _, progress := range r.Prs {
			if progress.Match >= commit {
				cnt++
			}
		}

		// a leader could commit log in its term.
		if cnt >= r.minimumQuorum() && term == r.Term {
			r.RaftLog.committed = commit
			log.Infof("%s commit %d, %d reach consensus", r.nodeIdentifier(), commit, cnt)
			break
		}
	}
	// synchronize the commit between leader and followers.
	if r.RaftLog.committed != oldValue {
		r.sendAppendEntriesToPeers()
	}
}

func (r *Raft) handleLeaderStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.heartbeatElapsed = 0
		r.sendHeartbeatToPeers()

	case pb.MessageType_MsgPropose:

		for _, address := range m.Entries {
			address.Index = r.RaftLog.LastIndex() + 1
			address.Term = r.Term
			address.EntryType = pb.EntryType_EntryNormal
			r.RaftLog.entries = append(r.RaftLog.entries, *address)
			r.Prs[r.id].Match = address.Index
			if r.minimumQuorum() == 1 {
				r.RaftLog.committed = address.Index
			}
		}
		r.sendAppendEntriesToPeers()

	case pb.MessageType_MsgAppendResponse:

		match := m.Index
		r.Prs[m.From].Match = match
		r.Prs[m.From].Next = match + 1
		log.Infof("%s receive append response from %d, match index:%d", r.nodeIdentifier(), m.From, match)
		r.updateCommit()
	}

	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// TODO: Your Code Here (2A).
	if term, err := r.RaftLog.Term(m.Index); !(err == nil && term == m.LogTerm) {
		// reject.
		appendResponse := pb.Message{
			From:    m.To,
			To:      m.From,
			Term:    r.Term,
			MsgType: pb.MessageType_MsgAppendResponse,
			Reject:  true,
		}
		r.msgs = append(r.msgs, appendResponse)
		return
	}

	for _, entry := range m.Entries {
		if term, err := r.RaftLog.Term(entry.Index); err == nil && term == entry.Term {
			// do nothing.
		} else {
			// truncate log start from this entry.
			offset := r.RaftLog.entries[0].Index
			end := entry.Index - offset
			log.Infof("before truncate last index is:%d", r.RaftLog.LastIndex())
			r.RaftLog.entries = r.RaftLog.entries[:end]
			log.Infof("after truncate last index is:%d", r.RaftLog.LastIndex())
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
			// roll back stabled entries.
			if r.RaftLog.stabled > end-1 {
				r.RaftLog.stabled = end - 1
			}
		}
	}
	if r.RaftLog.committed < m.Commit {
		r.RaftLog.committed = m.Commit
	}

	responseMsg := pb.Message{
		From:    m.To,
		To:      m.From,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index:   m.Index + uint64(len(m.Entries)),
		Reject:  false,
	}

	r.msgs = append(r.msgs, responseMsg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// TODO: Your Code Here (2A).
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
