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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
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
	Match, Next  uint64
	RecentActive bool
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
		State:            StateFollower,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	// Initialize other fields...
	raft.initRaftLog(c)
	raft.initPeersByConfig(c)
	raft.initHardSate(c)
	raft.initVotes()

	log.Infof("new raft node %s", raft.nodeIdentifier())
	//log.Infof("peers:")
	//for id, progress := range raft.Prs {
	//	log.Infof("node %d match index: %d, next index: %d", id, progress.Match, progress.Next)
	//}
	return raft

}

func (r *Raft) initHardSate(c *Config) {
	hardState, _, _ := c.Storage.InitialState()
	r.Term = hardState.Term
	r.Vote = hardState.Vote
	r.RaftLog.committed = hardState.Commit
}

func (r *Raft) initPeersByConfig(c *Config) {
	var peers []uint64
	if len(c.peers) > 0 {
		peers = c.peers
	} else if _, confState, _ := c.Storage.InitialState(); len(confState.Nodes) > 0 {
		peers = confState.Nodes
	}

	r.initPeers(peers)
}

func (r *Raft) initVotes() {
	r.votes = make(map[uint64]bool, len(r.Prs))
	for key := range r.Prs {
		r.votes[key] = false
	}
}

func (r *Raft) initRaftLog(config *Config) {
	raftLog := newLog(config.Storage)

	if snapshot, err := config.Storage.Snapshot(); err != nil {
		//log.Errorf("%d snapshot: %+v", r.id, snapshot)
		raftLog.applied = config.Applied
	} else {
		raftLog.applied = snapshot.Metadata.Index

	}

	r.RaftLog = raftLog
}

func (r *Raft) sendSnapshot(to uint64) {

	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		//log.Errorf("%s send stale snapshot to %d: %+v", r.nodeIdentifier(), to, err)
		return
	} else {
		//log.Infof("%s send snapshot to %d, snap: %+v", r.nodeIdentifier(), to, *snapshot.Metadata)
	}
	snapshotMsg := pb.Message{
		From:     r.id,
		To:       to,
		Term:     r.Term,
		MsgType:  pb.MessageType_MsgSnapshot,
		Snapshot: &snapshot,
	}
	r.msgs = append(r.msgs, snapshotMsg)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// TODO: Your Code Here (2A).

	prs := r.Prs[to]
	if prs.Next <= r.RaftLog.getOffset() {
		r.sendSnapshot(to)
		return false
	}

	var prevLogIndex, prevLogTerm uint64

	if prs.Match <= r.RaftLog.getOffset() {
		compactedLog := r.RaftLog.entries[0]
		prevLogTerm = compactedLog.Term
		prevLogIndex = compactedLog.Index
	} else {
		prevLogIndex = prs.Match
		term, err := r.RaftLog.Term(prevLogIndex)
		if err != nil {
			message := "%s failed to access log index %d when send append entries, error %+v"
			log.Error(fmt.Sprintf(message, r.nodeIdentifier(), prevLogIndex, err))
			return false
		} else {
			prevLogTerm = term
		}
	}

	entries := r.RaftLog.getEntries(prevLogIndex+1, r.RaftLog.LastIndex()+1)

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
	r.electionElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
	if r.electionElapsed >= r.electionTimeout {
		count := 0
		for _, prs := range r.Prs {
			if prs.RecentActive {
				count++
			}
		}
		if count < r.minimumQuorum() {
			r.becomeFollower(r.Term, None)
		}
	} else {
		r.electionElapsed = -r.electionTimeout
		for _, prs := range r.Prs {
			prs.RecentActive = false
		}
		r.Prs[r.id].RecentActive = true
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// TODO: Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.leadTransferee = None
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
	r.Lead = None
	r.State = StateCandidate
	r.leadTransferee = None
	r.votes = make(map[uint64]bool, len(r.Prs))
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
	r.leadTransferee = None
	r.Lead = r.id
	//log.Infof("%s become leader", r.nodeIdentifier())
	// initialize leader data structure.
	for id := range r.Prs {
		progress := r.Prs[id]
		if progress.Match != 0 {
			// to avoid new leader missing newly added node.
			progress.Next = r.RaftLog.LastIndex() + 1
			progress.Match = r.RaftLog.getOffset()
		}
	}
	// propose noop entry.
	r.proposeNoopEntry()

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
		// this message will be rejected.
		// do nothing here.
	} else if m.Term == r.Term {
		// do nothing.
	} else if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	return nil
}

func (r *Raft) candidateIsMoreUpToDate(m pb.Message) bool {
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

func (r *Raft) resetElectionElapsed() {
	r.electionElapsed = -randIntInRange(0, r.electionTimeout)
}

func (r *Raft) handleFollowerStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.Prs[r.id] == nil {
			// do nothing
			log.Infof("node %d was removed from peers, just do nothing when timeout or msgHup", r.id)
		} else {
			r.becomeCandidate()
			r.resetElectionElapsed()
			r.sendRequestVoteToPeers()
		}

	case pb.MessageType_MsgRequestVote:
		if r.Term == m.Term && r.Lead == None && r.Vote == None && r.candidateIsMoreUpToDate(m) {
			r.Vote = m.From
			// log.Infof("%s, leader %+v, vote node %d", r.nodeIdentifier(), r.Lead, m.From)
		}
		r.sendVoteResponse(m.From, r.Vote != m.From)

	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)

	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(r.Term, m.From)
		r.resetElectionElapsed()
		r.handleHeartbeat(m)

	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)

	case pb.MessageType_MsgTransferLeader:
		r.handleFollowerTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:

		r.Step(pb.Message{From: r.id, To: r.id, Term: r.Term, MsgType: pb.MessageType_MsgHup})
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
	vote := 0
	veto := 0
	for _, ballot := range r.votes {
		if ballot {
			vote++
		} else {
			veto++
		}
	}
	if vote >= r.minimumQuorum() {
		r.becomeLeader()
	}
	if veto >= r.minimumQuorum() {
		r.becomeFollower(r.Term, None)
		r.Vote = r.id
	}
	// log.Infof("%s campaign result: vote(%d), veto(%d)", r.nodeIdentifier(), vote, veto)
}

func (r *Raft) handleCandidateStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.electionElapsed = -randIntInRange(0, r.electionTimeout)
		r.sendRequestVoteToPeers()

	case pb.MessageType_MsgRequestVoteResponse:
		if r.Term == m.Term {
			r.votes[m.From] = !m.Reject
			r.checkVotes()
		}

	case pb.MessageType_MsgRequestVote:
		r.sendVoteResponse(m.From, true)

	case pb.MessageType_MsgAppend:
		if r.Term == m.Term {
			r.handleAppendEntries(m)
		}

	case pb.MessageType_MsgHeartbeat:

		r.handleHeartbeat(m)
	}
	return nil
}

func (r *Raft) nodeIdentifier() string {
	pattern := "node %d (term %d, state %+v, commit %+v)"
	return fmt.Sprintf(pattern, r.id, r.Term, r.State, r.RaftLog.committed)
}

func (r *Raft) sendRequestVoteToPeers() {
	for peerId := range r.Prs {
		if peerId != r.id {
			r.sendRequestVote(peerId)
		}
	}
}

func (r *Raft) sendHeartbeatToPeers() {
	for peerId := range r.Prs {
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
			//log.Infof("%s commit %d, %d reach consensus", r.nodeIdentifier(), commit, cnt)
			break
		}
	}
	// synchronize the commit between leader and followers.
	if r.RaftLog.committed != oldValue {
		r.sendAppendEntriesToPeers()
	}
}

func (r *Raft) updatePrs(id, match, next uint64) {
	//log.Infof("node %d, match: %d, next: %d", id, match, match+1)
	r.Prs[id].Match = match
	r.Prs[id].Next = next
}

func (r *Raft) handleLeaderStep(m pb.Message) error {
	from := m.From
	prs := r.Prs[from]
	if prs != nil {
		prs.RecentActive = true
	}
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.heartbeatElapsed = 0
		r.sendHeartbeatToPeers()

	case pb.MessageType_MsgRequestVote:
		r.sendVoteResponse(m.From, true)
		//r.sendAppend(m.From)

	case pb.MessageType_MsgPropose:
		//  leadership transferring, return proposal dropped
		if r.leadTransferee != None {
			log.Errorf("%d reject to propose, leader transfering.", r.id)
			return ErrProposalDropped
		}
		// TODO: when config is changing, could the leader propose normal command?
		if r.RaftLog.applied < r.PendingConfIndex && m.Entries[0].EntryType == pb.EntryType_EntryConfChange {
			log.Errorf("%d reject to propose, previous pending confchange %d not finish, now apply %d", r.id, r.PendingConfIndex, r.RaftLog.applied)
			return ErrProposalDropped
		}

		for _, address := range m.Entries {
			address.Index = r.RaftLog.LastIndex() + 1
			address.Term = r.Term
			r.RaftLog.entries = append(r.RaftLog.entries, *address)
			r.updatePrs(r.id, address.Index, address.Index+1)
			if r.minimumQuorum() == 1 {
				r.RaftLog.committed = address.Index
			}
		}

		if m.Entries[0].EntryType == pb.EntryType_EntryConfChange {
			r.PendingConfIndex = m.Entries[0].Index
		}
		//log.Infof("%s propose at %d, entry type: %+v", r.nodeIdentifier(), m.Entries[0].Index, m.Entries[0].EntryType)
		r.sendAppendEntriesToPeers()

	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			prs := r.Prs[m.From]
			if prs.Next > 0 {
				r.updatePrs(m.From, prs.Match, min(m.Index, prs.Next-1))
			}
		} else {
			match := m.Index
			prs := r.Prs[m.From]
			r.updatePrs(m.From, max(prs.Match, match), max(prs.Next, match+1))
			// log.Infof("%s receive append response from %d, match index:%d", r.nodeIdentifier(), m.From, match)
			r.updateCommit()
			if r.transfereeIsExist(m.From) && r.leadTransferee == m.From && r.transfereeIsMostUpdate(m.From) {
				log.Infof("%s send timeoutnow to %d", r.nodeIdentifier(), m.From)
				r.sendMsgTimeoutNow(m.From)
			}
		}

	case pb.MessageType_MsgHeartbeatResponse:
		//log.Infof("%s receive a message from %d, detail: %+v", r.nodeIdentifier(), m.From, m)
		if m.Term == r.Term && m.Index != r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}

	case pb.MessageType_MsgTransferLeader:
		r.handleLeaderTransferLeader(m)

	}

	return nil
}

func (r *Raft) truncateRaftLog(end uint64) {
	if end == r.RaftLog.LastIndex()+1 {
		return
	}
	offset := r.RaftLog.getOffset()
	endIndex := end - offset
	r.RaftLog.entries = r.RaftLog.entries[:endIndex]
	r.RaftLog.stableTo(end - 1)
}

func (r *Raft) sendAppendResponse(to, index uint64, reject bool) {
	appendResponse := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index:   index,
		Reject:  reject,
	}
	//log.Infof("%s send resposne: %+v", r.nodeIdentifier(), appendResponse)
	r.msgs = append(r.msgs, appendResponse)
}

func (r *Raft) checkMessageNotValid(m *pb.Message) bool {
	lastLogIndex := r.RaftLog.LastIndex()
	if lastLogIndex < m.Index {
		return true
	}
	offset := r.RaftLog.getOffset()
	if m.Index >= offset {
		term, err := r.RaftLog.Term(m.Index)
		if err != nil {
			return false
		}
		return term != m.LogTerm
	}
	// this progress could with time complexity o(logn)
	for len(m.Entries) > 0 {
		firstEntry := m.Entries[0]
		m.Entries = m.Entries[1:]
		if firstEntry.Index < offset {
			// do nothing
		} else if firstEntry.Index == offset {
			m.Index = firstEntry.Index
			m.LogTerm = firstEntry.Term
			return r.checkMessageNotValid(m)
		}
	}
	return true

}

func (r *Raft) findMostApproximateIndex(index uint64, term uint64) uint64 {
	allEntries := r.RaftLog.allEntries()
	for i := len(allEntries) - 1; i >= 0; i-- {
		entry := allEntries[i]
		if entry.Index < index && entry.Term < term {
			return entry.Index
		}
	}
	return r.RaftLog.getOffset()
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// TODO: Your Code Here (2A).
	//log.Infof("%s receive append entries message, from: %d, to %d, prev log index: %d, prev log term: %d, size: %d", r.nodeIdentifier(), m.From, m.To, m.Index, m.LogTerm, len(m.Entries))
	if r.Term > m.Term {
		// reject.
		r.sendAppendResponse(m.From, None, true)
		return
	}
	r.becomeFollower(r.Term, m.From)
	// check whether the current follower contains entry that match previous log and previous term in message.
	if r.checkMessageNotValid(&m) {
		mostApproximateIndex := r.findMostApproximateIndex(m.Index, m.LogTerm)
		r.sendAppendResponse(m.From, mostApproximateIndex, true)
		return
	}

	for _, entry := range m.Entries {
		if term, err := r.RaftLog.Term(entry.Index); err == nil && term == entry.Term {
			// do nothing.
		} else {
			// truncate log start from this entry.
			r.truncateRaftLog(entry.Index)
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
	}
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Index+uint64(len(m.Entries)), m.Commit)
	}

	r.sendAppendResponse(m.From, m.Index+uint64(len(m.Entries)), false)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	heartbeatResponse := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Index:   r.RaftLog.LastIndex(),
		Reject:  reject,
	}
	r.msgs = append(r.msgs, heartbeatResponse)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// TODO: Your Code Here (2A).

	// reject.
	if r.Term > m.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	//if term, err := r.RaftLog.Term(m.Index); !(err == nil && term == m.LogTerm) {
	//	r.sendHeartbeatResponse(m.From, true)
	//	return
	//}
	r.becomeFollower(m.GetTerm(), m.GetFrom())
	// accept.
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(r.RaftLog.LastIndex(), m.Commit)
	}
	r.sendHeartbeatResponse(m.From, false)

}

func (r *Raft) initPeers(peers []uint64) {
	r.Prs = make(map[uint64]*Progress, len(peers))

	for _, peer := range peers {
		r.Prs[peer] = &Progress{
			Match:        r.RaftLog.getOffset(),
			Next:         r.RaftLog.LastIndex() + 1,
			RecentActive: false,
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).

	if m.Snapshot.Metadata == nil {
		// ignore it.
		return
	}

	if r.Term > m.Term {
		return
	}
	if m.Snapshot.Metadata == nil {
		return
	}
	if r.RaftLog.pendingSnapshot != nil && r.RaftLog.pendingSnapshot.Metadata.Index == m.Snapshot.Metadata.Index && r.RaftLog.pendingSnapshot.Metadata.Term == m.Snapshot.Metadata.Term {
		log.Infof("%s reject to handle snapshot: %+v", r.nodeIdentifier(), m)
		r.sendAppendResponse(m.From, m.Snapshot.Metadata.Index, false)
		return
	}
	log.Infof("%s triggered handle snapshot: %+v", r.nodeIdentifier(), m)

	if r.RaftLog.LastIndex() >= m.Snapshot.Metadata.Index {
		term, err := r.RaftLog.Term(r.RaftLog.LastIndex())
		if err != nil {
			log.Errorf("failed to get last index")
		} else {
			if term >= m.Snapshot.Metadata.Term {
				r.sendAppendResponse(m.From, m.Snapshot.Metadata.Index, false)
				return
			}
		}

	}
	r.becomeFollower(r.Term, m.From)
	//
	//if m.Index < r.RaftLog.entries[0].Index || m.Index > r.RaftLog.LastIndex() {
	//	message := "warning: %s, trying to access index: %d, lastIncludedIndex: %d, commit: %d"
	//	log.Errorf(message, r.nodeIdentifier(), m.Index, r.RaftLog.entries[0].Index, r.RaftLog.committed)
	//	return
	//}

	r.compressRaftLog(m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term)

	r.initPeers(m.Snapshot.Metadata.ConfState.Nodes)
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.sendAppendResponse(m.From, m.Snapshot.Metadata.Index, false)
}

func (r *Raft) compressRaftLog(index, term uint64) {
	r.RaftLog.entries = []pb.Entry{
		{
			EntryType: pb.EntryType_EntryNormal,
			Index:     index,
			Term:      term,
		},
	}
	r.RaftLog.stableTo(index)
	r.RaftLog.applyTo(index)
	r.RaftLog.commitTo(index)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.Prs[id] = &Progress{
		Match:        0,
		Next:         1,
		RecentActive: false,
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	delete(r.Prs, id)
	if r.State == StateLeader {
		r.updateCommit()
	}
}

func (r *Raft) handleLeaderTransferLeader(m pb.Message) {
	if m.From == r.id {
		r.leadTransferee = None
		return
	}

	if r.transfereeIsExist(m.From) {
		r.leadTransferee = m.From
		if r.transfereeIsMostUpdate(m.From) {
			r.sendMsgTimeoutNow(m.From)
			log.Infof("%s send timeoutnow request to %d", r.nodeIdentifier(), m.From)
		} else {
			r.sendAppend(m.From)
			log.Infof("%s send append request to %d", r.nodeIdentifier(), m.From)
		}
	} else {
		// do nothing.
		log.Infof("failed to transfer leadership to %d, node not exists", m.From)

	}
}

func (r *Raft) sendMsgTimeoutNow(to uint64) {
	timeoutMessage := pb.Message{
		From:    r.id,
		To:      to,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgTimeoutNow,
	}
	r.msgs = append(r.msgs, timeoutMessage)
}

func (r *Raft) transfereeIsExist(transferee uint64) bool {
	return r.Prs[transferee] != nil
}

func (r *Raft) transfereeIsMostUpdate(transferee uint64) bool {
	return r.RaftLog.LastIndex() == r.Prs[transferee].Match
}

func (r *Raft) handleFollowerTransferLeader(m pb.Message) {
	if r.leadTransferee != r.Lead {
		r.sendTransferLeaderMessage(r.id)
	} else {
		r.sendTransferLeaderMessage(r.Lead)
	}

}

func (r *Raft) sendTransferLeaderMessage(to uint64) {
	transferLeaderMessage := pb.Message{
		From:    r.id,
		To:      r.Lead,
		Term:    r.Term,
		MsgType: pb.MessageType_MsgTransferLeader,
	}
	r.msgs = append(r.msgs, transferLeaderMessage)
}
