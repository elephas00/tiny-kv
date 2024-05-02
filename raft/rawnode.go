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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	prevSoftState *SoftState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// TODO: Your Code Here (2A).
	raft := newRaft(config)
	return &RawNode{Raft: raft, prevSoftState: &SoftState{}}, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {

	if rn.Raft.leadTransferee != None {
		return ErrProposalDropped
	}

	if rn.Raft.PendingConfIndex > rn.Raft.RaftLog.applied {
		log.Errorf("%s reject to propose conf change, only one conf change processing at one time, previous %d, progress:%+v", rn.Raft.nodeIdentifier(), rn.Raft.PendingConfIndex, rn.GetProgress())
		return ErrProposalDropped
	}

	log.Infof("%s receive propose conf change: %+v, transferee %d", rn.Raft.nodeIdentifier(), cc, rn.Raft.leadTransferee)
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	// TODO: Your Code Here (2A).
	var hardState pb.HardState
	initialState, _, _ := rn.Raft.RaftLog.storage.InitialState()
	if initialState.Term == rn.Raft.Term &&
		initialState.Vote == rn.Raft.Vote &&
		initialState.Commit == rn.Raft.RaftLog.committed {
		// do nothing
	} else {
		hardState.Term = rn.Raft.Term
		hardState.Vote = rn.Raft.Vote
		hardState.Commit = rn.Raft.RaftLog.committed
	}

	entries := rn.Raft.RaftLog.unstableEntries()
	commitedEntries := rn.Raft.RaftLog.nextEnts()
	msgs := rn.Raft.msgs
	peers := make([]uint64, 0)
	for id := range rn.Raft.Prs {
		peers = append(peers, id)
	}

	for _, m := range msgs {
		if m.MsgType == pb.MessageType_MsgSnapshot && m.Snapshot == nil {
			log.Panic("snapshot is empty")
		}
	}

	truncatedIndex := rn.Raft.RaftLog.getOffset()
	term, err := rn.Raft.RaftLog.Term(truncatedIndex)
	if err != nil {
		log.Errorf("%s failed to get term of index %d", rn.Raft.nodeIdentifier(), term)
	}

	ready := Ready{
		HardState:        hardState,
		Entries:          entries,
		CommittedEntries: commitedEntries,
		Messages:         msgs,
	}
	if rn.Raft.RaftLog.pendingSnapshot != nil {
		ready.Snapshot = *rn.Raft.RaftLog.pendingSnapshot
	}

	softState := &SoftState{RaftState: rn.Raft.State, Lead: rn.Raft.Lead}
	if rn.softStateChanged(softState) {
		ready.SoftState = softState
		rn.prevSoftState = softState
	}
	return ready
}

func (rn *RawNode) softStateChanged(state *SoftState) bool {
	return !(rn.prevSoftState.RaftState == state.RaftState && rn.prevSoftState.Lead == state.Lead)
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// TODO: Your Code Here (2A).
	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// TODO: Your Code Here (2A).
	//log.Infof("node %+v, commit:", rn.Raft.nodeIdentifier())
	raft := rn.Raft
	// apply snapshot.
	if size := len(rd.Entries); size > 0 {
		raft.RaftLog.stabled = rd.Entries[size-1].Index
	}
	if size := len(rd.CommittedEntries); size > 0 {
		raft.RaftLog.applyTo(rd.CommittedEntries[size-1].Index)
	}
	if size := len(rd.Messages); size > 0 {
		// clear messages in ready.
		raft.msgs = make([]pb.Message, 0)
	}
	rn.Raft.RaftLog.pendingSnapshot = nil
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {

	peers := []uint64{}
	for id := range rn.Raft.Prs {
		peers = append(peers, id)
	}

	log.Infof("%s receive transfer leadership to: %d, peers %+v", rn.Raft.nodeIdentifier(), transferee, peers)
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
