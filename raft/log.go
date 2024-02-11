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
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

func (l *RaftLog) initEntries(storage Storage) {

	// empty entry with a dummy entry
	l.entries = []pb.Entry{{
		EntryType: pb.EntryType_EntryNormal,
		Term:      0,
		Index:     0,
		Data:      nil,
	}}

	low, err := storage.FirstIndex()
	if err == nil {
		high, err := storage.LastIndex()
		if err == nil && low <= high {
			ents, err := storage.Entries(low, high+1)
			if err == nil {
				l.entries = append(l.entries, ents...)
			}
		}
	}
	l.stabled = l.LastIndex()
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// TODO: Your Code Here (2A).

	raftLog := &RaftLog{
		storage:   storage,
		committed: 0,
		applied:   0,
	}
	raftLog.initEntries(storage)

	log.Infof("new raft log: %+v", raftLog)

	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// TODO: Your Code Here (2A).
	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// TODO: Your Code Here (2A).
	return l.entries[l.stabled+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// TODO: Your Code Here (2A).

	return l.entries[l.applied+1 : l.committed+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// TODO: Your Code Here (2A).
	entriesLen := len(l.entries)
	return l.entries[entriesLen-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// TODO: Your Code Here (2A).
	if lastIndex := l.LastIndex(); lastIndex < i {
		message := "entry with index %d not exist and last index is %d"
		return 0, errors.New(fmt.Sprintf(message, i, lastIndex))
	}
	return l.entries[i].Term, nil
}
