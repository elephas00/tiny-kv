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

func (l *RaftLog) initCompactedLog(firstLogIndex uint64, lastLogIndex uint64, storage Storage) {
	//if snapshot, err := storage.Snapshot(); err == nil {
	//	l.entries = append(l.entries, pb.Entry{
	//		EntryType: pb.EntryType_EntryNormal,
	//		Term:      snapshot.Metadata.Term,
	//		Index:     snapshot.Metadata.Index,
	//	})
	//	return
	//}
	l.entries = append(l.entries, pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      0,
		Index:     firstLogIndex - 1,
	})
	//if firstLogIndex < meta.RaftInitLogIndex {
	//	l.entries = append(l.entries, pb.Entry{
	//		EntryType: pb.EntryType_EntryNormal,
	//		Term:      0,
	//		Index:     0,
	//	})
	//	return
	//}
	//
	//term, err := storage.Term(firstLogIndex - 1)
	//if err != nil {
	//	log.Errorf("failed to get log %d", firstLogIndex)
	//}
	//
	//l.entries = append(l.entries, pb.Entry{
	//	EntryType: pb.EntryType_EntryNormal,
	//	Term:      term,
	//	Index:     firstLogIndex - 1,
	//})

}

func (l *RaftLog) initEntries(storage Storage) {

	// empty entry with a dummy entry
	l.entries = []pb.Entry{}

	firstLogIndex, err := storage.FirstIndex()
	if err != nil {
		log.Panicf("failed to get firstLogIndex, err: %+v", err)
	}
	lastLogIndex, err := storage.LastIndex()
	if err != nil {
		log.Panicf("failed to get lastLogIndex, err: %+v", err)
	}
	l.initCompactedLog(firstLogIndex, lastLogIndex, storage)

	if firstLogIndex <= lastLogIndex {
		ents, err := storage.Entries(firstLogIndex, lastLogIndex+1)
		if err != nil {
			log.Panicf("failed to get stabled logs, first:%d, last:%d, err: %+v", firstLogIndex, lastLogIndex, err)
		} else {
			l.entries = append(l.entries, ents...)
		}
	}

	log.Infof("first %d, last %d", firstLogIndex, lastLogIndex)
	log.Infof("init raft log, last %d, len: %d", l.LastIndex(), len(l.entries))

	l.stableTo(l.LastIndex())

}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).

	state, _, err := storage.InitialState()
	if err != nil {
		log.Errorf("failed to init log: %+v", err)
	}

	raftLog := &RaftLog{
		storage:   storage,
		committed: state.Commit,
	}
	raftLog.initEntries(storage)

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

// getEntries return all entries in [low, high)
func (l *RaftLog) getEntries(low, high uint64) []pb.Entry {
	if low == high {
		return []pb.Entry{}
	}
	if low > high {
		log.Panicf("failed to get entries with args low: %d, high: %d", low, high)
	}

	if low < l.getOffset() {
		log.Panicf("failed to get entries with args low: %d, offset is: %d", low, l.getOffset())
	}
	return l.entries[low-l.getOffset() : high-l.getOffset()]
}

func (l *RaftLog) getOffset() uint64 {
	return l.entries[0].Index
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.getEntries(l.stabled+1, l.LastIndex()+1)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	start := l.applied + 1
	end := l.committed + 1
	return l.getEntries(start, end)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	entriesLen := len(l.entries)
	return l.entries[entriesLen-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	offset := l.getOffset()
	if offset == i {
		return l.entries[0].Term, nil
	}
	if lastIndex := l.LastIndex(); lastIndex < i {
		message := "entry with index %d not exist and last index is %d"
		return 0, errors.New(fmt.Sprintf(message, i, lastIndex))
	}
	entries := l.getEntries(i, i+1)
	return entries[0].Term, nil
}

func (l *RaftLog) stableTo(index uint64) {
	l.stabled = index
}

func (l *RaftLog) applyTo(index uint64) {
	if index < l.applied {
		log.Panicf("failed to apply to index %d, current %d", index, l.applied)
	}
	l.applied = index
}

func (l *RaftLog) commitTo(index uint64) {
	if index < l.committed {
		log.Panicf("failed to commit to index %d, current %d", index, l.committed)
	}
	l.committed = index
}
