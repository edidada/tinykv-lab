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
	// entries[0] is a dummy entry holding the index/term of the last
	// compacted (or snapshotted) position, so that entries[i] always has
	// index entries[0].Index + i.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	l := &RaftLog{storage: storage}

	// Rebuild the in-memory view: dummy entry at firstIndex-1 plus every
	// entry still available in storage.
	dummyTerm, termErr := storage.Term(firstIndex - 1)
	if termErr != nil && termErr != ErrCompacted && termErr != ErrUnavailable {
		panic(termErr)
	}
	l.entries = make([]pb.Entry, 0, lastIndex-firstIndex+2)
	l.entries = append(l.entries, pb.Entry{Index: firstIndex - 1, Term: dummyTerm})
	if lastIndex >= firstIndex {
		ents, err := storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			panic(err)
		}
		l.entries = append(l.entries, ents...)
	}

	hs, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}
	l.committed = hs.Commit
	if l.committed < firstIndex-1 {
		l.committed = firstIndex - 1
	}
	if l.committed > lastIndex {
		l.committed = lastIndex
	}
	l.stabled = lastIndex
	l.applied = firstIndex - 1
	return l
}

// firstIndex is the index of the first entry in the log (the dummy entry is
// not a real entry).
func (l *RaftLog) firstIndex() uint64 {
	return l.entries[0].Index + 1
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	return l.entries[len(l.entries)-1].Index
}

// lastTerm returns the term of the last entry.
func (l *RaftLog) lastTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	dummyIndex := l.entries[0].Index
	if i == dummyIndex {
		return l.entries[0].Term, nil
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	if i < dummyIndex {
		return l.storage.Term(i)
	}
	return l.entries[i-dummyIndex].Term, nil
}

// slice returns entries in [lo, hi), lo must be > entries[0].Index.
func (l *RaftLog) slice(lo, hi uint64) []pb.Entry {
	if lo >= hi {
		return nil
	}
	if lo <= l.entries[0].Index {
		lo = l.entries[0].Index + 1
	}
	if hi > l.LastIndex()+1 {
		hi = l.LastIndex() + 1
	}
	base := l.entries[0].Index
	return l.entries[lo-base : hi-base]
}

// truncate removes entries with index >= i.
func (l *RaftLog) truncate(i uint64) {
	if i <= l.entries[0].Index || i > l.LastIndex() {
		return
	}
	l.entries = l.entries[:i-l.entries[0].Index]
	if l.stabled >= i {
		l.stabled = i - 1
	}
}

// append appends entries, truncating any conflicting suffix first.
func (l *RaftLog) append(ents ...pb.Entry) {
	for _, e := range ents {
		last := l.LastIndex()
		if e.Index <= last {
			if t, err := l.Term(e.Index); err == nil && t == e.Term {
				continue
			}
			l.truncate(e.Index)
			last = l.LastIndex()
		}
		// Fill in any gap defensively (should not happen with a correct leader).
		for last+1 < e.Index {
			last++
			l.entries = append(l.entries, pb.Entry{Index: last, Term: 0})
		}
		l.entries = append(l.entries, e)
	}
}

// maybeAppend tries to append entries sent by the leader. It reports whether
// the entries match the local log at (index, logTerm), and if so commits up to
// committed and returns the new last index.
func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	lasti := l.LastIndex()
	if index > lasti {
		return lasti, false
	}
	if index < l.entries[0].Index {
		return lasti, false
	}
	t, err := l.Term(index)
	if err != nil || t != logTerm {
		return lasti, false
	}

	l.append(ents...)
	if committed > l.committed {
		l.commitTo(min(committed, index+uint64(len(ents))))
	}
	return l.LastIndex(), true
}

// commitTo bumps the commit index (never decreases).
func (l *RaftLog) commitTo(index uint64) {
	if index > l.committed {
		l.committed = index
	}
	if l.committed > l.LastIndex() {
		l.committed = l.LastIndex()
	}
}

// appliedTo bumps the applied index (never decreases).
func (l *RaftLog) appliedTo(i uint64) {
	if i > l.applied {
		l.applied = i
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if l.stabled >= l.LastIndex() {
		return []pb.Entry{}
	}
	ents := l.slice(max(l.stabled+1, l.firstIndex()), l.LastIndex()+1)
	if ents == nil {
		return []pb.Entry{}
	}
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if l.committed <= l.applied {
		return nil
	}
	return l.slice(max(l.applied+1, l.firstIndex()), l.committed+1)
}

// maybeCompact drops entries that are already persisted and no longer needed
// in memory (i.e. covered by the storage snapshot).
func (l *RaftLog) maybeCompact() {
	first, err := l.storage.FirstIndex()
	if err != nil {
		return
	}
	// Keep the dummy entry at first-1.
	if first <= l.entries[0].Index+1 {
		return
	}
	offset := first - l.entries[0].Index
	if int(offset) >= len(l.entries) {
		return
	}
	term, err := l.storage.Term(first - 1)
	if err != nil {
		return
	}
	newEntries := make([]pb.Entry, 0, len(l.entries)-int(offset))
	newEntries = append(newEntries, pb.Entry{Index: first - 1, Term: term})
	newEntries = append(newEntries, l.entries[offset:]...)
	l.entries = newEntries
	if l.applied < first-1 {
		l.applied = first - 1
	}
	if l.committed < first-1 {
		l.committed = first - 1
	}
}

// restore resets the log to the state described by the given snapshot.
func (l *RaftLog) restore(s *pb.Snapshot) {
	meta := s.Metadata
	l.entries = []pb.Entry{{Index: meta.Index, Term: meta.Term}}
	l.committed = meta.Index
	l.applied = meta.Index
	l.stabled = meta.Index
	l.pendingSnapshot = s
}

