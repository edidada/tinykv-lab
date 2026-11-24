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
	"math/rand"
	"sort"

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

	// randomizedElectionTimeout is the current election timeout, drawn
	// randomly in [electionTimeout, 2*electionTimeout) to avoid split votes.
	randomizedElectionTimeout int

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

	raftLog := newLog(c.Storage)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	r := &Raft{
		id:               c.ID,
		Lead:             None,
		Term:             hs.Term,
		Vote:             hs.Vote,
		State:            StateFollower,
		RaftLog:          raftLog,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Prs:              make(map[uint64]*Progress),
		randomizedElectionTimeout: c.ElectionTick,
	}
	r.resetRandomizedElectionTimeout()

	if c.Applied > 0 {
		raftLog.applied = c.Applied
	}
	raftLog.commitTo(hs.Commit)

	// Members come either from the config (new cluster) or from the stored
	// ConfState (restart).
	members := c.peers
	if len(members) == 0 && cs.Nodes != nil {
		members = cs.Nodes
	}
	for _, id := range members {
		r.Prs[id] = &Progress{Next: raftLog.LastIndex() + 1}
	}
	if _, ok := r.Prs[r.id]; !ok {
		// A restarted node with an empty conf state still knows itself.
		r.Prs[r.id] = &Progress{Next: raftLog.LastIndex() + 1}
	}
	r.Prs[r.id].Match = raftLog.LastIndex()

	if len(r.Prs) == 1 {
		// single-node cluster: no need to wait for heartbeats
		r.becomeFollower(r.Term, None)
	}

	return r
}

// send appends a message to the outgoing queue, stamping it with our term.
func (r *Raft) send(m pb.Message) {
	m.Term = r.Term
	m.From = r.id
	r.msgs = append(r.msgs, m)
}

// quorum is the majority of the cluster size.
func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	pr := r.Prs[to]
	if pr == nil {
		return false
	}

	term, err := r.RaftLog.Term(pr.Next - 1)
	if err != nil {
		// The prefix is no longer available, a snapshot is needed (2C).
		r.sendSnapshot(to)
		return false
	}

	ents := r.RaftLog.slice(pr.Next, r.RaftLog.LastIndex()+1)
	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		Index:   pr.Next - 1,
		LogTerm: term,
		Commit:  r.RaftLog.committed,
	}
	for i := range ents {
		e := ents[i]
		m.Entries = append(m.Entries, &e)
	}
	r.send(m)
	return true
}

// sendSnapshot sends a snapshot RPC to the given peer (2C).
func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	r.send(pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		Snapshot: &snapshot,
	})
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	pr := r.Prs[to]
	if pr == nil {
		return
	}
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		Commit:  min(r.RaftLog.committed, pr.Match),
	})
}

// bcastAppend sends append RPCs to all other peers.
func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

// bcastHeartbeat sends heartbeats to all other peers.
func (r *Raft) bcastHeartbeat() {
	for id := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

// resetRandomizedElectionTimeout draws a new randomized election timeout.
func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	default:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			r.resetRandomizedElectionTimeout()
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Term = term
	r.Vote = None
	r.Lead = lead
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.votes = make(map[uint64]bool)
	r.leadTransferee = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	if r.State == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool)

	lasti := r.RaftLog.LastIndex()
	for id, pr := range r.Prs {
		pr.Match = 0
		pr.Next = lasti + 1
		if id == r.id {
			pr.Match = lasti
			pr.Next = lasti + 1
		}
	}

	// Leader should propose a noop entry on its term
	noop := pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
	}
	r.RaftLog.append(noop)
	r.Prs[r.id].Match = noop.Index
	r.Prs[r.id].Next = noop.Index + 1

	// A single-node cluster commits immediately.
	r.maybeCommit()

	r.bcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Handle term changes first (except for local, term-less messages).
	switch m.MsgType {
	case pb.MessageType_MsgHup, pb.MessageType_MsgPropose, pb.MessageType_MsgBeat:
	default:
		if m.Term < r.Term {
			// Stale message: refuse and report our own term, but never
			// modify any state.
			switch m.MsgType {
			case pb.MessageType_MsgAppend:
				r.sendAppendResponse(m.From, false, r.RaftLog.committed)
			case pb.MessageType_MsgHeartbeat:
				r.send(pb.Message{
					MsgType: pb.MessageType_MsgHeartbeatResponse,
					To:      m.From,
					Index:   r.RaftLog.committed,
				})
			case pb.MessageType_MsgRequestVote:
				r.send(pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					Reject:  true,
				})
			}
			return nil
		}
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		}
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State == StateLeader {
			return nil
		}
		r.campaign()

	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			r.bcastHeartbeat()
		}

	case pb.MessageType_MsgPropose:
		switch r.State {
		case StateLeader:
			return r.handlePropose(m)
		case StateFollower:
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
			return nil
		default:
			return ErrProposalDropped
		}

	case pb.MessageType_MsgAppend:
		if r.State == StateCandidate {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)

	case pb.MessageType_MsgHeartbeat:
		if r.State == StateCandidate {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)

	case pb.MessageType_MsgSnapshot:
		if r.State == StateCandidate {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleSnapshot(m)

	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)

	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)

	case pb.MessageType_MsgAppendResponse:
		if r.State == StateLeader {
			r.handleAppendResponse(m)
		}

	case pb.MessageType_MsgHeartbeatResponse:
		if r.State == StateLeader {
			r.handleHeartbeatResponse(m)
		}

	case pb.MessageType_MsgTransferLeader:
		// (Used in 3A)
		r.handleTransferLeader(m)

	case pb.MessageType_MsgTimeoutNow:
		// (Used in 3A)
		if r.State == StateFollower {
			r.campaign()
		}
	}
	return nil
}

// campaign starts an election: become candidate, vote for self and ask for votes.
func (r *Raft) campaign() {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		r.bcastAppend()
		return
	}
	lasti := r.RaftLog.LastIndex()
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			Index:   lasti,
			LogTerm: r.RaftLog.lastTerm(),
		})
	}
}

// handlePropose appends the proposed entries to the leader's log.
func (r *Raft) handlePropose(m pb.Message) error {
	for _, e := range m.Entries {
		entry := pb.Entry{
			EntryType: e.EntryType,
			Term:      r.Term,
			Index:     r.RaftLog.LastIndex() + 1,
			Data:      e.Data,
		}
		// (3A) only allow one pending conf change
		if entry.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex > r.RaftLog.applied {
				return ErrProposalDropped
			}
			r.PendingConfIndex = entry.Index
		}
		r.RaftLog.append(entry)
	}
	if len(m.Entries) > 0 {
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	}
	r.maybeCommit()
	r.bcastAppend()
	return nil
}

// handleRequestVote decides whether to grant the vote requested by m.
func (r *Raft) handleRequestVote(m pb.Message) {
	reject := false
	if m.Term < r.Term {
		reject = true
	} else if r.Vote != None && r.Vote != m.From {
		reject = true
	} else if !r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
		reject = true
	}

	if !reject {
		r.Vote = m.From
		r.electionElapsed = 0
	}

	r.send(pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		Reject:  reject,
	})
}

// handleVoteResponse tallies votes and turns the candidate into leader or follower.
func (r *Raft) handleVoteResponse(m pb.Message) {
	if r.State != StateCandidate {
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.votes[m.From] = !m.Reject

	granted, rejected := 0, 0
	for _, v := range r.votes {
		if v {
			granted++
		} else {
			rejected++
		}
	}

	if granted >= r.quorum() {
		r.becomeLeader()
		return
	}
	if rejected >= r.quorum() {
		r.becomeFollower(r.Term, None)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	r.Lead = m.From
	r.electionElapsed = 0

	lastnewi, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, derefEntries(m.Entries)...)
	if !ok {
		r.sendAppendResponse(m.From, false, r.RaftLog.LastIndex())
		return
	}
	r.sendAppendResponse(m.From, true, lastnewi)
}

// sendAppendResponse reports the result of an AppendEntries RPC.
func (r *Raft) sendAppendResponse(to uint64, ok bool, index uint64) {
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		Index:   index,
		Reject:  !ok,
	})
}

// handleAppendResponse updates the progress of a follower.
func (r *Raft) handleAppendResponse(m pb.Message) {
	pr := r.Prs[m.From]
	if pr == nil {
		return
	}

	if m.Reject {
		// step back, but never below 1
		next := pr.Next
		if next > 1 {
			next--
		}
		if m.Index+1 < next {
			next = m.Index + 1
		}
		if next < 1 {
			next = 1
		}
		pr.Next = next
		r.sendAppend(m.From)
		return
	}

	if m.Index > pr.Match {
		pr.Match = m.Index
		pr.Next = pr.Match + 1
	}

	if r.maybeCommit() {
		r.bcastAppend()
	}

	// keep the transferee in sync (3A)
	if r.leadTransferee == m.From && pr.Match == r.RaftLog.LastIndex() {
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgTimeoutNow,
			To:      m.From,
		})
		r.leadTransferee = None
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.Lead = m.From
	r.electionElapsed = 0
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.commitTo(min(m.Commit, r.RaftLog.LastIndex()))
	}
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		Index:   r.RaftLog.LastIndex(),
	})
}

// handleHeartbeatResponse handles the reply to a heartbeat.
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	pr := r.Prs[m.From]
	if pr == nil {
		return
	}
	if pr.Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	if m.Snapshot == nil || m.Snapshot.Metadata == nil {
		return
	}
	meta := m.Snapshot.Metadata
	if meta.Index <= r.RaftLog.committed {
		r.sendAppendResponse(m.From, false, r.RaftLog.committed)
		return
	}

	r.becomeFollower(m.Term, m.From)
	r.RaftLog.restore(m.Snapshot)
	r.Prs = make(map[uint64]*Progress)
	for _, id := range meta.ConfState.Nodes {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}
	r.sendAppendResponse(m.From, true, r.RaftLog.LastIndex())
}

// handleTransferLeader starts leadership transfer (3A).
func (r *Raft) handleTransferLeader(m pb.Message) {
	transferee := m.From
	if transferee == r.id || r.Prs[transferee] == nil {
		return
	}
	if r.State != StateLeader {
		return
	}
	r.leadTransferee = transferee
	if pr := r.Prs[transferee]; pr.Match == r.RaftLog.LastIndex() {
		r.send(pb.Message{MsgType: pb.MessageType_MsgTimeoutNow, To: transferee})
		r.leadTransferee = None
	} else {
		r.sendAppend(transferee)
	}
}

// maybeCommit advances the commit index once a quorum has replicated an entry
// from the current term.
func (r *Raft) maybeCommit() bool {
	matches := make([]uint64, 0, len(r.Prs))
	for _, pr := range r.Prs {
		matches = append(matches, pr.Match)
	}
	sort.Sort(uint64Slice(matches))
	m := matches[len(matches)-(len(matches)/2+1)]
	if m <= r.RaftLog.committed {
		return false
	}
	if term, err := r.RaftLog.Term(m); err == nil && term == r.Term {
		r.RaftLog.commitTo(m)
		return true
	}
	return false
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	if _, ok := r.Prs[id]; ok {
		return
	}
	r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	if _, ok := r.Prs[id]; !ok {
		return
	}
	delete(r.Prs, id)
	if r.id == id {
		return
	}
	if r.State == StateLeader {
		r.maybeCommit()
		r.bcastAppend()
	}
}

// derefEntries converts the pointer entries used on the wire into values.
func derefEntries(ents []*pb.Entry) []pb.Entry {
	out := make([]pb.Entry, len(ents))
	for i, e := range ents {
		if e != nil {
			out[i] = *e
		}
	}
	return out
}

// isUpToDate reports whether a candidate with (lastIndex, lastTerm) has a log
// at least as up to date as ours.
func (l *RaftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	return lastTerm > l.lastTerm() || (lastTerm == l.lastTerm() && lastIndex >= l.LastIndex())
}
