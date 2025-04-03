package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"math/rand"

	"6.5840/labgob"

	"6.5840/labrpc"
)

const MaxElectionTimeout int = 500
const MinElectionTimeout int = 300
const HeartbeatInterval = 100 * time.Millisecond
const WinningElectionWaitTime = 50 * time.Millisecond

type raftState int

const (
	Follower raftState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	becomeFollowerCh chan struct{}
	validHeartbeatCh chan struct{}
	grantVoteCh      chan struct{}
	majorGrantCh     chan struct{}
	broadcastCh      chan struct{}
	applyCh          chan ApplyMsg

	state raftState
	nVote int

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// util functions
func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func randomTimeout(minTime, maxTime int) time.Duration {
	return time.Duration(rand.Intn(maxTime-minTime)+minTime) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

func (rf *Raft) adjustLeaderCommitIndex() {
	if rf.state != Leader {
		return
	}
	virtualCommitIndex := rf.commitIndex + 1
	lastIndex := rf.LastLogIndex()
	for virtualCommitIndex <= lastIndex {
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= virtualCommitIndex {
				count += 1
			}
		}
		if count <= len(rf.peers)/2 {
			break
		}
		virtualCommitIndex += 1
	}
	virtualCommitIndex -= 1
	if virtualCommitIndex > rf.commitIndex && rf.safeLogEntryGetter(virtualCommitIndex-1).Term == rf.currentTerm {
		rf.commitIndex = virtualCommitIndex
	}
}

// ! with lock
func (rf *Raft) safeLogEntryGetter(index int) LogEntry {
	if index == -1 {
		return LogEntry{
			Index:   0,
			Term:    -1,
			Command: nil,
		}
	} else if index < -1 {
		panic("only allowed index = -1 as virtual index")
	} else if index >= len(rf.log) {
		panic("index out of range")
	} else {
		return rf.log[index]
	}
}

// ! with lock
func (rf *Raft) LastLogIndex() int {
	return rf.safeLogEntryGetter(len(rf.log) - 1).Index
}

// ! with lock
func (rf *Raft) LastLogTerm() int {
	return rf.safeLogEntryGetter(len(rf.log) - 1).Term
}

// ! with lock
func (rf *Raft) becomeFollower(term int) {
	if term < rf.currentTerm {
		panic("term should be greater than current term")
	}
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	notify(rf.becomeFollowerCh)
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Candidate {
		return
	}
	DPrintf("server %d [Term = %d] become leader\n", rf.me, rf.currentTerm)
	rf.state = Leader
	for node := range rf.peers {
		rf.nextIndex[node] = rf.LastLogIndex() + 1
		rf.matchIndex[node] = 0
	}

	go func() {
		time.Sleep(WinningElectionWaitTime)
		rf.broadcast()
	}()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		return
	}
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.nVote = 1
	rf.persist()
	DPrintf("server %d [Term = %d] start election\n", rf.me, rf.currentTerm)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.LastLogIndex(),
		LastLogTerm:  rf.LastLogTerm(),
	}
	for i := range rf.peers {
		reply := RequestVoteReply{}
		if i != rf.me {
			go rf.sendRequestVote(i, &args, &reply)
		}
	}
}

func (rf *Raft) broadcast() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}

	DPrintf("server %d [Term = %d] broadcast heartbeat, %d\n", rf.me, rf.currentTerm, rf.nextIndex)

	for i := range rf.peers {
		if i != rf.me {
			nextIndex := rf.nextIndex[i]
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  rf.safeLogEntryGetter(nextIndex - 2).Term,
				Entries:      rf.log[nextIndex-1:],
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)
		}
	}
}

type PersistentState struct {
	Term    int
	VoteFor int
	Log     []LogEntry
}

func (rf *Raft) encodePersistentState() []byte {
	state := PersistentState{
		Term:    rf.currentTerm,
		VoteFor: rf.votedFor,
		Log:     rf.log,
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(state)
	raftstate := w.Bytes()

	return raftstate
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.Save(rf.encodePersistentState(), nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state PersistentState
	if d.Decode(&state) != nil {
		panic("Error when decoding peristent states.")
	} else {
		rf.currentTerm = state.Term
		rf.votedFor = state.VoteFor
		rf.log = state.Log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %d [Term = %d] receive vote request from %d\n", rf.me, rf.currentTerm, args.CandidateId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.persist()
	}
	if rf.votedFor == -1 {
		if args.LastLogTerm > rf.LastLogTerm() || (args.LastLogTerm == rf.LastLogTerm() && args.LastLogIndex >= rf.LastLogIndex()) {
			rf.votedFor = args.CandidateId
			DPrintf("server %d [Term = %d] first vote to %d\n", rf.me, rf.currentTerm, args.CandidateId)
			rf.persist()
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = rf.votedFor == args.CandidateId
	if reply.VoteGranted {
		notify(rf.grantVoteCh)
		DPrintf("server %d [Term = %d] grant vote to %d\n", rf.me, rf.currentTerm, args.CandidateId)
	}
	// if args.Term == rf.currentTerm {
	// 	// already voted for someone
	// 	reply.Term = rf.currentTerm
	// 	reply.VoteGranted = rf.votedFor == args.CandidateId
	// } else {
	// 	rf.becomeFollower(args.Term)
	// 	rf.votedFor = args.CandidateId
	// 	reply.Term = rf.currentTerm
	// 	reply.VoteGranted = true
	// 	DPrintf("server %d [Term = %d] grant vote to %d\n", rf.me, args.CandidateId, rf.currentTerm)
	// 	notify(rf.grantVoteCh)
	// }
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm || rf.state == Candidate {
		rf.becomeFollower(args.Term)
		rf.persist()
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	reply.ConflictIndex = 0
	reply.ConflictTerm = -1
	notify(rf.validHeartbeatCh)

	if args.PrevLogIndex > 0 && (args.PrevLogIndex > rf.LastLogIndex() || rf.safeLogEntryGetter(args.PrevLogIndex-1).Term != args.PrevLogTerm) {
		reply.Success = false
		if args.PrevLogIndex > rf.LastLogIndex() {
			reply.ConflictIndex = rf.LastLogIndex() + 1
			return
		}
		reply.ConflictTerm = rf.safeLogEntryGetter(args.PrevLogIndex - 1).Term
		conflictIdx := args.PrevLogIndex
		for rf.safeLogEntryGetter(conflictIdx-1).Term == reply.ConflictTerm {
			conflictIdx--
		}
		reply.ConflictIndex = conflictIdx + 1
		return
	}

	firstAppendIndex := args.PrevLogIndex + 1
	lastIndex := rf.LastLogIndex()
	insertIndex := firstAppendIndex
	idx := 0
	// delete all the log entries mismatched
	for idx < len(args.Entries) && insertIndex <= lastIndex {
		if rf.safeLogEntryGetter(insertIndex-1).Term != args.Entries[idx].Term {
			rf.log = rf.log[:insertIndex-1]
			break
		}
		insertIndex++
		idx++
	}
	// case 1: delete conflict log
	// case 2: idx == len(args.Entries) || insertIndex > lastIndex
	for idx < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[idx])
		idx++
	}
	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.LastLogIndex())
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return ok
	}
	if rf.currentTerm > reply.Term || rf.state != Candidate || !reply.VoteGranted {
		return ok
	}
	rf.nVote += 1
	DPrintf("server %d [Term = %d] receive vote from %d, nVote: %d\n", rf.me, rf.currentTerm, server, rf.nVote)
	if rf.nVote > len(rf.peers)/2 {
		notify(rf.majorGrantCh)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
	}
	if rf.state != Leader {
		return ok
	}
	DPrintf("server %d [Term = %d] receive AppendEntries reply from %d, reply: %v\n", rf.me, rf.currentTerm, server, reply)
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.adjustLeaderCommitIndex()
		return ok
	}
	if reply.ConflictTerm < 0 {
		rf.nextIndex[server] = reply.ConflictIndex
	} else {
		var sameTermIndex int
		for sameTermIndex = args.PrevLogIndex; sameTermIndex > 0; sameTermIndex-- {
			if rf.safeLogEntryGetter(sameTermIndex-1).Term == reply.ConflictTerm {
				break
			}
		}
		if sameTermIndex > 0 { // found the log entry with the same term
			rf.nextIndex[server] = sameTermIndex + 1
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return index, term, false
	}

	index = rf.LastLogIndex() + 1
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	})
	rf.persist()
	notify(rf.broadcastCh)
	go rf.broadcast()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applier() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			logToApply := rf.safeLogEntryGetter(rf.lastApplied)
			rf.lastApplied += 1
			applyMsg := ApplyMsg{
				CommandValid:  true,
				Command:       logToApply.Command,
				CommandIndex:  logToApply.Index,
				SnapshotValid: false,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			DPrintf("server %d [Term = %d] apply log %d\n", rf.me, rf.currentTerm, logToApply.Index)
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.Lock()
		state := rf.state
		term := rf.currentTerm
		rf.mu.Unlock()

		switch state {
		case Follower:
			select {
			case <-time.After(randomTimeout(MinElectionTimeout, MaxElectionTimeout)):
				rf.startElection()
			case <-rf.grantVoteCh:
			case <-rf.validHeartbeatCh:
				DPrintf("server %d [Term = %d] receive heartbeat\n", rf.me, term)
			}
		case Candidate:
			select {
			case <-time.After(randomTimeout(MinElectionTimeout, MaxElectionTimeout)):
				rf.startElection()
			case <-rf.majorGrantCh:
				rf.becomeLeader()
			case <-rf.becomeFollowerCh:
				// become follower and exit election timer
			}
		case Leader:
			select {
			case <-time.After(HeartbeatInterval):
				// broadcast heartbeat
				rf.broadcast()
			case <-rf.becomeFollowerCh:
				// become follower and reset heartbeat
			case <-rf.broadcastCh:
				// reset heartbeat
			}
		default:
			panic("unexpected state")
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	// Your initialization code here (2A, 2B, 2C).
	rf.becomeFollowerCh = make(chan struct{})
	rf.validHeartbeatCh = make(chan struct{})
	rf.grantVoteCh = make(chan struct{})
	rf.majorGrantCh = make(chan struct{})
	rf.broadcastCh = make(chan struct{})
	rf.applyCh = applyCh

	rf.state = Follower
	rf.nVote = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
