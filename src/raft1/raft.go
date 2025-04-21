package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"fmt"

	// "encoding/gob"

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// CONSTANTS
const heartbeatTime = time.Duration(50)
const maxEntries = 50

// A Go object implementing a single Raft peer.
type Raft struct {
	mu         sync.Mutex // Lock to protect shared access to this peer's state
	eventCh    chan struct{}
	commitCond sync.Cond
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *tester.Persister   // Object to hold this peer's persisted state
	me         int                 // this peer's index into peers[]
	dead       int32               // set by Kill()

	// state
	state string

	// Election related
	currentTerm int
	votedFor    int
	leaderId    int

	// Log related
	logs         []Log
	lastLogIndex int
	lastLogTerm  int
	commitIndex  int
	lastApplied  int
}

type Log struct {
	Term    int
	Command interface{}
}

type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Logs        []Log
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.leaderId == rf.me
}

// Transition to follower state
func (rf *Raft) updateStaleState(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.leaderId = -1
	rf.state = "follower"
	rf.eventCh <- struct{}{}
	rf.persist()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	state := PersistentState{rf.currentTerm, rf.votedFor, rf.logs}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(state)
	if err != nil {
		log.Fatalf("Raft [%d]: Error encoding state: %v", rf.me, err)
	}

	raftstate := w.Bytes()

	// Really inefficient, we resave the entire state instead of just changes
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	var state PersistentState

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	err := d.Decode(&state)
	if err != nil {
		log.Fatalf("Raft [%d]: Error decoding state: %v", rf.me, err)
	}

	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.logs = state.Logs
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {

}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// Participate in election, potentially update stale state
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// CASE: they have old term (>)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// CASE: already voted (=)
	if rf.currentTerm == args.Term && rf.votedFor != -1 {
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
		return
	}
	// CASE: new term (<)
	if rf.currentTerm < args.Term {
		rf.updateStaleState(args.Term)
	}
	<-rf.eventCh

	// VOTE: at least as old history
	if (args.LastLogTerm > rf.lastLogTerm) || (args.LastLogTerm == rf.lastLogTerm && args.LastLogIndex >= rf.lastLogIndex) {
		rf.votedFor = args.CandidateId
		rf.persist() // Save vote

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}
	// DON'T VOTE: history not up to date with ours
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []Log

	leaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Replicate leaders logs
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// CASE: they have old term (>)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// CASE: new term (<), or canidate
	if rf.currentTerm < args.Term || rf.state == "canidate" {
		rf.updateStaleState(args.Term)
	}
	<-rf.eventCh
	// Update commit index and leader id
	newCommit := min(rf.lastLogIndex, args.leaderCommit)
	if rf.commitIndex != newCommit {
		rf.commitIndex = newCommit
		rf.commitCond.Signal()
	}
	rf.leaderId = args.LeaderId

	// If we don't have the prev entry, flag failure
	if rf.lastLogIndex < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// REPLICATE LOGS: If we have the prev entry, append the new entries given, then respond

	// If we have any entries past the prev entry, remove all of them
	if rf.lastLogIndex > args.PrevLogIndex {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
	}

	// replicate logs
	if len(args.Entries) > 0 {
		rf.logs = append(rf.logs, args.Entries...)
		rf.lastLogIndex += len(args.Entries)
		rf.lastLogTerm = args.Term
		rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

// Heartbeat to followers, replicate our logs
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.lastLogIndex + 1
	term, isLeader := rf.GetState()

	// Append to our log
	if isLeader {
		rf.lastLogIndex += 1
		rf.lastLogTerm = term
		rf.logs = append(rf.logs,
			Log{
				Term:    rf.currentTerm,
				Command: command,
			},
		)
		rf.persist()
	}

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	rf.mu.Lock()
	for !rf.killed() {
		fmt.Printf("Raft [%d] State: %v\n", rf.me, rf.state)
		switch rf.state {

		// *** FOLLOWER STATE ***
		case "follower":
			// Set timeout for a random amount of time between 100 and 350 milliseconds.
			rf.mu.Unlock()
			ms := 100 + (rand.Int63() % 300)
			followerTimeout := time.Duration(ms) * time.Millisecond

			select {
			case <-rf.eventCh:
			case <-time.After(followerTimeout):
				// If we haven't heard from anyone start a new election
				rf.mu.Lock()
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.leaderId = -1
				rf.state = "candidate"
				rf.persist()
				rf.mu.Unlock()
			}
			rf.mu.Lock()

		// *** CANIDATE STATE ***
		case "candidate":
			rf.mu.Unlock()
			// Set timeout for a random amount of time between 150 and 350 milliseconds.
			ms := 150 + (rand.Int63() % 200)
			electionTimeout := time.Duration(ms) * time.Millisecond

			go rf.executeCanidateRoutine() // Gather votes

			select {
			case <-rf.eventCh:
			case <-time.After(electionTimeout):
				// If we haven't heard from anyone start a new election
				rf.mu.Lock()
				rf.currentTerm += 1
				rf.persist()
				rf.mu.Unlock()
			}
			rf.mu.Lock()

		// *** LEADER STATE ***
		case "leader":
			rf.mu.Unlock()
			go rf.executeLeaderRoutine()
			<-rf.eventCh
			rf.mu.Lock()
		}
	}
}

// Candidate routine: try to gather votes and become the leader
func (rf *Raft) executeCanidateRoutine() {
	// Initialize canidate state
	peersLeft := make(map[int]struct{})
	votes := 1

	for i := range len(rf.peers) {
		peersLeft[i] = struct{}{}
	}
	delete(peersLeft, rf.me)

	// Set up RPC arguments
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm:  rf.lastLogTerm,
	}
	rf.mu.Unlock()

	// Keep requesting votes until state change
	for !rf.killed() {
		// Check for state change
		rf.mu.Lock()
		if rf.state != "candidate" || rf.currentTerm != args.Term {
			return
		}

		// If we have enough votes become leader
		if votes > (len(rf.peers) / 2) {
			rf.state = "leader"
			rf.leaderId = rf.me
			rf.eventCh <- struct{}{}
			rf.mu.Unlock()
			return
		}

		// Request votes from remaining peers
		for i := range peersLeft {
			go func(server int) {
				// Send vote request
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				// Check for stale state
				if rf.currentTerm > args.Term || rf.state != "candidate" || rf.currentTerm < reply.Term {
					// If we have a stale term, update it
					if rf.currentTerm < reply.Term {
						rf.updateStaleState(reply.Term)
					}
					return
				}

				// Register peers vote
				delete(peersLeft, server)
				if reply.VoteGranted {
					votes += 1
				}
			}(i)
		}

		rf.mu.Unlock()
		// Wait 1 hearbeat and resend
		time.Sleep(heartbeatTime)
	}
}

// Leader routine: send all peers heartbeats
func (rf *Raft) executeLeaderRoutine() {
	// Initialize leader state
	rf.mu.Lock()
	term := rf.currentTerm
	nextCommit := rf.lastLogIndex + 1
	nextIndex := make([]int, len(rf.peers))
	matchIndex := make([]int, len(rf.peers))

	for i := range len(rf.peers) {
		nextIndex[i] = rf.lastLogIndex + 1
	}
	rf.mu.Unlock()

	// Periodically send heartbeat AppendEntires RPC
	for !rf.killed() {
		// Check for state change
		rf.mu.Lock()
		if rf.state != "leader" || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		//  *** Find new commit index ***

		// Majority rules: If the most recent-term logs hit a majority
		// they are always persistent since future leaders must have them to be elected.

		// We can only commit entries from our term, if we reach a majority on an old term
		// That is not grounds to commit it since, more recent term data can overule it, if
		// it didn't reach a majority during its term.
		prevCommit := rf.commitIndex
		for {
			count := 1
			for _, peerIndex := range matchIndex {
				if peerIndex >= nextCommit {
					count++
				}
			}
			if count < len(rf.peers)/2 {
				break
			}
			rf.commitIndex = nextCommit
			nextCommit++
		}
		if prevCommit != rf.commitIndex {
			rf.commitCond.Signal()
		}

		// Heartbeat everyone
		for i := range len(rf.peers) {
			// Don't send RPC to ourselves
			if i == rf.me {
				continue
			}

			// Set up arguments
			prevLogIndex := nextIndex[i] - 1
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,

				PrevLogTerm:  rf.logs[prevLogIndex].Term,
				Entries:      make([]Log, 0),
				leaderCommit: rf.commitIndex,
			}

			// If we've found the match index, send logs (up to some max), for follower to replicate
			if matchIndex[i] == prevLogIndex {
				for logIndex := prevLogIndex + 1; logIndex <= min(prevLogIndex+maxEntries, rf.lastLogIndex); logIndex++ {
					args.Entries = append(args.Entries, rf.logs[logIndex])
				}
			}

			go func(server int, args *AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				// Check for stale term
				if rf.currentTerm > term || rf.state != "leader" || rf.currentTerm < reply.Term {
					// If we have a stale term, update it
					if rf.currentTerm < reply.Term {
						rf.updateStaleState(reply.Term)
					}
					return
				}

				// If not success we haven't found the match index yet, decrement nextIndex
				if !reply.Success {
					nextIndex[server]--
				} else {
					// Increment the next index, set match index
					nextIndex[server] += len(args.Entries)
					matchIndex[server] = nextIndex[server] - 1
				}
			}(i, args)
		}

		rf.mu.Unlock()
		// Wait 1 hearbeat and resend
		time.Sleep(heartbeatTime)
	}
}

func (rf *Raft) applyLogsRoutine() {
	for !rf.killed() {
		rf.commitCond.Wait()

		// Apply commited logs
		rf.mu.Lock()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		rf.lastApplied = commitIndex
		rf.mu.Unlock()
		for i := lastApplied + 1; i <= commitIndex; i++ {

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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	fmt.Printf("Starting Raft [%d]\n", me)
	rf := &Raft{
		mu:         sync.Mutex{},
		eventCh:    make(chan struct{}),
		commitCond: sync.Cond{},
		peers:      peers,
		persister:  persister,
		me:         me,
		dead:       0,

		state: "follower",

		currentTerm: 0,
		votedFor:    -1,
		leaderId:    -1,

		lastLogIndex: -1,
		lastLogTerm:  -1,
		commitIndex:  -1,
		lastApplied:  -1,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLogsRoutine()

	return rf
}
