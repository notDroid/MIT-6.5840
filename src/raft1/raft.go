package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	// "fmt"

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

// *** CONSTANTS ***
const maxEntries = 50

// TIMING CONSTANTS/MACROS
const (
	heartbeatInterval  = 50 * time.Millisecond
	electionMinTimeout = 15 * heartbeatInterval
)

// electionTimeout returns a randomized timeout in [electionMinTimeout, 2*electionMinTimeout).
func electionTimeout() time.Duration {
	jitter := time.Duration(rand.Int63n(int64(electionMinTimeout)))
	return electionMinTimeout + jitter
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// *** State Management ***

	// Concurrency Control
	mu         sync.Mutex
	eventCh    chan struct{}
	commitCond sync.Cond

	// Provided info + tools
	peers     []*labrpc.ClientEnd   // RPC end points of all peers
	persister *tester.Persister     // Object to hold this peer's persisted state
	applyCh   chan raftapi.ApplyMsg // Apply messages through here
	me        int                   // this peer's index into peers[]

	dead int32 // set by Kill()

	// *** State Variables ***

	// Current State String
	state string

	// Election State
	currentTerm int // PERSISTENT
	votedFor    int // PERSISTENT
	leaderId    int

	// Log State
	logs         []Log
	lastLogIndex int
	lastLogTerm  int
	commitIndex  int
	lastApplied  int

	// Leader State
	nextIndex  []int
	matchIndex []int
	nextCommit int
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.leaderId == rf.me
}

// Transition to follower state for the current term, we didn't vote in this election (yet).
func (rf *Raft) updateStaleState(term, leaderId int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.leaderId = leaderId
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
		log.Fatalf("Raft [%d] (%d): Error encoding state: %v", rf.me, rf.currentTerm, err)
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
		log.Fatalf("Raft [%d] (%d): Error decoding state: %v", rf.me, rf.currentTerm, err)
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
		rf.updateStaleState(args.Term, -1)
	}

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

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Replicate leaders logs
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("Raft [%d] (%d) Recieved Heartbeat: [%d]\n", rf.me, rf.currentTerm, args.LeaderId)
	// CASE: they have old term (>)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// CASE: new term (<)
	if rf.currentTerm < args.Term {
		rf.updateStaleState(args.Term, args.LeaderId)
	} else {
		// Set leaderId (if isn't already), reset follower state
		rf.leaderId = args.LeaderId
		rf.state = "follower"
		rf.eventCh <- struct{}{}
	}

	// Update commit index and leader id
	newCommit := min(rf.lastLogIndex, args.LeaderCommit)
	// Update and signal on new commit index
	if rf.commitIndex != newCommit {
		rf.commitIndex = newCommit
		rf.commitCond.Signal()
	}

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
	term, isLeader := rf.currentTerm, rf.leaderId == rf.me

	// Append to our log
	if rf.leaderId == rf.me {
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
	defer rf.mu.Unlock()
	for !rf.killed() {
		// fmt.Printf("Raft [%d] (%d) State: %v\n", rf.me, rf.currentTerm, rf.state)
		switch rf.state {

		// *** FOLLOWER STATE ***
		case "follower":
			rf.mu.Unlock()

			// Set timeout timer
			timer := time.NewTimer(electionTimeout())

			select {
			case <-rf.eventCh:
			case <-timer.C:
				// If we haven't heard from anyone start a new election
				rf.mu.Lock()
				// fmt.Printf("Raft [%d] (%d): Follower Timeout\n", rf.me, rf.currentTerm)
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
			go rf.executeCanidateRoutine(rf.currentTerm) // Gather votes
			rf.mu.Unlock()

			// Set timeout timer
			timer := time.NewTimer(electionTimeout())

			select {
			case <-rf.eventCh:
			case <-timer.C:
				// If we haven't heard from anyone start a new election
				rf.mu.Lock()
				// fmt.Printf("Raft [%d] (%d): Election Timeout\n", rf.me, rf.currentTerm)
				rf.currentTerm += 1
				rf.persist()
				rf.mu.Unlock()
			}
			rf.mu.Lock()

		// *** LEADER STATE ***
		case "leader":
			go rf.executeLeaderRoutine(rf.currentTerm)
			rf.mu.Unlock()
			<-rf.eventCh
			rf.mu.Lock()
		}
	}
}

// Candidate routine: try to gather votes and become the leader
func (rf *Raft) executeCanidateRoutine(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Safety Check: verify term before modifying state
	if term != rf.currentTerm {
		return
	}

	// Initialize canidate state
	peersLeft := make(map[int]struct{})

	for i := range len(rf.peers) {
		peersLeft[i] = struct{}{}
	}

	// Vote for ourselves
	votes := 1
	rf.votedFor = rf.me
	delete(peersLeft, rf.me)

	// Set up RPC args
	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm:  rf.lastLogTerm,
	}

	// Keep requesting votes until state change
	for !rf.killed() && rf.state == "candidate" && rf.currentTerm == term {
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
				if rf.currentTerm != term || rf.state != "candidate" || rf.currentTerm < reply.Term {
					// If we have a stale term, update it
					if rf.currentTerm < reply.Term {
						rf.updateStaleState(term, -1)
					}
					return
				}

				// Register peers vote
				delete(peersLeft, server)
				if reply.VoteGranted {
					votes += 1
					// fmt.Printf("Raft [%d] (%d) Recieved Vote: [%d]\n", rf.me, rf.currentTerm, server)

					// If we have enough votes become leader
					if votes > (len(rf.peers) / 2) {
						rf.state = "leader"
						rf.leaderId = rf.me
						rf.eventCh <- struct{}{}
						return
					}
				}
			}(i)
		}

		// Wait 1 hearbeat and resend
		rf.mu.Unlock()
		time.Sleep(heartbeatInterval)
		rf.mu.Lock()
	}
}

// Leader routine: send all peers heartbeats
func (rf *Raft) executeLeaderRoutine(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Safety Check: verify term before modifying state
	if term != rf.currentTerm {
		return
	}

	// Initialize leader state
	rf.nextCommit = rf.lastLogIndex + 1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// Initialize nextIndex to lastLogIndex + 1
	for i := range len(rf.peers) {
		rf.nextIndex[i] = rf.lastLogIndex + 1
	}

	// Heartbeat AppendEntires RPC until state change
	for !rf.killed() && rf.state == "leader" && rf.currentTerm == term {
		// fmt.Printf("Raft [%d] (%d): Leader Heartbeat\n", rf.me, rf.currentTerm)
		// Update commit index
		rf.updateCommitIndex()

		// Heartbeat everyone
		for i := range len(rf.peers) {
			// Don't send RPC to ourselves
			if i == rf.me {
				continue
			}

			// Set up arguments
			prevLogIndex := rf.nextIndex[i] - 1
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,

				PrevLogTerm:  rf.logs[prevLogIndex].Term,
				Entries:      make([]Log, 0),
				LeaderCommit: rf.commitIndex,
			}

			// If we've found the match index, send logs (if any left, up to some max), for follower to replicate
			if rf.matchIndex[i] == prevLogIndex && prevLogIndex < rf.lastLogIndex {
				endLogIndex := min(prevLogIndex+maxEntries, rf.lastLogIndex)
				args.Entries = append(args.Entries, rf.logs[prevLogIndex+1:endLogIndex+1]...)
			}

			// Send RPC
			go func(server int, args AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				// fmt.Printf("Raft [%d] (%d) Heartbeat Response: [%d]\n", rf.me, rf.currentTerm, server)
				// Check for stale term
				if rf.currentTerm > term || rf.state != "leader" || rf.currentTerm < reply.Term {
					// If we have a stale term, update it
					if rf.currentTerm < reply.Term {
						rf.updateStaleState(reply.Term, -1)
					}
					return
				}

				// If not success we haven't found the match index yet, decrement nextIndex
				if !reply.Success {
					rf.nextIndex[server]--
				} else {
					// Increment the next index, set match index
					rf.nextIndex[server] += len(args.Entries)
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				}
			}(i, args)
		}

		// Wait 1 hearbeat and resend
		rf.mu.Unlock()
		time.Sleep(heartbeatInterval)
		rf.mu.Lock()
	}
}

// Find new commit index
//
// Majority rules: If the most recent-term logs hit a majority
// they are always persistent since future leaders must have them to be elected.
//
// We can only commit entries from our term, if we reach a majority on an old term
// That is not grounds to commit it since, more recent term data can overule it, if
// it didn't reach a majority during its term.
func (rf *Raft) updateCommitIndex() {
	prevCommit := rf.commitIndex

	// Increment commit index until less than half have the index.
	for rf.commitIndex <= rf.lastLogIndex {
		count := 1 // matchIndex[me] = 0, so count it here.
		for _, peerIndex := range rf.matchIndex {
			if peerIndex >= rf.nextCommit {
				count++
			}
		}
		if count < (len(rf.peers)+1)/2 {
			break
		}

		// Update commit index
		rf.commitIndex = rf.nextCommit
		rf.nextCommit++
	}

	// If we updated the commit index, notify the applyLogsRoutine
	if prevCommit != rf.commitIndex {
		rf.commitCond.Signal()
	}
}

func (rf *Raft) applyLogsRoutine() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		// Wait for event
		for rf.commitIndex == rf.lastApplied && !rf.killed() {
			rf.commitCond.Wait()
		}

		// Exit on killed
		if rf.killed() {
			return
		}

		// Take snapshot of current commit index, last applied
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		rf.mu.Unlock()

		// Apply the logs without locking
		for i := lastApplied + 1; i <= commitIndex; i++ {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command, // Commited indices logs aren't modified so this is safe
				CommandIndex: i,
			}
		}
		// Relock and increment lastApplied
		rf.mu.Lock()
		rf.lastApplied = commitIndex
	}
}

func init() {
	// Register empty struct into gob
	labgob.Register(struct{}{})
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
func Make(peers []*labrpc.ClientEnd, me int, persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	// fmt.Printf("Starting Raft [%d]\n", me)

	// Initialize raft object
	rf := &Raft{
		mu:      sync.Mutex{},
		eventCh: make(chan struct{}),

		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,

		state: "follower",

		currentTerm: 0,
		votedFor:    -1,
		leaderId:    -1,

		lastLogIndex: 0,
		lastLogTerm:  0,
		commitIndex:  0,
		lastApplied:  0,
	}
	rf.commitCond = *sync.NewCond(&rf.mu)

	// Load persisted state, if any
	rf.readPersist(persister.ReadRaftState())
	// Initialize empty log (if not already)
	if rf.lastLogIndex == 0 {
		rf.logs = []Log{{0, struct{}{}}}
	}

	// start ticker goroutine to start elections, start apply logs routine
	go rf.ticker()
	go rf.applyLogsRoutine()

	return rf
}
