package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"

	"fmt"
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
const maxSnapshotBytes = 4096

// TIMING CONSTANTS/MACROS
const (
	heartbeatInterval  = 10 * time.Millisecond
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
	logs         []Log // PERSISTENT
	lastLogIndex int
	lastLogTerm  int

	commitIndex int
	lastApplied int

	// Leader State
	nextIndex  []int
	matchIndex []int
	offsets    []int // Snapshot

	// Snapshot
	snapshot      []byte // PERSISTENT
	snapshotIndex int    // PERSISTENT
	snapshotTerm  int    // PERSISTENT

	// Temporary Snapshot
	tmpSnapshot      []byte
	tmpSnapshotIndex int
	installTerm      int
}

type Log struct {
	Term    int
	Command interface{}
}

type PersistentState struct {
	CurrentTerm int
	VotedFor    int

	Logs []Log

	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.leaderId == rf.me
}

// Get actual log index
func (rf *Raft) logIndex(index int) int {
	return index - rf.snapshotIndex
}

// Non blocking channel signal
func (rf *Raft) signalEvent() {
	// When sending (e.g., in updateStaleState or elsewhere):
	// Try to send, but don't block if the buffer is full (meaning a signal is already pending)
	select {
	case rf.eventCh <- struct{}{}:
		// Signal sent successfully
	default:
		// Signal already pending, do nothing (the ticker will wake up eventually)
	}
}

// Transition to follower state for the current term, we didn't vote in this election (yet).
func (rf *Raft) updateStaleState(term, leaderId int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.leaderId = leaderId
	rf.state = "follower"
	rf.signalEvent()
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
	state := PersistentState{rf.currentTerm, rf.votedFor, rf.logs, rf.snapshot, rf.snapshotIndex, rf.snapshotTerm}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(state)
	if err != nil {
		log.Fatalf("Raft [%d] (%d): Error encoding state: %v", rf.me, rf.currentTerm, err)
	}

	raftstate := w.Bytes()

	rf.persister.Save(raftstate, rf.snapshot)
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

	rf.snapshot = state.Snapshot
	rf.snapshotIndex = state.SnapshotIndex
	rf.snapshotTerm = state.SnapshotTerm

	rf.lastLogIndex = len(rf.logs) + rf.snapshotIndex - 1
	if len(rf.logs) > 1 {
		rf.lastLogTerm = rf.logs[rf.logIndex(rf.lastLogIndex)].Term
	} else {
		rf.lastLogTerm = state.SnapshotTerm
	}

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
	rf.mu.Lock()
	fmt.Printf("Raft [%d] (%d): Adding Snapshot: %d\n", rf.me, rf.currentTerm, index)
	defer rf.mu.Unlock()
	// CASE: bad snapshot
	if index <= rf.snapshotIndex {
		return
	}

	// Truncate logs
	rf.logs = rf.logs[rf.logIndex(index):]

	// Save snapshot
	rf.snapshot = snapshot
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.logs[0].Term

	rf.logs[0] = Log{0, struct{}{}}

	// Save
	rf.persist()
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
		rf.signalEvent()
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
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool
// (Deleted)

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

func (rf *Raft) removeConflicts(conflictIndex int) {
	rf.logs = rf.logs[:rf.logIndex(conflictIndex)]
	rf.lastLogIndex = conflictIndex - 1
	rf.lastLogTerm = rf.logs[rf.logIndex(rf.lastLogIndex)].Term
	rf.persist()
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
		rf.signalEvent()
	}

	// If we don't have the prev entry, flag failure
	if rf.lastLogIndex < args.PrevLogIndex ||
		(rf.snapshotIndex < args.PrevLogIndex && rf.logs[rf.logIndex(args.PrevLogIndex)].Term != args.PrevLogTerm) {

		// Set response
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// REPLICATE LOGS: we have the prev entry, append the new entries given, then respond

	// Check for conflicts, skip ones we have the same, remove all that aren't the same
	entryIndex := 0
	for _, entry := range args.Entries {
		index := (args.PrevLogIndex + 1) + entryIndex

		if index > rf.lastLogIndex {
			// CASE 1: END of logs, no conflicts
			break

		} else if rf.logs[rf.logIndex(index)].Term != entry.Term {
			// CASE 2:  CONFLICT appeared, remove all entries past that
			rf.removeConflicts(index)
			break
		}

		// CASE 3: CORRECT log here
		entryIndex++
	}

	// Print new logs added
	for i := entryIndex; i < len(args.Entries); i++ {
		fmt.Printf("Raft [%d] (%d) Log Recieved: (%d, %d)\n", rf.me, rf.currentTerm, rf.lastLogIndex+i-entryIndex+1, args.Entries[i].Term)
	}

	// Append rest of the logs (no conflicts past this point)
	if entryIndex < len(args.Entries) {
		rf.logs = append(rf.logs, args.Entries[entryIndex:]...)
		rf.lastLogIndex += len(args.Entries) - entryIndex
		rf.lastLogTerm = rf.logs[rf.logIndex(rf.lastLogIndex)].Term
		rf.persist()
	}

	// Update commit index
	if args.LeaderCommit > rf.commitIndex {
		newCommit := min(args.LeaderCommit, rf.lastLogIndex) // Only commit up to what we have replicated
		// Update if larger and signal on new commit index
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.commitCond.Signal()
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true
}

// Send AppendEntries to followers, handle the response
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs) {
	// Send RPC
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("Raft [%d] (%d) Heartbeat Response: [%d]\n", rf.me, rf.currentTerm, server)
	// Check for stale term
	if rf.currentTerm > args.Term || rf.state != "leader" || rf.currentTerm < reply.Term {
		// If we have a stale term, update it
		if rf.currentTerm < reply.Term {
			rf.updateStaleState(reply.Term, -1)
		}
		return
	}

	// If not success we haven't found the match index yet, decrement nextIndex
	// We can use the matchIndex if its found, since it will always have the latest index (since it increases monotonically)
	if !reply.Success {
		// Decrement (only if we haven't found the match index)
		rf.nextIndex[server] = max(args.PrevLogIndex, rf.matchIndex[server]+1)
	} else {
		// Strictly increment the next index
		rf.nextIndex[server] = max(args.PrevLogIndex+1+len(args.Entries), rf.nextIndex[server])
		// Set match index
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}
}

type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int

	Offset int
	Data   []byte
	Done   bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// CASE: they have old term (>)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	// CASE: new term (<)
	if rf.currentTerm < args.Term {
		rf.updateStaleState(args.Term, args.LeaderId)
	} else {
		// Set leaderId (if isn't already), reset follower state
		rf.leaderId = args.LeaderId
		rf.state = "follower"
		rf.signalEvent()
	}
	reply.Term = rf.currentTerm // Set reply here

	// Install snapshot if it is newer than existing snapshot
	if rf.snapshotIndex > args.LastIncludedIndex {
		return
	}

	// Delete old existing temporary snapshot
	if rf.tmpSnapshot != nil && (rf.installTerm != rf.currentTerm || rf.tmpSnapshotIndex != args.LastIncludedIndex) {
		rf.tmpSnapshot = nil
	}

	if args.Offset == 0 {
		// Create new snapshot
		rf.tmpSnapshot = make([]byte, 0)
		rf.tmpSnapshotIndex = args.LastIncludedIndex
		rf.installTerm = args.LastIncludedTerm
	} else if args.Offset != len(rf.tmpSnapshot) {
		// Already applied or out of order
		return
	}

	// Install snapshot segment
	rf.tmpSnapshot = append(rf.tmpSnapshot, args.Data...)

	// Save finished snaphots
	if args.Done {
		// Truncate logs
		if rf.lastLogIndex > rf.tmpSnapshotIndex {
			// Truncate
			rf.logs = rf.logs[rf.logIndex(rf.tmpSnapshotIndex):]
			rf.logs[0] = Log{0, struct{}{}}
		} else {
			// Reset logs
			rf.logs = []Log{{0, struct{}{}}}
			rf.lastLogIndex = rf.tmpSnapshotIndex
			rf.lastLogTerm = args.LastIncludedTerm
		}

		// Save snapshot
		rf.snapshot = rf.tmpSnapshot
		rf.snapshotIndex = rf.tmpSnapshotIndex
		rf.snapshotTerm = args.LastIncludedTerm

		// Delete tmp snapshot
		rf.tmpSnapshot = nil

		// Save
		rf.persist()
	}

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

	// Append to our log
	if rf.leaderId == rf.me {
		rf.lastLogIndex += 1
		rf.lastLogTerm = rf.currentTerm
		rf.logs = append(rf.logs,
			Log{
				Term:    rf.currentTerm,
				Command: command,
			},
		)
		rf.persist()
		fmt.Printf("Raft [%d] (%d) Command Recieved: (%d, %d)\n", rf.me, rf.currentTerm, rf.lastLogIndex, rf.lastLogTerm)
		return rf.lastLogIndex, rf.currentTerm, true
	} else {
		return -1, rf.currentTerm, false
	}
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
				ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
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
				// Check if they already voted
				if _, in := peersLeft[server]; !in {
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
						rf.signalEvent()
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
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.offsets = make([]int, len(rf.peers))

	for i := range len(rf.peers) {
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.offsets[i] = -1
	}

	// Heartbeat AppendEntires RPC until state change
	for !rf.killed() && rf.state == "leader" && rf.currentTerm == term {
		// fmt.Printf("Raft [%d] (%d): Leader Heartbeat\n", rf.me, rf.currentTerm)
		// Update commit index
		rf.updateCommitIndex()

		// Heartbeat everyone
		for i := range len(rf.peers) {
			// Don't send RPC to ourselves or peers installing snapshots
			if i == rf.me || rf.offsets[i] != -1 {
				continue
			}

			prevLogIndex := rf.nextIndex[i] - 1
			// If the follower doesn't have entries before the snapshot, send them our snapshot
			if prevLogIndex < rf.snapshotIndex {
				rf.offsets[i] = 0
				go rf.sendSnapshot(i, term)
				continue
			}
			// Otherwise send a AppendEntries RPC

			// Set up arguments
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,

				PrevLogTerm:  rf.logs[rf.logIndex(prevLogIndex)].Term,
				Entries:      make([]Log, 0),
				LeaderCommit: rf.commitIndex,
			}

			// Send logs after snapshot (if any left, up to some max), for follower to replicate
			if prevLogIndex >= rf.snapshotIndex && prevLogIndex < rf.lastLogIndex {
				endLogIndex := min(prevLogIndex+maxEntries, rf.lastLogIndex)
				args.Entries = append(args.Entries, rf.logs[rf.logIndex(prevLogIndex+1):rf.logIndex(endLogIndex+1)]...)
			}

			// Send RPC
			go rf.sendAppendEntries(i, args)
		}
		// Wait 1 hearbeat and resend
		rf.mu.Unlock()
		time.Sleep(heartbeatInterval)
		rf.mu.Lock()
	}
}

// Handle sending the full snapshot to the server that needs it
func (rf *Raft) sendSnapshot(server int, term int) {
	rf.mu.Lock()
	fmt.Printf("Raft [%d] (%d): Sending Snapshot: [%d]\n", rf.me, rf.currentTerm, server)
	defer func() {
		rf.offsets[server] = -1
	}()
	defer rf.mu.Unlock()

	// Safety Check: verify term before modifying state
	for !rf.killed() && rf.currentTerm == term {
		offset := rf.offsets[server]

		var done bool
		var data []byte

		// Check if this is the last one, get data and done
		if len(rf.snapshot)-offset <= maxSnapshotBytes {
			data = rf.snapshot[offset:]
			done = true
		} else {
			data = rf.snapshot[offset : offset+maxSnapshotBytes]
			done = false
		}

		args := InstallSnapshotArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.leaderId,

			LastIncludedIndex: rf.snapshotIndex,
			LastIncludedTerm:  rf.snapshotTerm,

			Offset: offset,
			Data:   data,
			Done:   done,
		}

		// Send InstallSnapshot RPC
		rf.mu.Unlock() // Unlock before sending
		reply := RequestVoteReply{}
		ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

		// If it didn't recieve the message try again
		if !ok {
			continue
		}

		rf.mu.Lock()
		// Check for stale state
		if rf.currentTerm != term || rf.currentTerm < reply.Term {
			// If we have a stale term, update it
			if rf.currentTerm < reply.Term {
				rf.updateStaleState(term, -1)
			}
			return
		}

		// Increment offset if more left, otherwise leave
		if !done {
			rf.offsets[server] += maxSnapshotBytes
		} else {
			return
		}
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

	// Search for a new commit index
	for N := rf.lastLogIndex; N > rf.commitIndex; N-- {
		// Verify index is in current term
		if rf.logs[rf.logIndex(N)].Term != rf.currentTerm {
			break
		}

		count := 1 // Our match index is 0, count it here
		for _, index := range rf.matchIndex {
			if index >= N {
				count++
			}
		}

		if count >= (len(rf.peers)+1)/2 {
			rf.commitIndex = N
			break
		}
	}

	// If we updated the commit index, notify the applyLogsRoutine
	if prevCommit != rf.commitIndex {
		// fmt.Printf("Raft [%d] (%d) Commit Index: %d\n", rf.me, rf.currentTerm, rf.commitIndex)
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

		// Apply snapshot (if needed)
		if rf.lastApplied+1 <= rf.snapshotIndex {
			rf.applyCh <- raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.snapshotTerm,
				SnapshotIndex: rf.snapshotIndex,
			}
			rf.lastApplied = rf.snapshotIndex
		}

		// Apply logs (if any)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			// fmt.Printf("Raft [%d] (%d) Applying Log: %d\n", rf.me, rf.currentTerm, i)
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.logIndex(i)].Command, // Uncommited logs are read only, no locking needed
				CommandIndex: i,
			}
		}

		rf.lastApplied = rf.commitIndex
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
	fmt.Printf("Starting Raft [%d]\n", me)

	// Initialize raft object
	rf := &Raft{
		mu:      sync.Mutex{},
		eventCh: make(chan struct{}, 1),

		peers:     peers,
		persister: persister,
		applyCh:   applyCh,
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

		snapshot:      nil,
		snapshotIndex: 0,
		snapshotTerm:  0,
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
