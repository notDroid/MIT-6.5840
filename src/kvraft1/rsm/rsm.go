package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1
const waitTime = 100 * time.Millisecond

type Op struct {
}

type retWait struct {
	successCh chan bool
	ret       any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu      sync.Mutex
	me      int
	rf      raftapi.Raft
	applyCh chan raftapi.ApplyMsg
	sm      StateMachine

	// Track raft state and requests
	ops      map[int]*retWait // index -> Op
	term     int
	isLeader bool

	// Snapshotting metadata
	maxraftstate  int // snapshot if log grows this big
	snapshotIndex int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg, 100),
		sm:           sm,

		ops:      make(map[int]*retWait),
		term:     0,
		isLeader: false,

		snapshotIndex: 0,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.HandleOps()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) checkLeader(term int, isLeader bool) {
	// Flush waiting
	if term > rsm.term {
		rsm.flushWaiting()
		rsm.term = term
		rsm.isLeader = false
	}

	// Strictly turn on isLeader (if isLeader)
	if rsm.term == term && !rsm.isLeader {
		rsm.isLeader = isLeader
	}
}

func (rsm *RSM) HandleOps() {
	for applyMsg := range rsm.applyCh {
		// Apply operation
		if applyMsg.SnapshotValid {
			rsm.sm.Restore(applyMsg.Snapshot)
			continue
		}
		// fmt.Printf("S%d: Applying command at index %d: %+v\n", rsm.me, applyMsg.CommandIndex, applyMsg.Command)
		ret := rsm.sm.DoOp(applyMsg.Command)

		// Provide return value if there are waiters
		index := applyMsg.CommandIndex
		rsm.handleReturns(index, ret)
		// fmt.Printf("S%d: Finished applying command at index %d\n", rsm.me, applyMsg.CommandIndex)

		// Snapshot if logs exceed limit

		if rsm.maxraftstate != -1 && rsm.Raft().PersistBytes() > rsm.maxraftstate {
			rsm.snapshotIndex = index
			rsm.Raft().Snapshot(index, rsm.sm.Snapshot())
		}
	}
	// Release anyone still waiting
	rsm.mu.Lock()
	rsm.flushWaiting()
	rsm.mu.Unlock()
}

func (rsm *RSM) handleReturns(index int, ret any) {
	term, isLeader := rsm.Raft().GetState()
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	// Check leader
	rsm.checkLeader(term, isLeader)

	// See if we have a waiting op to wake up
	if op, exists := rsm.ops[index]; exists {
		// Provide return, then wake it up
		op.ret = ret
		op.successCh <- true
		delete(rsm.ops, index)
	}
}

// Invalidate waiting Sumbits and reset waiting op map
func (rsm *RSM) flushWaiting() {
	// Report failure to waiting ops
	for _, op := range rsm.ops {
		op.successCh <- false
	}
	// Reset waiting ops list
	rsm.ops = make(map[int]*retWait)
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// Try to queue command
	index, term, isLeader := rsm.Raft().Start(req)

	rsm.mu.Lock()
	// Check leader
	rsm.checkLeader(term, isLeader)
	if !rsm.isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	// fmt.Printf("S%d: Submit started for index %d, term %d, isLeader %t\n", rsm.me, index, term, isLeader)

	// Add the op to the waiting group
	op := retWait{successCh: make(chan bool, 1)}
	rsm.ops[index] = &op
	rsm.mu.Unlock()

	// fmt.Printf("S%d: Submit waiting for op %d\n", rsm.me, index)
	// Wait for apply/reject, if we have waited a long time
	// this could be an instance where we were the leader, lost leadership (logs never get commited)
	// and we never check if we are the leader, so force in a check every once in a while
	var success bool
	done := false
	for !done {
		select {
		case success = <-op.successCh:
			done = true
		case <-time.After(waitTime):
			term, isLeader := rsm.Raft().GetState()
			rsm.mu.Lock()
			rsm.checkLeader(term, isLeader)
			rsm.mu.Unlock()
		}
	}

	// fmt.Printf("S%d: Submit woke up for op %d, success: %t\n", rsm.me, index, success)

	// Error on failure
	if !success {
		return rpc.ErrMaybe, nil
	}

	// Provide return value
	return rpc.OK, op.ret
}
