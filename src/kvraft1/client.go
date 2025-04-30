package kvraft

import (
	"math/rand"
	"sync"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	me      int

	mu       sync.Mutex
	leaderId int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:     clnt,
		servers:  servers,
		me:       rand.Int(),
		mu:       sync.Mutex{},
		leaderId: 0,
	}

	return ck
}

func (ck *Clerk) getLeaderId() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leaderId
}

func (ck *Clerk) setLeaderId(id int) {
	ck.mu.Lock()
	ck.leaderId = id
	ck.mu.Unlock()
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := KVArgs{
		ClientId: ck.me,
		Args:     rpc.GetArgs{Key: key},
	}
	reply := rpc.GetReply{}

	// Keep searching for the leader until success
	i := ck.getLeaderId()
	for {
		// Send RPC
		ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)

		// Stop sending on success
		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey) {
			// Update leader id
			ck.setLeaderId(i)
			break
		}
		if reply.Err != rpc.ErrWrongLeader {
			// fmt.Printf("Cycling... server=%d ok=%t err=%v\n", i, ok, reply.Err)
		}

		// Cycle on rpc failure or wrong leader
		i = (i + 1) % len(ck.servers)
		// fmt.Printf("Cycling... server=%d ok=%t err=%v\n", i, ok, reply.Err)
	}
	// fmt.Printf("Get Success... server=%d value=%v version=%v err=%v\n", i, reply.Value, reply.Version, reply.Err)

	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := KVArgs{
		ClientId: ck.me,
		Args: rpc.PutArgs{
			Key:     key,
			Value:   value,
			Version: version,
		},
	}
	reply := rpc.PutReply{}

	// Keep searching for the leader until success
	maybeSuccess := false
	i := ck.getLeaderId()
	for {
		// Send RPC
		ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)

		// Stop sending on success, accept rpc.ErrMaybe
		if ok && !(reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrMaybe) {
			// Update leader id
			ck.setLeaderId(i)
			break
		}

		// On RPC failure we might have succedeed submitting the request but the response back failed
		if !ok || reply.Err == rpc.ErrMaybe {
			maybeSuccess = true
		}

		if reply.Err != rpc.ErrWrongLeader {
			// fmt.Printf("Cycling... server=%d ok=%t err=%v\n", i, ok, reply.Err)
		}

		// Cycle on rpc failure or wrong leader
		i = (i + 1) % len(ck.servers)
		// fmt.Printf("Cycling... server=%d ok=%t err=%v\n", i, ok, reply.Err)
	}

	// If our request results in a version error but we had a RPC fail before it, we might have done the PUT
	if maybeSuccess && reply.Err == rpc.ErrVersion {
		return rpc.ErrMaybe
	}
	return reply.Err
}
