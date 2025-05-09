package shardgrp

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string

	mu       sync.Mutex
	leaderId int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{
		clnt:    clnt,
		servers: servers,

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

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
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

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
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

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	reply := shardrpc.FreezeShardReply{}

	// Keep searching for the leader until success
	i := ck.getLeaderId()
	for {
		// Send RPC
		ok := ck.clnt.Call(ck.servers[i], "KVServer.FreezeShard", &args, &reply)

		// Stop sending on success
		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrWrongGroup) {
			// Update leader id
			ck.setLeaderId(i)
			break
		}

		// Cycle on rpc failure or wrong leader
		i = (i + 1) % len(ck.servers)
		// fmt.Printf("Cycling... server=%d ok=%t err=%v\n", i, ok, reply.Err)
	}

	return reply.State, reply.Err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, Num: num}
	reply := shardrpc.InstallShardReply{}

	// Keep searching for the leader until success
	i := ck.getLeaderId()
	for {
		// Send RPC
		ok := ck.clnt.Call(ck.servers[i], "KVServer.InstallShard", &args, &reply)

		// Stop sending on success
		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrWrongGroup) {
			// Update leader id
			ck.setLeaderId(i)
			break
		}

		// Cycle on rpc failure or wrong leader
		i = (i + 1) % len(ck.servers)
		// fmt.Printf("Cycling... server=%d ok=%t err=%v\n", i, ok, reply.Err)
	}

	return reply.Err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	reply := shardrpc.DeleteShardReply{}

	// Keep searching for the leader until success
	i := ck.getLeaderId()
	for {
		// Send RPC
		ok := ck.clnt.Call(ck.servers[i], "KVServer.DeleteShard", &args, &reply)

		// Stop sending on success
		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrWrongGroup) {
			// Update leader id
			ck.setLeaderId(i)
			break
		}

		// Cycle on rpc failure or wrong leader
		i = (i + 1) % len(ck.servers)
		// fmt.Printf("Cycling... server=%d ok=%t err=%v\n", i, ok, reply.Err)
	}

	return reply.Err
}
