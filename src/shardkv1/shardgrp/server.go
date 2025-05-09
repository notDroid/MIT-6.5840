package shardgrp

import (
	"bytes"
	"fmt"
	"log"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Key-Value Store
	kvmap    map[string]string
	versions map[string]rpc.Tversion

	// Shard Management
	shard2key      map[shardcfg.Tshid][]string
	shardConfigNum map[shardcfg.Tshid]shardcfg.Tnum
	frozenSet      map[shardcfg.Tshid]struct{}
}

type SnapshotState struct {
	Kvmap    map[string]string
	Versions map[string]rpc.Tversion

	Shard2key      map[shardcfg.Tshid][]string
	ShardConfigNum map[shardcfg.Tshid]shardcfg.Tnum
	FrozenSet      map[shardcfg.Tshid]struct{}
}

type FrozenState struct {
	Keys     []string
	Values   []string
	Versions []rpc.Tversion
}

func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(args)
	case rpc.PutArgs:
		return kv.doPut(args)
	case shardrpc.FreezeShardArgs:
		return kv.doFreezeShard(args)
	case shardrpc.InstallShardArgs:
		return kv.doInstallShard(args)
	case shardrpc.DeleteShardArgs:
		return kv.doDeleteShard(args)
	default:
		log.Fatalf("DoOp should execute only (Put, Get) or (FreezeShard, InstallShard, DeleteShard) and not %T", req)
	}
	return nil
}

func (kv *KVServer) doGet(args rpc.GetArgs) rpc.GetReply {
	key := args.Key
	reply := rpc.GetReply{}

	if _, exists := kv.frozenSet[shardcfg.Key2Shard(key)]; exists {
		reply.Value = ""
		reply.Version = 0
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	// If there is a key, get it, otherwise rpc.ErrNoKey
	if value, in := kv.kvmap[key]; in {
		reply.Value = value
		reply.Version = kv.versions[key]
		reply.Err = rpc.OK
	} else {
		reply.Value = ""
		reply.Version = 0
		reply.Err = rpc.ErrWrongGroup
	}

	return reply
}

func (kv *KVServer) doPut(args rpc.PutArgs) rpc.PutReply {
	key := args.Key
	version := args.Version
	sId := shardcfg.Key2Shard(key)
	reply := rpc.PutReply{}

	if _, exists := kv.frozenSet[sId]; exists {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	// Check version, return error for invalid requests
	// For special case of matching version = 0 make a new key
	if sv, in := kv.versions[key]; sv != version {
		// Key doesn't exist
		if !in {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		// Version mismatch
		reply.Err = rpc.ErrVersion
		return reply
	}

	// If a new key is created add it to the shard2key map
	if version == 0 {
		kv.shard2key[sId] = append(kv.shard2key[sId], key)
	}

	// Increment version, store key-value pair
	kv.versions[key] += 1
	kv.kvmap[key] = args.Value

	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) doFreezeShard(args shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {
	sId := args.Shard
	version := args.Num
	reply := shardrpc.FreezeShardReply{}

	currentVersion := kv.shardConfigNum[sId]

	// Old RPC: already deleted or past request
	if version < currentVersion {
		reply.Num = currentVersion
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
	// Update version
	kv.shardConfigNum[sId] = version

	// Freeze keys
	keys := kv.shard2key[sId]
	kv.frozenSet[sId] = struct{}{}

	// Assemble state struct
	state := FrozenState{Keys: keys}
	for _, key := range keys {
		state.Values = append(state.Values, kv.kvmap[key])
		state.Versions = append(state.Versions, kv.versions[key])
	}

	// Encode state
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(state)
	if err != nil {
		log.Fatalf("Server [%d]: Error encoding frozen state: %v", kv.me, err)
	}

	// Respond
	reply.State = w.Bytes()
	reply.Num = version
	reply.Err = rpc.OK

	return reply
}

func (kv *KVServer) doInstallShard(args shardrpc.InstallShardArgs) shardrpc.InstallShardReply {
	sId := args.Shard
	version := args.Num
	reply := shardrpc.InstallShardReply{}

	currentVersion := kv.shardConfigNum[sId]

	// Old RPC
	if version < currentVersion {
		reply.Err = rpc.ErrWrongGroup
		return reply
	} else if version == currentVersion {
		reply.Err = rpc.OK
		return reply
	}
	// Update version
	kv.shardConfigNum[sId] = version
	// If the shard was previously transferred out, unfreeze it
	if _, in := kv.frozenSet[sId]; in {
		delete(kv.frozenSet, sId)
	}

	// Decode state
	var state FrozenState

	r := bytes.NewBuffer(args.State)
	d := labgob.NewDecoder(r)

	err := d.Decode(&state)
	if err != nil {
		log.Fatalf("Server [%d]: Error decoding frozen state: %v", kv.me, err)
	}

	// Install
	for i, key := range state.Keys {
		kv.shard2key[sId] = append(kv.shard2key[sId], key)
		kv.kvmap[key] = state.Values[i]
		kv.versions[key] = state.Versions[i]
	}

	// Respond
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) doDeleteShard(args shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {
	sId := args.Shard
	version := args.Num
	reply := shardrpc.DeleteShardReply{}

	currentVersion := kv.shardConfigNum[sId]

	// Old RPC
	if version < currentVersion {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	// Delete shard
	keys := kv.shard2key[sId]
	delete(kv.shard2key, sId)
	for _, key := range keys {
		delete(kv.kvmap, key)
		delete(kv.versions, key)
	}

	// Respond
	reply.Err = rpc.OK
	return reply

}

// Encode key-value store into snapshot
func (kv *KVServer) Snapshot() []byte {
	state := SnapshotState{kv.kvmap, kv.versions, kv.shard2key, kv.shardConfigNum, kv.frozenSet}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(state)
	if err != nil {
		log.Fatalf("Server [%d]: Error encoding snapshot: %v", kv.me, err)
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	var state SnapshotState

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	err := d.Decode(&state)
	if err != nil {
		log.Fatalf("Server [%d]: Error decoding snapshot: %v", kv.me, err)
	}

	kv.kvmap = state.Kvmap
	kv.versions = state.Versions
	kv.shard2key = state.Shard2key
	kv.shardConfigNum = state.ShardConfigNum
	kv.frozenSet = state.FrozenSet
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Submit request
	err, ret := kv.rsm.Submit(*args)

	// On error give up
	if err != rpc.OK {
		reply.Err = err
		return
	}

	// Provide return
	*reply = ret.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Submit request
	err, ret := kv.rsm.Submit(*args)

	// Custom return on not okay
	if err != rpc.OK {
		reply.Err = err
		return
	}

	// Provide return
	*reply = ret.(rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Submit request
	err, ret := kv.rsm.Submit(*args)

	// Custom return on not okay
	if err != rpc.OK {
		reply.Err = err
		return
	}

	// Provide return
	*reply = ret.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Submit request
	err, ret := kv.rsm.Submit(*args)

	// Custom return on not okay
	if err != rpc.OK {
		reply.Err = err
		return
	}

	// Provide return
	*reply = ret.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Submit request
	err, ret := kv.rsm.Submit(*args)

	// Custom return on not okay
	if err != rpc.OK {
		reply.Err = err
		return
	}

	// Provide return
	*reply = ret.(shardrpc.DeleteShardReply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{
		gid: gid,
		me:  me,

		kvmap:    make(map[string]string),
		versions: make(map[string]rpc.Tversion),

		shard2key:      make(map[shardcfg.Tshid][]string),
		shardConfigNum: make(map[shardcfg.Tshid]shardcfg.Tnum),
		frozenSet:      make(map[shardcfg.Tshid]struct{}),
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	fmt.Printf("Started Key-Value server: %d\n", gid)

	return []tester.IService{kv, kv.rsm.Raft()}
}
