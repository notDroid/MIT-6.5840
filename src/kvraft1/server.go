package kvraft

import (
	"fmt"
	"log"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Key-Value Store
	kvmap    map[string]string
	versions map[string]rpc.Tversion
}

type KVArgs struct {
	ClientId int
	Args     any
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(args)
	case rpc.PutArgs:
		return kv.doPut(args)
	default:
		log.Fatalf("DoOp should execute only (Put, Get) and not %T", req)
	}
	return nil
}

func (kv *KVServer) doGet(args rpc.GetArgs) rpc.GetReply {
	key := args.Key
	reply := rpc.GetReply{}

	// If there is a key, get it, otherwise rpc.ErrNoKey
	if value, in := kv.kvmap[key]; in {
		reply.Value = value
		reply.Version = kv.versions[key]
		reply.Err = rpc.OK
	} else {
		reply.Value = ""
		reply.Version = 0
		reply.Err = rpc.ErrNoKey
	}

	return reply
}

func (kv *KVServer) doPut(args rpc.PutArgs) rpc.PutReply {
	key := args.Key
	version := args.Version
	reply := rpc.PutReply{}

	// Check version, return error for invalid requests
	// For special case of matching version = 0 make a new key
	if sv, in := kv.versions[key]; sv != version {
		// Key doesn't exist
		if !in {
			reply.Err = rpc.ErrNoKey
			return reply
		}
		// Version mismatch
		reply.Err = rpc.ErrVersion
		return reply
	}

	// Increment version, store key-value pair
	kv.versions[key] += 1
	kv.kvmap[key] = args.Value

	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	fmt.Printf("SNAPSHOT\n")
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	fmt.Printf("RESTORE\n")
}

func (kv *KVServer) Get(args *KVArgs, reply *rpc.GetReply) {
	// Submit request
	err, ret := kv.rsm.Submit(args.Args.(rpc.GetArgs))

	// On error give up
	if err != rpc.OK {
		reply.Err = err
		return
	}

	// Provide return
	*reply = ret.(rpc.GetReply)
}

func (kv *KVServer) Put(args *KVArgs, reply *rpc.PutReply) {
	// Submit request
	err, ret := kv.rsm.Submit(args.Args.(rpc.PutArgs))

	// Custom return on not okay
	if err != rpc.OK {
		reply.Err = err
		return
	}

	// Provide return
	*reply = ret.(rpc.PutReply)
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
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(KVArgs{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me: me,

		kvmap:    make(map[string]string),
		versions: make(map[string]rpc.Tversion),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
