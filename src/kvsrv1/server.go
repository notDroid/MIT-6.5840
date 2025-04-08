package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.RWMutex

	kvmap    map[string]string
	versions map[string]rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		mu: sync.RWMutex{},

		kvmap:    make(map[string]string),
		versions: make(map[string]rpc.Tversion),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	key := args.Key

	// Lock
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if value, in := kv.kvmap[key]; in {
		reply.Value = value
		reply.Version = kv.versions[key]
		reply.Err = rpc.OK
	} else {
		reply.Value = ""
		reply.Version = 0
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	key := args.Key
	version := args.Version

	// Lock
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check version, return error for invalid requests
	if sv, in := kv.versions[key]; sv != version {
		// Key doesn't exist
		if !in {
			reply.Err = rpc.ErrNoKey
			return
		}
		// Version mismatch
		reply.Err = rpc.ErrVersion
		return
	}

	// Increment version, store key-value pair
	kv.versions[key] += 1
	kv.kvmap[key] = args.Value

	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
