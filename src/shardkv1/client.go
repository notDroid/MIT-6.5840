package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}

	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	sId := shardcfg.Key2Shard(key)

	var value string
	var version rpc.Tversion
	var err rpc.Err

	// Keep trying shard groups
	for {
		// Query and find group that holds shard
		cfg := ck.sck.Query()
		gId := cfg.Shards[sId]

		// Create clerk and contact shardgrp
		clerk := shardgrp.MakeClerk(ck.clnt, cfg.Groups[gId])
		value, version, err = clerk.Get(key)

		// If we contacted the wrong group, try again
		if err != rpc.ErrWrongGroup {
			break
		}
	}

	return value, version, err
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	sId := shardcfg.Key2Shard(key)
	var err rpc.Err

	// Keep trying shard groups
	for {
		// Query and find group that holds shard
		cfg := ck.sck.Query()
		gId := cfg.Shards[sId]

		// Create clerk and contact shardgrp
		clerk := shardgrp.MakeClerk(ck.clnt, cfg.Groups[gId])
		err = clerk.Put(key, value, version)

		// If we contacted the wrong group, try again
		if err != rpc.ErrWrongGroup {
			break
		}
	}

	return err
}
