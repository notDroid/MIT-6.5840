package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"fmt"
	"log"
	"time"

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
	const MaxAttempts = 10
	sId := shardcfg.Key2Shard(key)

	var value string
	var version rpc.Tversion
	var err rpc.Err

	// fmt.Printf("Starting GET %v\n", key)
	// Keep trying shard groups
	attempts := 0
	for {
		if attempts >= MaxAttempts {
			log.Fatalf("STUCK IN PUT LOOP\n")
		}
		attempts++
		// Query and find group that holds shard
		cfg := ck.sck.Query()
		_, servers, ok := cfg.GidServers(sId)

		// In case the shard group in down give up
		if !ok {
			fmt.Printf("Exiting on not OK\n")
			return "", 0, rpc.ErrWrongGroup
		}

		// Create clerk and contact shardgrp
		clerk := shardgrp.MakeClerk(ck.clnt, servers)
		value, version, err = clerk.Get(key, 3*len(servers))

		// If we contacted the wrong group, try again
		if err != rpc.ErrWrongGroup && err != rpc.ErrTimeout {
			// fmt.Printf("Exiting GET %v on err=%v\n", key, err)
			return value, version, err
		}
		// fmt.Printf("Cycling... group=%d err=%v\n", gId, err)
		time.Sleep(100 * time.Millisecond) // Wait for config change
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	const MaxAttempts = 10
	sId := shardcfg.Key2Shard(key)

	// fmt.Printf("Starting PUT %v\n", key)
	// Keep trying shard groups
	maybeSuccess := false
	attempts := 0
	for {
		if attempts >= MaxAttempts {
			log.Fatalf("STUCK IN PUT LOOP\n")
		}
		attempts++
		// Query and find group that holds shard
		cfg := ck.sck.Query()
		_, servers, ok := cfg.GidServers(sId)

		// In case the shard group in down give up
		if !ok {
			fmt.Printf("Exiting on not OK\n")
			return rpc.ErrWrongGroup
		}

		// Create clerk and contact shardgrp
		clerk := shardgrp.MakeClerk(ck.clnt, servers)
		// Track extra maybeSuccess state, which is needed because of our max_attempts retry policy, we need to track if it might have already been successful
		err, currentMaybeSuccess := clerk.Put(key, value, version, 3*len(servers))
		maybeSuccess = maybeSuccess || currentMaybeSuccess

		// If we contacted the wrong group, try again
		if err != rpc.ErrWrongGroup && err != rpc.ErrTimeout {
			// If maybeSuccess is true then one of our previous RPCs could have already done the put
			if maybeSuccess && err == rpc.ErrVersion {
				return rpc.ErrMaybe
			}
			// fmt.Printf("Exiting PUT %v on err=%v\n", key, err)
			return err
		}

		// fmt.Printf("Cycling... group=%d err=%v\n", gId, err)
		time.Sleep(100 * time.Millisecond) // Wait for config change
	}
}
