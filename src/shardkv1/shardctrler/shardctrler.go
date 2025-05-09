package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"sync"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk
	clrks map[tester.Tgid]*shardgrp.Clerk

	killed int32 // set by Kill()

	mu  sync.Mutex
	cfg shardcfg.ShardConfig
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{
		clnt: clnt,
		mu:   sync.Mutex{},
	}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)

	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {

}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Get clerk groups
	for gId, servers := range cfg.Groups {
		sck.clrks[gId] = shardgrp.MakeClerk(sck.clnt, servers)
	}
	sck.IKVClerk.Put("cfg", cfg.String(), 0)
	sck.cfg = *cfg
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	num := new.Num
	wg := sync.WaitGroup{}

	// Scan config for group changes
	for sId, gId := range new.Shards {
		// Exit on stale num
		if new.Num != sck.cfg.Num {
			return
		}
		// Find moved shards
		if gId == sck.cfg.Shards[sId] {
			continue
		}

		wg.Add(1)
		// Move shard
		go func(sId shardcfg.Tshid, gId tester.Tgid) {
			defer wg.Done()
			clerk := sck.clrks[gId]

			// Try freeze
			state, err := clerk.FreezeShard(sId, num)
			// On old config give up
			if err == rpc.ErrWrongGroup {
				return
			}

			// Try install shard
			err = clerk.InstallShard(sId, state, num)
			// On old config give up
			if err == rpc.ErrWrongGroup {
				return
			}

			// Try delete shard
			clerk.DeleteShard(sId, num)
		}(shardcfg.Tshid(sId), gId)
	}
	// Wait for all shards to be moved
	wg.Wait()

	// Update config
	sck.mu.Lock()
	sck.cfg = *new
	sck.mu.Unlock()
	sck.IKVClerk.Put("cfg", new.String(), 0)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	sck.mu.Lock()
	defer sck.mu.Unlock()

	cfg := sck.cfg // Heap allocate a copy of the current config
	return &cfg
}
