package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"fmt"
	"sync"
	"time"

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

	killed      int32 // set by Kill()
	nextVersion rpc.Tversion
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{
		clnt: clnt,
	}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)

	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	// Get current and next config
	nextcfgS, nextVersion, _ := sck.IKVClerk.Get("nextcfg")
	cfgS, _, _ := sck.IKVClerk.Get("cfg")
	nextcfg := shardcfg.FromString(nextcfgS)
	cfg := shardcfg.FromString(cfgS)

	// Jump to current nextVersion
	sck.nextVersion = nextVersion

	// If we need to apply a config do it before returning
	if nextcfg.Num > cfg.Num {
		sck.ChangeConfigTo(nextcfg)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Initialize config (if it already exists this does nothing)
	sck.IKVClerk.Put("cfg", cfg.String(), 0)
	sck.IKVClerk.Put("nextcfg", cfg.String(), 0)
	sck.nextVersion = 1
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	num := new.Num
	wg := sync.WaitGroup{}

	// Get current config
	cfgS, version, _ := sck.IKVClerk.Get("cfg")
	cfg := shardcfg.FromString(cfgS)

	// Check if the new config is already applied/old
	if cfg.Num >= num {
		fmt.Printf("Old config recieved\n")
		return
	}

	// Put new version, if not already
	for {
		err := sck.IKVClerk.Put("nextcfg", new.String(), sck.nextVersion)
		if err == rpc.ErrVersion || err == rpc.OK {
			break
		}
	}
	sck.nextVersion++

	fmt.Printf("New config recieved\n")
	// Scan config for group changes
	for sIdi := range len(new.Shards) {
		// Find moved shards
		sId := shardcfg.Tshid(sIdi)

		oldGId, oldServers, ook := cfg.GidServers(sId)
		newGId, newServers, nok := new.GidServers(sId)

		// Is moved?
		if newGId == oldGId {
			continue
		}

		// Give up on non existant group
		if !ook || !nok {
			fmt.Printf("GROUP DOESN'T EXIST SKIPPING\n")
			continue
		}

		wg.Add(1)
		// Move shard
		go func(sId shardcfg.Tshid, oldServers, newServers []string) {
			// fmt.Printf("Moving Shard %d, from %d to %d\n", sId, oldGId, newGId)
			defer wg.Done()
			oldClerk := shardgrp.MakeClerk(sck.clnt, oldServers)
			newClerk := shardgrp.MakeClerk(sck.clnt, newServers)

			// Try freeze
			state, err := sck.FreezeShard(oldClerk, sId, num, len(oldServers))
			// On old config give up
			if err == rpc.ErrWrongGroup {
				fmt.Printf("ERROR FREEZING SHARD\n")
				return
			}

			// Try install shard
			err = sck.InstallShard(newClerk, sId, state, num, len(newServers))
			// On old config give up
			if err == rpc.ErrWrongGroup {
				fmt.Printf("ERROR INSTALLING SHARD\n")
				return
			}

			// Try delete shard
			err = sck.DeleteShard(oldClerk, sId, num, len(oldServers))
			if err == rpc.ErrWrongGroup {
				fmt.Printf("ERROR DELETING SHARD\n")
				return
			}

			// fmt.Printf("Success Moving Shard %d, from %d to %d\n", sId, oldGId, newGId)
		}(sId, oldServers, newServers)
	}
	// Wait for all shards to be moved
	wg.Wait()

	// Update current config
	sck.IKVClerk.Put("cfg", new.String(), version)
	fmt.Printf("Config Updated\n")
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// fmt.Printf("Config Queried\n")
	cfg, _, _ := sck.IKVClerk.Get("cfg")
	return shardcfg.FromString(cfg)
}

// Freeze shard wrapper
func (sck *ShardCtrler) FreezeShard(clerk *shardgrp.Clerk, sId shardcfg.Tshid, num shardcfg.Tnum, servers int) ([]byte, rpc.Err) {
	const MaxAttempts = 10

	attempts := 0
	for {
		if attempts >= MaxAttempts {
			fmt.Printf("Assuming the group is down and the config was already changed...\n")
			return nil, rpc.ErrWrongGroup
		}
		attempts++

		// Try to install shard
		state, err := clerk.FreezeShard(sId, num, 3*servers)

		if err != rpc.ErrTimeout {
			return state, err
		}

		time.Sleep(100 * time.Millisecond) // Wait before retrying
	}
}

func (sck *ShardCtrler) InstallShard(clerk *shardgrp.Clerk, sId shardcfg.Tshid, state []byte, num shardcfg.Tnum, servers int) rpc.Err {
	const MaxAttempts = 10

	attempts := 0
	for {
		if attempts >= MaxAttempts {
			fmt.Printf("Assuming the group is down and the config was already changed...\n")
			return rpc.ErrWrongGroup
		}
		attempts++

		// Try to install shard
		err := clerk.InstallShard(sId, state, num, 3*servers)

		if err != rpc.ErrTimeout {
			return err
		}

		time.Sleep(100 * time.Millisecond) // Wait before retrying
	}
}

func (sck *ShardCtrler) DeleteShard(clerk *shardgrp.Clerk, sId shardcfg.Tshid, num shardcfg.Tnum, servers int) rpc.Err {
	const MaxAttempts = 10

	attempts := 0
	for {
		if attempts >= MaxAttempts {
			fmt.Printf("Assuming the group is down and the config was already changed...\n")
			return rpc.ErrWrongGroup
		}
		attempts++

		// Try to install shard
		err := clerk.DeleteShard(sId, num, 3*servers)

		if err != rpc.ErrTimeout {
			return err
		}

		time.Sleep(100 * time.Millisecond) // Wait before retrying
	}
}
