package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck  kvtest.IKVClerk
	id  string
	ver rpc.Tversion
}

var lockKey = "$lock"

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck: ck,
		id: kvtest.RandValue(8),
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		// Check if the lock is avaliable
		id, ver, _ := lk.ck.Get(lockKey)
		lk.ver = ver

		if id != "" {
			// Lock in use, wait and try again
			time.Sleep(time.Second)
			continue
		}

		// Try to acquire it
		err := lk.ck.Put(lockKey, lk.id, ver)

		if err == rpc.OK {
			// We got it
			break
		} else if err == rpc.ErrVersion {
			// Another client got the lock before us, wait then retry
			time.Sleep(time.Second)
			continue
		}

		// Err maybe case, we need to check if we got it or not
		id, _, _ = lk.ck.Get(lockKey)

		if id == lk.id {
			break
		}
		// Not ours, wait and try again
		time.Sleep(time.Second)
	}
}

func (lk *Lock) Release() {
	lk.ck.Put(lockKey, "", lk.ver+1)
}
