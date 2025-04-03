package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Input slices
	inputFiles []string // List of map input file names

	// Tracking and assigning map tasks
	M            int               // Total number of map tasks to be processed
	mapIdsLeft   map[int]struct{}  // Map ids left set
	mapSocks     map[int]string    // Map from map worker ids to their sockets
	mapTimes     map[int]time.Time // In progress map tasks
	mapCompleted map[int]struct{}  // Completed map tasks

	// Tracking and assigning reduce tasks
	R             int               // Total number of reduce tasks
	reduceIdsLeft map[int]struct{}  // Reduce ids left set
	reduceSocks   map[int]string    // Map from reduce worker ids to their sockets
	reduceTimes   map[int]time.Time // In progress reduce tasks
	nDone         int               // Number of completed reduce tasks
}

var cm = sync.Mutex{}

// Your code here -- RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.

// Request task from coordinater
func (c *Coordinator) GetTask(args *WorkerIdentifier, reply *TaskReply) error {
	cm.Lock()
	defer cm.Unlock()

	// Assign map, if map tasks remaining.
	if len(c.mapIdsLeft) > 0 {
		// Grab a remaining map id
		mId := c.fetchMId()

		// Update data structures
		c.mapSocks[mId] = args.Sock
		c.mapTimes[mId] = time.Now()

		// Send map task info: (id, filename)
		reply.Task = "map"
		reply.MapTask.R = c.R
		reply.MapTask.MId = mId
		reply.MapTask.Filename = c.inputFiles[mId]

	} else if len(c.reduceIdsLeft) > 0 { // Assign reduce task, no map tasks, reduce tasks left.
		// Grab a remaining reduce id
		rId := c.fetchRId()

		// Update data structures
		c.reduceSocks[rId] = args.Sock
		c.reduceTimes[rId] = time.Now()

		// Send reduce task info
		reply.Task = "reduce"
		reply.ReduceTask.M = c.M
		reply.ReduceTask.RId = rId

		// Send map worker locations
		reply.ReduceTask.MIds = []int{}
		for mId := range c.mapCompleted {
			reply.ReduceTask.MIds = append(reply.ReduceTask.MIds, mId)
		}

	} else {
		reply.Task = "none"
	}

	return nil
}

// Map task must report locations of files on local disk, so that reduce f
func (c *Coordinator) ReportCompletedMapTask(args *MapIdentifier, reply *struct{}) error {
	cm.Lock()
	defer cm.Unlock()
	// Only accept completions from valid tasks
	if !c.isValidMap(args.MId, args.Sock) {
		return nil
	}

	fmt.Println("Map Worker Finished:", args.MId)

	// Get file locations
	c.mapCompleted[args.MId] = struct{}{}
	delete(c.mapTimes, args.MId) // Remove from in progress set

	// Broadcast locations to reduce workers
	invalidIds := []int{}
	for rId, _ := range c.reduceTimes {
		rSock := c.reduceSocks[rId]
		reduceArgs := MapId{args.MId}
		err := RPCall(rSock, "ReduceWorker.SendMapIntermediate", &reduceArgs, &struct{}{})

		if err != nil {
			fmt.Printf("Reduce Worker didn't respond %d: %v\n", rId, err)
			invalidIds = append(invalidIds, rId)
		}
	}

	// Invalidate reduce workers that didn't respond
	for _, id := range invalidIds {
		c.invalidateReduce(id)
	}

	return nil
}

// Map task must report locations of files on local disk, so that reduce f
func (c *Coordinator) ReportCompletedReduceTask(args *ReduceIdentifier, reply *struct{}) error {
	cm.Lock()
	defer cm.Unlock()
	// Ignore if invalidated
	if !c.isValidReduce(args.RId, args.Sock) {
		return nil
	}
	fmt.Println("Reduce Worker Finished:", args.RId)

	delete(c.reduceTimes, args.RId) // Remove from in progress set
	c.nDone += 1

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := CoordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	cm.Lock()
	defer cm.Unlock()
	return c.nDone == c.R
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Initialize ids left sets
	mapIdsLeft := map[int]struct{}{}
	for id := range len(files) {
		mapIdsLeft[id] = struct{}{}
	}
	reduceIdsLeft := map[int]struct{}{}
	for id := range nReduce {
		reduceIdsLeft[id] = struct{}{}
	}

	// Intialize coordinator
	c := Coordinator{
		inputFiles: files,

		M:            len(files),
		mapIdsLeft:   mapIdsLeft,
		mapSocks:     make(map[int]string),
		mapTimes:     make(map[int]time.Time),
		mapCompleted: make(map[int]struct{}),

		R:             nReduce,
		reduceIdsLeft: reduceIdsLeft,
		reduceSocks:   make(map[int]string),
		reduceTimes:   make(map[int]time.Time),
	}

	// Start server
	c.server()
	go c.waitTasks()
	return &c
}

// Start a thread to wait on map tasks. We only use a timeout here, but an improvement could be adding pinging.
func (c *Coordinator) waitTasks() {
	for {
		time.Sleep(5 * time.Second)
		cm.Lock()
		// Poll for incomplete map tasks that have timed out
		invalidIds := []int{}
		for id, startTime := range c.mapTimes {
			// If we have waited more than 10 seconds give up
			if time.Since(startTime) > 10*time.Second {
				invalidIds = append(invalidIds, id)
			}
		}

		// Invalidate map
		for _, id := range invalidIds {
			fmt.Println("Map worker timed out:", id)
			c.invalidateMap(id)
		}

		// Poll for incomplete reduce tasks that have timed out
		invalidIds = []int{}
		for id, startTime := range c.reduceTimes {
			// If we have waited more than 10 seconds give up
			if time.Since(startTime) > 10*time.Second {
				invalidIds = append(invalidIds, id)
			}
		}

		// Invalidate reduce
		for _, id := range invalidIds {
			fmt.Println("Reduce worker timed out:", id)
			c.invalidateReduce(id)
		}
		cm.Unlock()
	}
}

func (c *Coordinator) invalidateMap(id int) {
	fmt.Println("Invalidate Map:", id)
	c.mapIdsLeft[id] = struct{}{}
	delete(c.mapTimes, id)
}

func (c *Coordinator) invalidateReduce(id int) {
	fmt.Println("Invalidate Reduce:", id)
	c.reduceIdsLeft[id] = struct{}{}
	delete(c.reduceTimes, id)
}

func (c *Coordinator) isValidMap(id int, sock string) bool {
	if _, inProgesss := c.mapTimes[id]; !inProgesss || c.mapSocks[id] != sock {
		return false
	}
	return true
}

func (c *Coordinator) isValidReduce(id int, sock string) bool {
	if _, inProgesss := c.reduceTimes[id]; !inProgesss || c.reduceSocks[id] != sock {
		return false
	}
	return true
}

func (c *Coordinator) fetchMId() int {
	var mId int
	for key := range c.mapIdsLeft {
		mId = key
		break
	}
	delete(c.mapIdsLeft, mId)
	return mId
}

func (c *Coordinator) fetchRId() int {
	var rId int
	for key := range c.reduceIdsLeft {
		rId = key
		break
	}
	delete(c.reduceIdsLeft, rId)
	return rId
}
