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
	// Tracking and assigning map tasks
	M            int               // Total number of map tasks to be processed
	nMap         int               // Number of map tasks assigned
	mapFiles     []string          // List of map input file names
	mapSocksById map[int]string    // Map from map worker ids to their sockets
	mapTimes     map[int]time.Time // In progress map tasks
	mapIdsLeft   map[int]struct{}  // Map ids left set

	// Tracking completed map tasks
	mapIFiles map[int][]string // Map results, intermediate file names

	// Tracking and assigning reduce tasks
	R               int               // Total number of reduce tasks
	nReduce         int               // number of reduce tasks assigned
	reduceSocksById map[int]string    // Map from reduce worker ids to their sockets
	reduceTimes     map[int]time.Time // In progress reduce tasks
	reduceIdsLeft   map[int]struct{}  // Reduce ids left set

	nDone int // Number of completed reduce tasks
}

var cm = sync.Mutex{}

// Your code here -- RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.

// Request task from coordinater
func (c *Coordinator) GetTask(args *Identifier, reply *TaskReply) error {
	cm.Lock()
	defer cm.Unlock()

	// If map tasks remaining assign map
	if c.nMap < c.M {
		// Assign Map
		c.nMap += 1
		reply.Task = "map"
		reply.MapTask.R = c.R

		// Grab a remaining map id
		var mId int
		for key := range c.mapIdsLeft {
			mId = key
			break
		}
		delete(c.mapIdsLeft, mId)

		reply.MapTask.Id = mId
		c.mapSocksById[mId] = args.Sock
		c.mapTimes[mId] = time.Now()

		// Get and send filename
		filename := c.mapFiles[mId]
		reply.MapTask.Filename = filename
	} else if c.nReduce < c.R {
		// Assign reduce task
		c.nReduce += 1
		reply.Task = "reduce"
		reply.ReduceTask.M = c.M

		// Grab a remaining reduce id
		var rId int
		for key := range c.reduceIdsLeft {
			rId = key
			break
		}
		delete(c.reduceIdsLeft, rId)

		reply.ReduceTask.Id = rId
		c.reduceSocksById[rId] = args.Sock
		c.reduceTimes[rId] = time.Now()

		// Send map worker locations
		reply.ReduceTask.IFiles = make(map[int]IFile)
		for mId, filenames := range c.mapIFiles {
			iFile := IFile{
				Sock:     c.mapSocksById[mId],
				Filename: filenames[rId],
			}
			reply.ReduceTask.IFiles[mId] = iFile
		}
	} else {
		reply.Task = "none"
	}

	return nil
}

// Map task must report locations of files on local disk, so that reduce f
func (c *Coordinator) ReportCompletedMapTask(args *MapIntermediate, reply *struct{}) error {
	cm.Lock()
	defer cm.Unlock()
	// Only accept completions from valid tasks
	if _, inProgesss := c.mapTimes[args.Id]; !inProgesss || c.mapSocksById[args.Id] != args.Sock {
		return nil
	}

	fmt.Println("Map Worker Finished:", args.Id)

	// Get file locations
	c.mapIFiles[args.Id] = args.IFiles
	delete(c.mapTimes, args.Id) // Remove from in progress set

	// Broadcast locations to reduce workers
	invalidIds := []int{}
	for rId, rSock := range c.reduceSocksById {
		reduceArgs := IFile{
			Sock:     c.mapSocksById[args.Id],
			Filename: args.IFiles[rId],
		}
		err := RPCall(rSock, "ReduceWorker.SendMapIntermediate", &reduceArgs, &struct{}{})

		// There is a race condition where the reduce worker hasn't setup its http server before the coordinator sends the intermediate locations
		// As a solution we can wait 1 second and retry, this stalls the entire master
		if err != nil {
			time.Sleep(time.Second)
			err = RPCall(rSock, "ReduceWorker.SendMapIntermediate", &reduceArgs, &struct{}{})

			// If the reduce worker doesn't respond, assume its dead
			if err != nil {
				invalidIds = append(invalidIds, rId)
			}
		}
	}

	// Invalidate reduce workers that didn't respond
	for _, id := range invalidIds {
		fmt.Println("Reduce worker didn't respond:", id)
		c.invalidateReduce(id)
	}

	return nil
}

// Map task must report locations of files on local disk, so that reduce f
func (c *Coordinator) ReportCompletedReduceTask(args *ReduceIdentifier, reply *struct{}) error {
	cm.Lock()
	defer cm.Unlock()
	// Ignore if invalidated
	if _, inProgesss := c.reduceTimes[args.Id]; !inProgesss || c.reduceSocksById[args.Id] != args.Sock {
		return nil
	}
	fmt.Println("Reduce Worker Finished:", args.Id)

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
	ret := c.nDone == c.R
	cm.Unlock()
	return ret
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
		M:            len(files),
		mapIdsLeft:   mapIdsLeft,
		mapFiles:     files,
		mapTimes:     make(map[int]time.Time),
		mapSocksById: make(map[int]string),

		mapIFiles: make(map[int][]string),

		R:               nReduce,
		reduceIdsLeft:   reduceIdsLeft,
		reduceTimes:     make(map[int]time.Time),
		reduceSocksById: make(map[int]string),
	}

	// Start server
	c.server()
	go c.waitTasks()
	return &c
}

// Start a thread to wait on map tasks. On a real distributed system we could ping the computers.
func (c *Coordinator) waitTasks() {
	for {
		time.Sleep(10 * time.Second)
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
	c.nMap -= 1
	delete(c.mapSocksById, id)
	delete(c.mapTimes, id)
}

func (c *Coordinator) invalidateReduce(id int) {
	fmt.Println("Invalidate Reduce:", id)
	c.reduceIdsLeft[id] = struct{}{}
	c.nReduce -= 1
	delete(c.reduceSocksById, id)
	delete(c.reduceTimes, id)
}
