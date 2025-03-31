package mr

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Tracking and assigning map tasks
	M          int              // total number of map tasks to be processed
	nMap       int              // number of active map tasks
	mapFiles   []string         // list of map input file names
	mapSocks   map[int]string   // Map from map worker ids to their sockets
	mapIdsLeft map[int]struct{} // Map ids left set

	// Tracking completed map tasks
	mapIFiles map[int][]string // map results, intermediate file names

	// Tracking and assigning reduce tasks
	R             int              // total number of reduce tasks
	nReduce       int              // number of active reduce tasks
	reduceIds     map[int]string   // Map from reduce worker ids to their sockets
	reduceIdsLeft map[int]struct{} // Reduce ids left set
}

var cm = sync.Mutex{}

// Your code here -- RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.

// Request task from coordinater
func (c *Coordinator) GetTask(args *Identifier, reply *TaskReply) error {
	cm.Lock()
	defer cm.Unlock()

	// If map tasks remaining assign map
	if c.nMap > 0 {
		// Assign Map
		c.nMap -= 1
		reply.task = "map"
		reply.mapTask.R = c.R

		// Grab a remaining map id
		var mId int
		for key := range c.mapIdsLeft {
			mId = key
			break
		}
		c.mapSocks[mId] = args.sock

		// Get and send filename
		filename := c.mapFiles[c.nMap]
		reply.mapTask.filename = filename

		// Read file contents
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		contents, err := io.ReadAll(file)
		file.Close()
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}

		// Send file contents
		reply.mapTask.contents = string(contents)

	} else if c.nReduce < c.R {
		// Assign reduce task
		c.nReduce += 1
		reply.task = "reduce"
		reply.reduceTask.M = c.M

		// Grab a remaining reduce id
		var rId int
		for key := range c.reduceIdsLeft {
			rId = key
			break
		}
		reply.reduceTask.id = rId
		c.reduceIds[rId] = args.sock

		// Send map worker locations
		for mId, filenames := range c.mapIFiles {
			iFile := IFile{
				sock:     c.mapSocks[mId],
				filename: filenames[mId],
			}
			reply.reduceTask.iFiles[mId] = iFile
		}
	}

	return nil
}

// Map task must report locations of files on local disk, so that reduce f
func (c *Coordinator) ReportCompletedTask(args *MapIntermediate, reply *struct{}) error {
	cm.Lock()
	defer cm.Unlock()

	// Get file locations
	c.mapIFiles[args.id] = args.iFiles

	// Broadcast locations to reduce workers
	for rId, rSock := range c.reduceIds {
		ok := call(rSock, "ReduceWorker.SendMapIntermediate", args.iFiles[rId], struct{}{})

		// If the reduce worker doesn't respond, assume its dead
		if !ok {
			// Remove the reduce task from the active set and add it to the reduce tasks left set
			c.nReduce -= 1
			delete(c.reduceIds, rId)
			c.reduceIdsLeft[rId] = struct{}{}
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
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
	ret := false

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.server()
	return &c
}
