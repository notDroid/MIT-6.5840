package mr

//
// RPC definitions.
//

import (
	"net/rpc"
	"os"
	"strconv"
)

//
// RPC struct definitions
//

type Identifier struct {
	sock string
}

type IFile struct {
	sock     string
	filename string
}

type TaskReply struct {
	task       string
	mapTask    MapTask
	reduceTask ReduceTask
}

type MapTask struct {
	R        int
	id       int
	filename string
}

type ReduceTask struct {
	M      int
	id     int
	iFiles map[int]IFile
}

type MapIntermediate struct {
	id     int
	iFiles []string
}

type Filename struct {
	filename string
}

type Content struct {
	content []byte
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func workerSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid()) + "-" + strconv.Itoa(os.Getpid())
	return s
}

// send an RPC request, wait for the response.
func call(sockname string, rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
