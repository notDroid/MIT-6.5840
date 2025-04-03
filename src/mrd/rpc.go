package mrd

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

type WorkerIdentifier struct {
	Sock string
}

type MIFile struct {
	MId      int
	Sock     string
	Filename string
}

type TaskReply struct {
	Task       string
	MapTask    MapTask
	ReduceTask ReduceTask
}

type MapTask struct {
	R        int
	MId      int
	Filename string
}

type ReduceTask struct {
	M      int
	RId    int
	IFiles map[int]string
}

type MapIntermediate struct {
	MId    int
	Sock   string
	IFiles []string
}

type Filename struct {
	Filename string
}

type ReduceIdentifier struct {
	RId  int
	Sock string
}

// Use port 8000 for the coordinator
func CoordinatorSock() string {
	s := "" + ":8000"
	return s
}

func WorkerSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid()) + "-" + strconv.Itoa(os.Getpid())
	return s
}

// send an RPC request, wait for the response.
func RPCall(sock string, rpcname string, args interface{}, reply interface{}) error {
	c, err := rpc.DialHTTP("tcp", sock)
	// c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
