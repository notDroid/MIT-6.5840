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
	Sock string
}

type IFile struct {
	Sock     string
	Filename string
}

type MIFile struct {
	Id       int
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
	Id       int
	Filename string
}

type ReduceTask struct {
	M      int
	Id     int
	IFiles map[int]IFile
}

type MapIntermediate struct {
	Id     int
	Sock   string
	IFiles []string
}

type Filename struct {
	Filename string
}

type Content struct {
	Content []byte
}

type ReduceIdentifier struct {
	Id   int
	Sock string
}

type ReduceInvalidRequest struct {
	RId  int
	Sock string
	MId  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func CoordinatorSock() string {
	s := "/var/tmp/5840-mr-c-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func WorkerSock() string {
	s := "/var/tmp/5840-mr-w-"
	s += strconv.Itoa(os.Getuid()) + "-" + strconv.Itoa(os.Getpid())
	return s
}

func ReduceSock() string {
	s := "/var/tmp/5840-mr-r-"
	s += strconv.Itoa(os.Getuid()) + "-" + strconv.Itoa(os.Getpid())
	return s
}

// send an RPC request, wait for the response.
func RPCall(sockname string, rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
