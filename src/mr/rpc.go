package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Identifier struct {
	sock string // Could be (ip, port) if we have multiple computers
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
	contents string
}

type ReduceTask struct {
	M      int
	id     int
	iFiles map[int]IFile
}

type MapIntermediate struct {
	id     int
	sock   string // Could be (ip, port) if we have multiple computers
	iFiles []string
}

func main() {

}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func mapSock(id int) string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	s += "-m-" + strconv.Itoa(id) // Map id part
	return s
}

func reduceSock(id int) string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	s += "-r-" + strconv.Itoa(id) // Reduce id part
	return s
}

// send an RPC request, wait for the response.
// returns false if something goes wrong.
func call(sockname string, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println(err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
