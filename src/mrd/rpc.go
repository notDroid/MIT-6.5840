package mrd

//
// RPC definitions.
//

import (
	"net"
	"net/rpc"
)

//
// RPC struct definitions
//

// Identifiers
type WorkerIdentifier struct {
	Sock string
}

type MapIdentifier struct {
	MId  int
	Sock string
}

type ReduceIdentifier struct {
	RId  int
	Sock string
}

// Task information
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
	M    int
	RId  int
	MIds []int
}

// Map id, for intermediate broadcasting
type MapId struct {
	MId int
}

// Get ip
func GetEC2PrivateIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String(), nil
}

// send an RPC request, wait for the response.
func RPCall(sockname string, rpcname string, args interface{}, reply interface{}) error {
	c, err := rpc.DialHTTP("tcp", sockname)
	if err != nil {
		return err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
