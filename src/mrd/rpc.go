package mrd

//
// RPC definitions.
//

import (
	"fmt"
	"io"
	"net/http"
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
	// AWS metadata endpoint for private IP
	const url = "http://169.254.169.254/latest/meta-data/local-ipv4"
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to fetch private IP: %v", err)
	}
	defer resp.Body.Close()

	ip, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %v", err)
	}

	return string(ip), nil
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
