package mrd

//
// RPC definitions.
//

import (
	"fmt"
	"io"
	"net/http"
	"net/rpc"
	"time"
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
	// Create an HTTP client with a timeout
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	// Make a request to the EC2 instance metadata service
	resp, err := client.Get("http://169.254.169.254/latest/meta-data/local-ipv4")
	if err != nil {
		fmt.Printf("Error retrieving private IP: %v\n", err)
		return "", err
	}
	defer resp.Body.Close()

	// Read the response body
	privateIP, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading response: %v\n", err)
		return "", err
	}
	return string(privateIP), nil
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
