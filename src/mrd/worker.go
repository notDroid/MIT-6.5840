package mrd

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Handles getting intermediate data
type ReduceWorker struct {
	// Reduce metadata
	rId       int
	M         int
	nRecieved int

	// Reduce input intermediate file
	tmpFile *os.File
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var rm = sync.Mutex{}     // Reduce Worker mutex
var wg = sync.WaitGroup{} // Reduce wait for all map files
var cSock string          // Coordinator location
var wSock string          // Our location
var r = ReduceWorker{}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Set sockets
	cSock = os.Getenv("MR_COORDINATOR_ADDRESS")

	// Set up file request server
	r.server()
	fmt.Println("Worker Started")

	// Poll until task assigned
	for {
		args := WorkerIdentifier{wSock}
		reply := TaskReply{}

		// In case of reduce task, we need to lock and set up the reduce server.
		// Avoid race condition of coordinator sending intermediate file locations before worker is ready.
		rm.Lock()
		err := RPCall(cSock, "Coordinator.GetTask", &args, &reply)

		// Coordinator didn't respond, assume its done
		if err != nil {
			fmt.Println("Coordinator didn't respond, giving up:", err)
			return
		}

		// If we get a map or reduce task, execute it
		if reply.Task == "map" {
			fmt.Println("Worker recieved map task:", reply.MapTask.MId)
			rm.Unlock()
			executeMap(reply.MapTask, mapf)
		} else if reply.Task == "reduce" {
			fmt.Println("Worker recieved reduce task")
			// Unlock in execute reduce function
			executeReduce(reply.ReduceTask, reducef)
		} else {
			fmt.Println("Worker didn't recieve task")
			rm.Unlock()
			time.Sleep(time.Second)
		}
	}
}

func executeMap(mapTask MapTask, mapf func(string, string) []KeyValue) {
	// Execute map function on data
	intermediate := mapf(mapTask.Filename, fetchInputSplit(mapTask.Filename)) // We fit the entire result in memory, don't stream into map output

	// ------------------ Store on local disk

	// Create a file for every R worker
	var tmpFiles []*os.File
	var encoders []*json.Encoder
	for _ = range mapTask.R {
		tmpFile, err := os.CreateTemp("", "mr-*")
		if err != nil {
			log.Fatalf("Error creating temporary intermediate file %v: %v\n", tmpFile.Name(), err)
		}
		tmpFiles = append(tmpFiles, tmpFile)

		enc := json.NewEncoder(tmpFile)
		encoders = append(encoders, enc)
	}

	// Partition key value pairs, encode in json
	for _, kv := range intermediate {
		rId := ihash(kv.Key) % mapTask.R
		err := encoders[rId].Encode(&kv)
		if err != nil {
			log.Fatalln("Error writing intermediate temporary file:", tmpFiles[rId].Name())
		}
	}

	// Send to s3
	for rId, tmpFile := range tmpFiles {
		tmpName := tmpFile.Name()
		tmpFile.Close()
		filename := iFilename(mapTask.MId, rId)
		LocalPathToS3(tmpName, "intermediate/"+filename)
	}

	// Intermediate data stored

	// Report completion with intermediate locations
	args := MapIdentifier{
		MId:  mapTask.MId,
		Sock: wSock,
	}

	err := RPCall(cSock, "Coordinator.ReportCompletedMapTask", &args, &struct{}{})

	// If the rpc fails we are cooked
	if err != nil {
		log.Fatalln("Error informing map task completion to coordinator:", err)
	}
}

func executeReduce(reduceTask ReduceTask, reducef func(string, []string) string) {
	//
	// Collect all data for our partition
	//

	// SET UP REDUCE SERVER
	tmpFile, err := os.CreateTemp("", "mr-*")
	tmpName := tmpFile.Name()
	if err != nil {
		log.Fatalf("Error creating reduce input file %v: %v\n", tmpName, err)
	}
	r = ReduceWorker{
		rId: reduceTask.RId,
		M:   reduceTask.M,

		tmpFile: tmpFile,
	}
	wg.Add(reduceTask.M)
	rm.Unlock() // Server set up

	// Request from all map tasks already finished
	for _, mId := range reduceTask.MIds {
		filename := iFilename(mId, reduceTask.RId)
		go r.fetchIntermediate(filename)
	}

	// Timout timer
	go func() {
		time.Sleep(10 * time.Second)
		rm.Lock()
		if r.nRecieved < r.M {
			os.Exit(1)
		}
		rm.Unlock()
	}()
	wg.Wait()

	//
	// Sort key value pairs by key
	//

	// Reset the file cursor to the beginning
	_, err = tmpFile.Seek(0, 0)
	if err != nil {
		log.Fatalln("Couldn't reset cursor in reduce input file", err)
	}

	// Read the data from the file, assume it all fits in memory.
	intermediate := []KeyValue{}
	dec := json.NewDecoder(tmpFile)
	for {
		var kv KeyValue
		if err = dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}
	tmpFile.Close()

	// Sort data in memory. Do not use external sort.
	sort.Sort(ByKey(intermediate))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-{reduce id}.
	//
	tmpFile, _ = os.CreateTemp("", "mr-")
	tmpName = tmpFile.Name()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// Write to s3
	tmpFile.Close()
	filename := rFilename(reduceTask.RId)
	LocalPathToS3(tmpName, "output/"+filename)

	// Report reduce task completion
	reduceIdentifier := ReduceIdentifier{
		RId:  reduceTask.RId,
		Sock: wSock,
	}

	err = RPCall(cSock, "Coordinator.ReportCompletedReduceTask", &reduceIdentifier, &struct{}{})
	// fmt.Println("Reduce Worker Finished:", reduceTask.Id)

	// If the rpc fails we are cooked
	if err != nil {
		log.Fatalln("Error informing reduce task completion to coordinator:", err)
	}
}

// Fetch input map split from s3
func fetchInputSplit(filename string) string {
	content, err := ReadFileFromS3("input/" + filename)
	if err != nil {
		log.Fatalf("Couldn't fetch input slice %v: %v", filename, err)
	}
	return string(content)
}

// Recieve location of map files
func (r *ReduceWorker) SendMapIntermediate(args *MapId, reply *struct{}) error {
	// Get intermediate content
	filename := iFilename(args.MId, r.rId)
	r.fetchIntermediate(filename)
	return nil
}

// Fetch intermediate data from s3
func (r *ReduceWorker) fetchIntermediate(filename string) {
	// Fetch content
	content, err := ReadFileFromS3("intermediate/" + filename)
	if err != nil {
		log.Fatalf("Could not read intermediate %v: %v\n", filename, err)
	}
	// Write to reduce input file
	rm.Lock()
	_, err = r.tmpFile.Write(content)
	if err != nil {
		log.Fatalln("Error writing to reduce input file:", err)
	}
	r.nRecieved += 1
	rm.Unlock()
	wg.Done()
}

// Start a thread to listen for intermediate file transfer requests for reduce tasks
func (r *ReduceWorker) server() {
	rpc.Register(r)
	rpc.HandleHTTP()

	// Open listener
	l, e := net.Listen("tcp", ":0")
	if e != nil {
		log.Fatalln("listen error:", e)
	}

	// Get the assigned port
	port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
	address, e := GetEC2PrivateIP()
	if e != nil {
		log.Fatalln("couldn't get private ip:", e)
	}

	wSock = address + ":" + port

	go http.Serve(l, nil)
}

//
// Utility functions
//

func iFilename(mId, rId int) string {
	return "mr-mid-" + strconv.Itoa(mId) + "-rid-" + strconv.Itoa(rId) // Map id, reduce id pair
}

func rFilename(rId int) string {
	return "mr-out-" + strconv.Itoa(rId)
}
