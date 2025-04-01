package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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

type ReduceWorker struct {
	filename string
	file     *os.File
}

type WorkerServer struct{}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var cSock = coordinatorSock() // Runtime constant
var m = sync.Mutex{}          // Worker mutex
var wg = sync.WaitGroup{}     // Reduce wait for all map files

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Set up file request server
	w := WorkerServer{}
	w.server()

	// Set up Coordinator RPC
	wSock := workerSock()

	args := Identifier{wSock}
	reply := TaskReply{}

	// Poll until task assigned
	for {
		err := call(cSock, "Coordinator.GetTask", &args, &reply)

		// Coordinator didn't respond, assume its done
		if err != nil {
			fmt.Println("Coordinator didn't respond, giving up") // ----------------------------------------------------- TEMPORARY COMMENT ----------------------------------------
			return
		}

		// If we get a map or reduce task, execute it
		if reply.task == "map" {
			fmt.Println("Worker recieved map task") // ----------------------------------------------------- TEMPORARY COMMENT ----------------------------------------
			executeMap(reply.mapTask, mapf)
		} else if reply.task == "reduce" {
			fmt.Println("Worker recieved reduce task") // ----------------------------------------------------- TEMPORARY COMMENT ----------------------------------------
			executeReduce(reply.reduceTask, reducef)
		} else {
			fmt.Println("Worker didn't recieve task") // ----------------------------------------------------- TEMPORARY COMMENT ----------------------------------------
			time.Sleep(time.Second)
		}
	}
}

func executeMap(mapTask MapTask, mapf func(string, string) []KeyValue) {
	// Execute map function on data
	intermediate := mapf(mapTask.filename, fetchInputSplit(mapTask.filename)) // We fit the entire result in memory, don't stream into map output

	// ------------------ Store on local disk

	// Create a file for every R worker
	var iFiles []string
	var openFiles []*os.File
	var encoders []*json.Encoder
	for i := range mapTask.R {
		filename := "mr-"
		filename += "mid-" + strconv.Itoa(mapTask.id) + "-rid-" + strconv.Itoa(i) // Map id, reduce id pair
		iFiles = append(iFiles, filename)

		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("Error creating intermediate file: %v", filename)
		}
		openFiles = append(openFiles, file)

		enc := json.NewEncoder(file)
		encoders = append(encoders, enc)
	}

	// Partition key value pairs, encode in json
	for _, kv := range intermediate {
		rId := ihash(kv.Key) % mapTask.R
		err := encoders[rId].Encode(&kv)
		if err != nil {
			log.Fatalf("Error creating intermediate file: %v", iFiles[rId])
		}
	}

	// Close files
	for _, file := range openFiles {
		file.Close()
	}

	// ------------------ Intermediate data stored

	// Report completion with intermediate locations
	args := MapIntermediate{
		id:     mapTask.id,
		iFiles: iFiles,
	}

	err := call(cSock, "Coordinator.ReportCompletedMapTask", &args, &struct{}{})

	// If the rpc fails we are cooked
	if err != nil {
		log.Fatalf("Error informing map task completion to coordinator")
	}
}

// Fetch input map split, we rely on the assumption that a shared file system is used, normally we should have some connection here
func fetchInputSplit(filename string) string {
	// Read file contents
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	return string(content)
}

func executeReduce(reduceTask ReduceTask, reducef func(string, []string) string) {
	//
	// Collect all data for our partition
	//
	filename := "mr-rid-" + strconv.Itoa(reduceTask.id) // Reduce id
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Error creating reduce input file: %v", filename)
	}
	r := ReduceWorker{filename: filename}
	wg.Add(reduceTask.M) // Start server and wait
	r.server()

	// Request from all map tasks already finished
	for _, iFile := range reduceTask.iFiles {
		go r.getIntermediate(iFile.sock, iFile.filename)
	}

	wg.Wait()

	//
	// Sort key value pairs by key
	//

	// Read the data from the file, assume it all fits in memory.
	intermediate := []KeyValue{}
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err = dec.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}
	file.Close()

	// Sort data in memory. Do not use external sort.
	sort.Sort(ByKey(intermediate))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-{reduce id}.
	//
	oname := "mr-out-" + strconv.Itoa(reduceTask.id)
	ofile, _ := os.Create(oname)

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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

	// Report reduce task completion
	err = call(cSock, "Coordinator.ReportCompletedReduceTask", &struct{}{}, &struct{}{})

	// If the rpc fails we are cooked
	if err != nil {
		log.Fatalf("Error informing reduce task completion to coordinator")
	}
}

// Fetch intermediate data from worker
func (r *ReduceWorker) getIntermediate(sock, filename string) error {
	// Connect to map worker and read file
	contentReply := Content{}
	filenameArg := Filename{filename}
	err := call(sock, "Worker.GetFile", &filenameArg, &contentReply)

	if err != nil {
		// Worker failure, just give up, I dont want to deal with this
		log.Fatalln("Failure getting intermediate file from worker.")
	}

	// Save content in reduce worker struct
	m.Lock()
	_, err = r.file.Write(contentReply.content)
	if err != nil {
		log.Fatalln("Error writing to reduce input file")
	}
	m.Unlock()
	wg.Done()

	return nil
}

// Recieve location of map files
func (r *ReduceWorker) SendMapIntermediate(args *IFile, reply *struct{}) error {
	// Get intermediate content
	r.getIntermediate(args.sock, args.filename)
	return nil
}

// Recieve location of map files
func (w *WorkerServer) GetFile(args *Filename, reply *Content) error {
	// Fetch content
	// Assume all requests are valid: Reduce worker accesses only its completed intermediate file
	// With that assumption we can run the server concurrently with no locks

	// Read file contents
	file, err := os.Open(args.filename)
	if err != nil {
		log.Fatalf("cannot open %v", args.filename)
	}
	content, err := io.ReadAll(file)
	file.Close()

	if err != nil {
		log.Fatalf("cannot read %v", args.filename)
	}

	// Send data
	reply.content = content

	return nil
}

// start a thread that listens for RPCs from worker.go
func (r *ReduceWorker) server() {
	rpc.Register(r)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := workerSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Start the worker server that listens for file transfer requests from reduce tasks
func (w *WorkerServer) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := workerSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
