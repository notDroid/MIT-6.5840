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
	listener *net.Listener
	readSet  map[int]struct{}
	id       int
	sock     string
}

type WorkerServer struct{}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var cSock = CoordinatorSock() // Runtime constant
var wSock = WorkerSock()      // Runtime constant, as long as we start execution from entry point mrworker.go entry point
var m = sync.Mutex{}          // Worker mutex
var wg = sync.WaitGroup{}     // Reduce wait for all map files

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Set up file request server
	w := WorkerServer{}
	w.server()

	// Poll until task assigned
	for {
		args := Identifier{wSock}
		reply := TaskReply{}
		err := RPCall(cSock, "Coordinator.GetTask", &args, &reply)

		// Coordinator didn't respond, assume its done
		if err != nil {
			// fmt.Println("Coordinator didn't respond, giving up:", err)
			return
		}

		// If we get a map or reduce task, execute it
		if reply.Task == "map" {
			fmt.Println("Worker recieved map task:", reply.MapTask.Id)
			executeMap(reply.MapTask, mapf)
		} else if reply.Task == "reduce" {
			fmt.Println("Worker recieved reduce task")
			executeReduce(reply.ReduceTask, reducef)
		} else {
			fmt.Println("Worker didn't recieve task")
			time.Sleep(time.Second)
		}
	}
}

func executeMap(mapTask MapTask, mapf func(string, string) []KeyValue) {
	// Execute map function on data
	intermediate := mapf(mapTask.Filename, fetchInputSplit(mapTask.Filename)) // We fit the entire result in memory, don't stream into map output

	// ------------------ Store on local disk

	// Create a file for every R worker
	var iFiles []string
	var openFiles []*os.File
	var encoders []*json.Encoder
	for i := range mapTask.R {
		filename := "mr-"
		filename += "mid-" + strconv.Itoa(mapTask.Id) + "-rid-" + strconv.Itoa(i) // Map id, reduce id pair
		iFiles = append(iFiles, filename)

		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("Error creating intermediate file %v: %v\n", filename, err)
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
			log.Fatalln("Error creating intermediate file:", iFiles[rId])
		}
	}

	// Close files
	for _, file := range openFiles {
		file.Close()
	}

	// ------------------ Intermediate data stored

	// Report completion with intermediate locations
	args := MapIntermediate{
		Id:     mapTask.Id,
		Sock:   wSock,
		IFiles: iFiles,
	}

	err := RPCall(cSock, "Coordinator.ReportCompletedMapTask", &args, &struct{}{})

	// If the rpc fails we are cooked
	if err != nil {
		log.Fatalln("Error informing map task completion to coordinator:", err)
	}
}

// Fetch input map split, we rely on the assumption that a shared file system is used, normally we should have some connection here
func fetchInputSplit(filename string) string {
	// Read file contents
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v: %v\n", filename, err)
	}
	content, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		log.Fatalf("cannot read %v: %v\n", filename, err)
	}

	return string(content)
}

func executeReduce(reduceTask ReduceTask, reducef func(string, []string) string) {
	//
	// Collect all data for our partition
	//
	filename := "mr-rid-" + strconv.Itoa(reduceTask.Id) // Reduce id
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Error creating reduce input file %v: %v\n", filename, err)
	}

	r := ReduceWorker{
		filename: filename,
		file:     file,
		readSet:  make(map[int]struct{}),
		id:       reduceTask.Id,
		sock:     ReduceSock(),
	}
	wg.Add(reduceTask.M) // Start server and wait
	r.server()

	// Request from all map tasks already finished
	for id, iFile := range reduceTask.IFiles {
		go r.getIntermediate(id, iFile.Sock, iFile.Filename)
	}

	wg.Wait()
	r.close() // Close the server

	//
	// Sort key value pairs by key
	//

	// Reset the file cursor to the beginning
	_, err = file.Seek(0, 0)
	if err != nil {
		log.Fatalln("Couldn't reset cursor in reduce input file", err)
	}

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
	oname := "mr-out-" + strconv.Itoa(reduceTask.Id)
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
	reduceIdentifier := ReduceIdentifier{
		Id:   reduceTask.Id,
		Sock: wSock,
	}
	err = RPCall(cSock, "Coordinator.ReportCompletedReduceTask", &reduceIdentifier, &struct{}{})
	// fmt.Println("Reduce Worker Finished:", reduceTask.Id)

	// If the rpc fails we are cooked
	if err != nil {
		log.Fatalln("Error informing reduce task completion to coordinator:", err)
	}
}

// Fetch intermediate data from worker
func (r *ReduceWorker) getIntermediate(id int, sock, filename string) {
	// Connect to map worker and read file
	contentReply := Content{}
	filenameArg := Filename{filename}
	err := RPCall(sock, "WorkerServer.GetFile", &filenameArg, &contentReply)

	if err != nil {
		// Worker failure
		log.Println("Failure getting intermediate file from worker:", err)

		// Send invalidation request
		invalidRequest := ReduceInvalidRequest{
			RId:  r.id,
			Sock: sock,
			MId:  id,
		}

		err = RPCall(cSock, "Coordinator.ReportInvalid", &invalidRequest, &struct{}{})

		if err != nil {
			log.Fatalln("Error informing the coordinator about invalid map server:", err)
		}
	}

	// Save content in reduce worker struct
	m.Lock()
	r.readSet[id] = struct{}{}
	_, err = r.file.Write(contentReply.Content)
	if err != nil {
		log.Fatalln("Error writing to reduce input file:", err)
	}
	m.Unlock()
	wg.Done()
}

// Recieve location of map files
func (r *ReduceWorker) SendMapIntermediate(args *MIFile, reply *struct{}) error {
	// Get intermediate content
	r.getIntermediate(args.Id, args.Sock, args.Filename)
	return nil
}

// Recieve location of map files
func (w *WorkerServer) GetFile(args *Filename, reply *Content) error {
	// Fetch content
	// Assume all requests are valid: Reduce worker accesses only its completed intermediate file
	// With that assumption we can run the server concurrently with no locks

	// Read file contents
	file, err := os.Open(args.Filename)
	if err != nil {
		log.Fatalf("cannot open %v: %v\n", args.Filename, err)
	}
	content, err := io.ReadAll(file)
	file.Close()

	if err != nil {
		log.Fatalf("cannot read %v: %v\n", args.Filename, err)
	}

	// Send data
	reply.Content = content

	return nil
}

// Start a thread to listen for coordinator messages for map intermediate locations
func (r *ReduceWorker) server() {
	server := rpc.NewServer()
	server.Register(r)

	// Create a new ServeMux for this server
	mux := http.NewServeMux()
	mux.Handle("/_goRPC_", server)

	sockname := ReduceSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalln("listen error:", e)
	}
	r.listener = &l

	go http.Serve(l, mux)
}

func (r *ReduceWorker) close() {
	(*r.listener).Close()
}

// Start a thread to listen for intermediate file transfer requests for reduce tasks
func (w *WorkerServer) server() {
	server := rpc.NewServer()
	server.Register(w)

	mux := http.NewServeMux()
	mux.Handle("/_goRPC_", server)

	sockname := WorkerSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalln("listen error:", e)
	}
	go http.Serve(l, mux)
}
