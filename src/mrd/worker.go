package mrd

import (
	"encoding/json"
	"errors"
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

// Handles getting intermediate data
type ReduceWorker struct {
	// Reduce metadata
	rId     int
	readSet map[int]struct{}

	// Reduce input intermediate file
	filename string
	file     *os.File
}

// Handles providing data to reduce workers and redirecting coordinator communications to reduce worker
type WorkerServer struct {
	reduceActive bool
	r            *ReduceWorker
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var wm = sync.Mutex{}         // Worker mutex
var rm = sync.Mutex{}         // Reduce Worker mutex
var wg = sync.WaitGroup{}     // Reduce wait for all map files
var cSock = CoordinatorSock() // Coordinator location
var wSock = WorkerSock()      // Our location
var w = WorkerServer{reduceActive: false}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Set up file request server
	w.server()
	fmt.Println("Worker Started")

	// Poll until task assigned
	for {
		args := WorkerIdentifier{wSock}
		reply := TaskReply{}

		// In case of reduce task, we need to lock and set up the reduce server.
		// Avoid race condition of coordinator sending intermediate file locations before worker is ready.
		wm.Lock()
		err := RPCall(cSock, "Coordinator.GetTask", &args, &reply)

		// Coordinator didn't respond, assume its done
		if err != nil {
			// fmt.Println("Coordinator didn't respond, giving up:", err)
			exit()
		}

		// If we get a map or reduce task, execute it
		if reply.Task == "map" {
			fmt.Println("Worker recieved map task:", reply.MapTask.MId)
			wm.Unlock()
			executeMap(reply.MapTask, mapf)
		} else if reply.Task == "reduce" {
			fmt.Println("Worker recieved reduce task")
			// Unlock in execute reduce function
			executeReduce(reply.ReduceTask, reducef)
		} else {
			fmt.Println("Worker didn't recieve task")
			wm.Unlock()
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
	var encoders []*json.Encoder
	for i := range mapTask.R {
		filename := "mr-"
		filename += "mid-" + strconv.Itoa(mapTask.MId) + "-rid-" + strconv.Itoa(i) // Map id, reduce id pair
		iFiles = append(iFiles, filename)

		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("Error creating intermediate file %v: %v\n", filename, err)
		}
		defer file.Close()

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

	// ------------------ Intermediate data stored

	// Report completion with intermediate locations
	args := MapIntermediate{
		MId:    mapTask.MId,
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

	// SET UP REDUCE SERVER
	filename := "mr-rid-" + strconv.Itoa(reduceTask.RId) // Reduce id
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Error creating reduce input file %v: %v\n", filename, err)
	}
	r := ReduceWorker{
		rId:     reduceTask.RId,
		readSet: make(map[int]struct{}),

		filename: filename,
		file:     file,
	}
	wg.Add(reduceTask.M)
	r.server()
	wm.Unlock() // Server set up

	// Request from all map tasks already finished
	for id, iFile := range reduceTask.IFiles {
		go r.getIntermediate(id, iFile.Sock, iFile.Filename)
	}

	wg.Wait()

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
	oname := "mr-out-" + strconv.Itoa(reduceTask.RId)
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
		RId:  reduceTask.RId,
		Sock: wSock,
	}

	err = RPCall(cSock, "Coordinator.ReportCompletedReduceTask", &reduceIdentifier, &struct{}{})
	wm.Lock() // Im pretty sure we don't need this lock
	r.close() // Close the server
	wm.Unlock()
	// fmt.Println("Reduce Worker Finished:", reduceTask.Id)

	// If the rpc fails we are cooked
	if err != nil {
		log.Fatalln("Error informing reduce task completion to coordinator:", err)
	}
}

// Fetch intermediate data from worker
func (r *ReduceWorker) getIntermediate(mId int, sock, filename string) {
	// Connect to map worker and read file
	contentReply := Content{}
	filenameArg := Filename{filename}
	err := RPCall(sock, "WorkerServer.GetFile", &filenameArg, &contentReply)

	if err != nil {
		// Worker failure
		log.Println("Failure getting intermediate file from worker:", err)

		// Send invalidation request
		invalidRequest := ReduceInvalidRequest{
			RId:   r.rId,
			RSock: wSock,
			MId:   mId,
			MSock: sock,
		}

		err = RPCall(cSock, "Coordinator.ReportInvalid", &invalidRequest, &struct{}{})

		if err != nil {
			log.Fatalln("Error informing the coordinator about invalid map server:", err)
		}
		return
	}

	// Save content in reduce worker struct
	rm.Lock()
	// Check if we already got content because of concurrent requests
	if _, read := r.readSet[mId]; read {
		return
	}

	// Mark read
	r.readSet[mId] = struct{}{}
	_, err = r.file.Write(contentReply.Content)
	if err != nil {
		log.Fatalln("Error writing to reduce input file:", err)
	}
	rm.Unlock()
	wg.Done()
}

// Recieve location of map files
func (r *ReduceWorker) SendMapIntermediate(args *MIFile, reply *struct{}) error {
	// Only call get intermediate if we need the intermediate
	rm.Lock()
	if _, read := r.readSet[args.MId]; read {
		return nil
	}
	rm.Unlock()
	// Get intermediate content
	r.getIntermediate(args.MId, args.Sock, args.Filename)
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
		log.Printf("cannot open %v: %v\n", args.Filename, err)
		return err
	}
	content, err := io.ReadAll(file)
	file.Close()

	if err != nil {
		log.Printf("cannot read %v: %v\n", args.Filename, err)
		return err
	}

	// Send data
	reply.Content = content

	return nil
}

// Start a thread to listen for intermediate file transfer requests for reduce tasks
func (w *WorkerServer) server() {
	rpc.Register(w)
	rpc.HandleHTTP()

	sockname := wSock
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalln("listen error:", e)
	}
	go http.Serve(l, nil)
}

// RPC wrapper for exit
func (w *WorkerServer) PleaseExit(args *struct{}, reply *struct{}) {
	exit()
}

// We exit the worker, files will no longer be served, active map/reduce task abadonded
func exit() {
	os.Remove(wSock) // Close socket
	os.Exit(1)
}

// Worker server reduce wrappers
func (w *WorkerServer) SendMapIntermediate(args *MIFile, reply *struct{}) error {
	wm.Lock()
	if !w.reduceActive {
		fmt.Println("Race condition")
		return errors.New("no reduce worker active")
	}
	wm.Unlock()

	return w.r.SendMapIntermediate(args, reply)
}

// Open reduce server
func (r *ReduceWorker) server() {
	w.reduceActive = true
	w.r = r
}

// Close reduce server
func (r *ReduceWorker) close() {
	w.reduceActive = false
	w.r = nil
}
