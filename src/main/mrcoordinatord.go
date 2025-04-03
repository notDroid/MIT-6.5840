package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"log"
	"os"
	"time"

	"6.5840/mrd"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
	mrd.InitS3Config("mapreduce-wali", "us-east-2")

	// Get ip
	ip, err := mrd.GetEC2PrivateIP()
	if err != nil {
		log.Fatalln("Error getting private ip:", err)
	}

	fmt.Println("Starting MapReduce coordinator:", ip)

	// Send input files to s3
	for _, filename := range os.Args[1:] {
		err = mrd.LocalPathToS3(filename, "input/"+filename)
		if err != nil {
			log.Fatalf("Couldn't upload file to s3 %v: %v", filename, err)
		}
	}

	m := mrd.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
