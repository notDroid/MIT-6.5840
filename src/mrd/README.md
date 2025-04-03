# Map Reduce Implementation Explained
<img width="735" alt="Screenshot 2025-04-02 at 1 10 33 AM" src="https://github.com/user-attachments/assets/59ed68b2-f42a-472c-bda3-2b041ed2c798" />

### Processes:
**Coordinator:** Starts with M (# map tasks) and R (# reduce tasks). Coordinator assigns and tracks map and reduce tasks.
**Worker:** Polls coordinator for task to execute.

### EC2
Start free tier instances, one coordinator, multiple workers. Set up:
```
sudo yum update -y
sudo yum install git -y

wget https://go.dev/dl/go1.24.2.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.24.2.linux-amd64.tar.gz
echo "export PATH=$PATH:/usr/local/go/bin" >> ~/.bashrc
source ~/.bashrc

sudo yum groupinstall "Development Tools"
git clone https://github.com/notDroid/MIT-6.5840

aws configure

cd MIT-6.5840/src/main
```
Start coordinator:
```
go run mrcoordinatord.go pg-*.txt
```
Start worker:
```
go build -buildmode=plugin ../mrapps/wcd.go
go run mrworkerd.go wcd.so 172.31.5.21:8000 &
```

## Phase 1: Assign Map
If we have map tasks left, assign map. 

1. Worker determined by:
    * Unix socket (listening process)
    * Real Distributed: location (ip), port (listening port for reduce worker, which is determined by pid)

2. Assign map-id: 
    * Keep a set of map ids that need to be resolved.
    * Take an id from the set.

3. Send task info:
    * Filename
  
4. Execute map task:
    * Fetch input split from s3.
    * Execute map function in memory (we could use double buffering instead).
    * Partition into R files using h_r, encode key-values with json. Write intermediate files to s3.
    * Report completed map task to coordinator.

## Phase 2: Assign Reduce
If we have no more map tasks to assign, and reduce tasks left, assign reduce.

1. Assign reduce-id:
    * Keep a set of reduce ids that need to be resolved.
    * Take an id from the set.
  
2. Send task info:
    * Send ids of map tasks completed so far.
  
3. Execute reduce task:
    * Start reduce server, fetch intermediate files from s3 server, wait for more locations from coordinator.
       * We don't want to wait indefinetly, add a timeout period.
    * Once all intermediate data is read, sort the data by key, assume partition fits in memory (do not external sort).
    * Execute reduce function for each unique key in memory. Write outputs files to s3.
    * Report completed reduce task to coordinator.
  
## Fault Tolerance
### Incomplete Failures

Map or reduce server failure before completion:
  1. **Timeout:** Every few seconds preform a check if map/reduce tasks have taken too long, invalidate them.
      * Keep track of task creation time.
  2. **Pinging:** We could also use a more complex policy like asking the map task if its alive, with longer timeout period.

**Invalidate:**
  1. Add id back to map/reduce ids left set.
  2. Delete from in progress tasks map (which has start times).
  3. If its a reduce task, tell it to stop its server if its still up.

**Backup Tasks:**
   * We could kind of implement backup tasks by allowing invalidated tasks to return valid results, this doesn't work for waiting reduce tasks though.
