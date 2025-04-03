# Map Reduce Implementation Explained
<img width="735" alt="Screenshot 2025-04-02 at 1 10 33 AM" src="https://github.com/user-attachments/assets/59ed68b2-f42a-472c-bda3-2b041ed2c798" />

### Processes:
**Coordinator:** Starts with M (# map tasks) and R (# reduce tasks). Coordinator assigns and tracks map and reduce tasks.
**Worker:** Polls coordinator for task to execute.

## Phase 1: Assign Map
If we have map tasks left, assign map. 

1. Worker determined by:
    * Unix socket (listening process)
    * Real Distributed: location (ip), port (listening process)

2. Assign map-id: 
    * Keep a set of map ids that need to be resolved.
    * Take an id from the set.

3. Send task info:
    * Filename
    * Real Distributed: Server serving file + filename
  
4. Execute map task:
    * Fetch input split.
    * Execute map function in memory (we could use double buffering instead).
    * Partition into R files using h_r, encode key-values with json.
    * Report completed map task to coordinator.

## Phase 2: Assign Reduce
If we have no more map tasks to assign, and reduce tasks left, assign reduce.

1. Assign reduce-id:
    * Keep a set of reduce ids that need to be resolved.
    * Take an id from the set.
  
2. Send task info:
    * Send intermediate file locations collected so far.
  
3. Execute reduce task:
    * Start reduce server, fetch intermediate files from worker server, wait for more locations from coordinator.
    * Keep a read set, if fetch is successful add to read set, otherwise report worker to coordinator.
    * Once all intermediate data is read, sort the data by key, assume partition fits in memory (do not external sort).
    * Execute reduce function for each unique key in memory, write output to local disk.
    * Report completed reduce task to coordinator.
  
## Fault Tolerance
### Incomplete Failures

Map or reduce server failure before completion:
  1. **Timeout:** Every few seconds preform a check if map/reduce tasks have taken too long, invalidate them.
      * Keep track of task creation time.
  2. **Pinging:** Don't use a more complex policy like asking the map task if its alive, with longer timeout period.

Invalidate in progress tasks:
  1. Add id back to map/reduce ids left set.
  2. Delete from in progress tasks map (which has start times).