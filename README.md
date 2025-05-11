# Distributed Systems Labs
I did MIT's [6.5840](https://pdos.csail.mit.edu/6.824/index.html) course. This repo has my solutions for those labs.

## [Lab 1: MapReduce](https://github.com/notDroid/MIT-6.5840/tree/master/src/mr)
- [Instructions](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

In this lab I implement [MapReduce](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf). 
A simple framework where a user provides a map and reduce function,
then calls the the MapReduce library (students responsibility) coordinates the execution of map tasks and reduce tasks across many computers 
in parallel automatically so the user doesn't need to worry about implementing distributed systems logic.

<img width="400" alt="Screenshot 2025-04-02 at 1 10 33 AM" src="https://github.com/user-attachments/assets/59ed68b2-f42a-472c-bda3-2b041ed2c798" />

This lab uses unix sockets instead of communicating over the network to test correctness. 
As part of the challenges section I also ran it over the network using AWS EC2 Instances in this [folder](https://github.com/notDroid/MIT-6.5840/tree/master/src/mrd).

## [Lab 2: Key/Value Server](https://github.com/notDroid/MIT-6.5840/tree/master/src/kvsrv1)
- [Instructions](https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv1.html)

This was an extremely simple lab where we make an in memory key-value store with server.go running on the server side and client.go on the client side. 
Also lock.go which deals with grabbing a distributed lock.

## [Lab 3: Raft](https://github.com/notDroid/MIT-6.5840/tree/master/src/raft1)
- [Instructions](https://pdos.csail.mit.edu/6.824/labs/lab-raft1.html)

### **Description**

In this lab I implemented RAFT which servers as a replicated state machine layer, 
the replication provides fault tolerance (if some computers in the cluster crash) and maybe avaliability (depending on application, load distribution across many computers). 

<img width="300" alt="Raft" src="https://github.com/user-attachments/assets/0263720e-83e9-46e5-a616-ce4687a97a71" />

### **Explanation**

What this means is that RAFT gaurantees (as long as we have a majoriy of the computers up always, otherwise stalled) 
that it will apply the same commands on all computers in a raft cluster in the same order making them all have the same state to the raft client user. 

It does this by electing a leader and letting it order all commands while also making it directly communicate to all followers to replicate its log, on worker failure the leader just catches up the follower.
On leader failure it elects a new leader that is at least up to the majority's log history, this ensures that recent logs that the majority of followers have are commited and never lost. 
There is also snapshotting logic to truncate the logs if they get too big.

<img width="400" alt="Screenshot 2025-05-11 at 2 23 39 AM" src="https://github.com/user-attachments/assets/24dfcbc5-965f-48f1-811a-17c26eecb918" />

## [Lab 4: Fault-tolerant Key/Value Service](https://github.com/notDroid/MIT-6.5840/tree/master/src/kvraft1)
- [Instructions](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft1.html)

### **Description**

In this lab I implemented a key-value store on top of raft providing fault tolerance to the data stored in the key-value store through replication. 
Many distributed systems applicaitons rely on fault tolerant and highly avaliable storage, specifically this type of key-value store, in order to implement distrubted application logic.

### **Why This is Important**
For example mapreduce in lab 1 relies on both its mapreduce data being on a fault tolerant key-value store (like s3, though it uses erasure coding not replication) 
and also saving coordinators current state to make it fault tolerant, in the case of a coordinator failure we could just detect it and boot a new coordinator with its saved state (or raft replicate the coordinator which would be considerably harder than just using the go to replicated key-value store). 
Many distributed applications can be coordinated and fault tolerant through just using a replicated key-value store, as seen with [ZooKeeper](https://www.usenix.org/legacy/event/atc10/tech/full_papers/Hunt.pdf).

<img width="300" alt="Zookeeper" src="https://github.com/user-attachments/assets/d2f83e95-e663-47d7-b652-62eaa7c73aab" />

## [6.5840 Lab 5: Sharded Key/Value Service](https://github.com/notDroid/MIT-6.5840/tree/master/src/shardkv1)
- [Instructions](https://pdos.csail.mit.edu/6.824/labs/lab-shard1.html)

This lab extends lab 4 and distributes partitions of the keys (called shards) among replicated shard groups, this makes it so that we can keep adding shard groups to never run out of space.
In order to do this we need to implement a shard controller which manages moving shards among groups, this shard controller needs its configuration replicated and clients accessing data
must first ask the configuration manager which shard group holds the shard (partition of the key space) they want to access.

<img width="300" alt="Sharded Key Value" src="https://github.com/user-attachments/assets/edaf94ea-2902-40dc-b21f-cbe8fc66b2c8" />
