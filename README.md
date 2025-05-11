# Distributed Systems Labs
I did MIT's 6.5840 course: https://pdos.csail.mit.edu/6.824/index.html. This repo has my solutions for those labs.

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
