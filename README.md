# Eventually Consistent Key-Value Store

 An eventually consistent key-value store that borrows designs from Amazon DynamoDB and Apache Cassandra.
 
 ## Prerequisites
 
 1. Google protobuf
 2. Java Development kit
 
 __Note:__ Protobuf.jar file is in the lib folder, after downloading, update the path to the protobuf in the following files.
 - client.sh
 - server.sh
 - Makefile
 
## Instructions

Compiling the project:

```
make
```

Starting replica server:

```
./server.sh {serverName} {port} {path to configuration file} {configuration value}
```

__Note:__ Configuration value: 1 -> Read repair,  0 -> Hinted Hand-off

Starting client:

```
./client.sh {coordinator serverName} {path to configuration file}
```

**Important:**  
Client is designed to be interactive for get and put requests. 
- Get syntax: get,key,consistencyLevel
- Put syntax: put,key,value,consistencyLevel

**Killing servers**  
To explicitly stop/kill the replica server(process) on a particular port(Ctrl+C sometimes does not kill process running on that port, it stops the program, not the socket), use command:- 

```
kill $(lsof -t -i:port)
```
## Project Description

### Key-Value Store  
Each replica server will be a key-value store. Keys are unsigned integers between 0 and 255. Values are strings. Each replica server should support the following key-value operations:

- get key - given a key, return its corresponding value
- put key value - if the key does not already exist, create a new key-value pair; otherwise, update the key to the new value.

For simplicity, each replica stores key-value pairs in its memory, that means content are not flushed to a persistent storage. As with Cassandra, to handle a write request, replica first logs the write in a write-ahead log on persistent storage before updating its in-memory data structure. In this way, if a replica failed and restarted, it can restore its memory state by replaying the disk log.

### Eventual Consistency  
The distributed key-value store has four replicas. Each replica server is pre-configured with information about all other replicas. The replication factor will be 4 – every key-value pair is stored on all four replicas. Every client request (get or put) is handled by a coordinator. Client can select any replica server as the coordinator. Therefore, any replica can be a coordinator.

1. **Consistency level**  
Similar to Cassandra, consistency level is configured by the client. When issuing a request, put or get, the client explicitly specifies the desired consistency level: ONE or TWO. For example, receiving a write request with consistency level TWO, the coordinator will send the request to all replicas (including itself). It will respond successful to the client once the write has been written to two replicas. For a read request with consistency level TWO, the coordinator will return the most recent data from two replicas. To support this operation, when handling a write request, the coordinator should record the time at which the request was received and include this as a timestamp when contacting replica servers for writing.  	
With eventual consistency, different replicas may be inconsistent. For example, due to failure, a replica misses one write for a key k. When it recovers, it replays its log to restore its memory state. When a read request for key k comes next, it returns its own version of the value, which is inconsistent. To ensure that all replicas eventually become consistent, you will implement the following two procedures, and your key-value store will be configured to use either of the two. 
	
2. **Read Repair**  
When handling read requests, the coordinator contacts all replicas. If it finds inconsistent data, it will perform “read repair” in the background. 
	
3. **Hinted Hand-off**   
During write, the coordinator tries to write to all replicas. As long as enough replicas have succeeded, ONE or TWO, it will respond successful to the client. However, if not all replicas succeeded, e.g., three have succeeded but one replica server has failed, the coordinator would store a “hint” locally. If at a later time the failed server has recovered, it might be selected as coordinator for another client’s request. This will allow other replica servers that have stored “hints” for it to know it has recovered and send over the stored hints. If not enough replicas are available, e.g., consistency level is configured to TWO, but only one replica is available, then the coordinator should return an exception to the issuing client.
	
### Client  
Client issues a stream of get and put requests to the key-value store. Once started, the client will act as a console, allowing users to issue a stream of requests. The client selects one replica server as the coordinator for all its requests. That is, all requests from a single client are handled by the same coordinator. You can launch multiple clients, potentially issue requests to different coordinators at the same time.

**List of Databases using Eventual consistency**  
- MongoDB
- CouchDB
- Amazon simpleDB
- Amazon DynamoDB
- Riak
- DeeDS
- ZATARA

Here are links to learn more about Eventual Consistency.  
1. [Eventual Consistency - Wikipedia](https://en.wikipedia.org/wiki/Eventual_consistency "Wikipedia")
2. [Eventually Consistent Databases: State of the Art](https://mariadb.org/eventually-consistent-databases-state-of-the-art/ "MariaDB Foundation")
3. [Dynamo: Amazon’s Highly Available Key-value Store](http://s3.amazonaws.com/AllThingsDistributed/sosp/amazon-dynamo-sosp2007.pdf)
4. [Eventually consistent - Stanford University](https://cs.stanford.edu/people/chrismre/cs345/rl/eventually-consistent.pdf) 


## Project Demo

#### Test Case 1 - **Key-value store put/get**
Start four replicas: S1, S2, S3 S4, in hinted handoff mode. Initially all four replicas are empty.  
A Client C1, selects S1 as Coordinator. 

|Client|Coordinator|Consistency|Operation|Key|Value|
|:---:|:---:|:---:|:---:|:---:|:---:|
|C1|S1|ONE|put|10|aaa|

Another client, C2, selects S2 as coordinator

|Client|Coordinator|Consistency|Operation|Key|Value|
|:---:|:---:|:---:|:---:|:---:|:---:|
|C2|S2|ONE|get|10|(**Expected result:aaa**)|

#### Test Case 2 - **Write-ahead Log**  
Kill all servers via command line, then restart S1. A client C3 issues request.  

|Client|Coordinator|Consistency|Operation|Key|Value|
|:---:|:---:|:---:|:---:|:---:|:---:|
|C3|S1|ONE|get|10|(**Expected result:aaa**)|

#### Test Case 3 - **Hinted Hand-off**
Restart S2, S3, S4. All four replicas are running. Kill S1. A client C4 to issue request.  

|Client|Coordinator|Consistency|Operation|Key|Value|
|:---:|:---:|:---:|:---:|:---:|:---:|
|C4|S2|ONE|put|10|bbb|

Restart S1, Instruct another client, C5, to issue request:  

|Client|Coordinator|Consistency|Operation|Key|Value|
|:---:|:---:|:---:|:---:|:---:|:---:|
|C5|S1|ONE|put|20|ccc|

Kill S2, S3, S4. Instruct C5 to issue request

|Client|Coordinator|Consistency|Operation|Key|Value|
|:---:|:---:|:---:|:---:|:---:|:---:|
|C5|S1|ONE|get|10|(**Expected result:bbb**)|

#### Test Case 4 - **Configurable Consistency Level**
With S2, S3, S4 killed, instruct C5 to issue request:

|Client|Coordinator|Consistency|Operation|Key|Value|
|:---:|:---:|:---:|:---:|:---:|:---:|
|C5|S1|TWO|get|10|(**Expected result:exception**)|

#### Test Case 5 - **Read Repair**
Start four replicas,S5,S6,S7,S8 in read repair mode.  
Instruct a Client C6 to issue request.

|Client|Coordinator|Consistency|Operation|Key|Value|
|:---:|:---:|:---:|:---:|:---:|:---:|
|C6|S5|TWO|put|30|ddd|

Kill S8.

|Client|Coordinator|Consistency|Operation|Key|Value|
|:---:|:---:|:---:|:---:|:---:|:---:|
|C6|S5|TWO|put|30|eee|

Restart S8, A new client C7 to issue request:

|Client|Coordinator|Consistency|Operation|Key|Value|
|:---:|:---:|:---:|:---:|:---:|:---:|
|C7|S8|ONE|get|30|(**Expected result:ddd**)|

Instruct C6 to issue request:

|Client|Coordinator|Consistency|Operation|Key|Value|
|:---:|:---:|:---:|:---:|:---:|:---:|
|C6|S5|TWO|get|30|(**Expected result:eee**)|

Kill S5,S6,S7. Instruct C7 to issue request:

|Client|Coordinator|Consistency|Operation|Key|Value|
|:---:|:---:|:---:|:---:|:---:|:---:|
|C7|S8|ONE|get|30|(**Expected result:eee**)|
