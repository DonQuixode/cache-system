# cache-system

Redis is an in-memory cache. What this means, is that the data present is stored in the RAM of the machine for quick access. 
This project focuses on developing a distributed version of Redis as a Sping Boot project. There are two components:

Redis-base: Turns a machine into a Redis node for storage. Comes prepackaged with Sentinel, a service that allows node-to-node communication for replication and durability. Implements a master-slave dynamic. Clients are registered in each node for updation functionality in case of new master election.
Redis-client: A library that is implemented at each machine that calls the Redis-base. It stores the addresses of the nodes for quick access to specific calls and exposes a webhook API that is triggered after master election.

Redis-base:
KVStore class is the class for key-value storage. It stores Strings, Lists, Integers, Sets, and Maps as values. All keys are String. ConcurrentHashMaps are used as buckets for thread safety.   At initialisation, a readRDB() function reads from the RDB snapshot (if present). Every 5 minutes, a snapshot is taken of the database and stored in the disk memory for resilience in case of system failure. In the future, there can be an Append-Only-Log(AOL) that can work alongside the snapshot for better resilience. As of now, RDB was selected for its quickness. 
KVStore object is also initialised with RedisNodes object that stores the addresses of all nodes and RedisClients object that stores the addresses of all the clients. 
RedisNodes class stores the addresses of all other nodes. It contains a list and a hash of addresses, the former for quick iteration and the latter for checking if a node already exists or not during updation and deletion. 
RedisClients stores the address of all clients that are registered.
Communication is done using REST API calls (can consider gRPC for faster processing in the future):
All requests are mapped to “/v1”.
Put call: writes data to the KVStore. Overrides previous data if present. Sends data to read replicas upon storage.
Get call: returns data from KVStore.
/isMaster/{boolean} :  updates a machine to either master or slave. 
/healthcheck : responsible for checking the health conditions of the node. 
/nodes: for updating the list of nodes inside the RedisNodes object.
/checkIfMaster : checks if a specific node is a master node or not
registerClient: registers a client at that node alongside its port number. 
For inter-nodal communication, Sentinel is utilised. This implements 
an asynchronous method called sendToNodes for communicating PUT requests to read replicas for resilience. 
A cron-job called healthCheck that runs every 2 seconds to check the status of every node. Upon failure of a node, it initiates a quorum call (yet to be implemented) which elects new master (if master went down) and calls the clients’ web hooks to update their address tables.

Redis-client:
In order to call Redis-base, Redis-client library needs to be implemented inside your service. This exposes a webhook that updates the address tables for get and put calls. This ensures that get and put calls are as fast as possible by going to either master (for put) or to slaves (for get). 
Create an object of the Redis-client and utilise it for calling functions. This is a Singleton class.

FAQs
Q1) Why this architecture of having a base and a client library?
Ans: Initially, the design I had in mind was to have an orchestrator/control plane that would do the work of re-routing calls to the required destination (eg: if get, then sending to slave node). However, this created a system where an additional point of failure, the orchestrator, becomes present. As a result, the decision was taken to reduce links in the chain by shifting orchestration functionalities into the nodes themselves, leading to Sentinel Service creation, and to the user’s code that calls it. 
In this manner, resilience and durability increases.
Q2) What if two users write the same key at the same time?
Ans: As this architecture closely follows the logic of Redis, the PUT functionality works to override data. Thus, whichever one arrives first shall be able to be written first. To overcome this, plans are present to implement a universal clock system in the client with a Kafka queue between the clients and the nodes. This queue can take care of same-time writes. However, resilience issues will still be present.
Currently, the nodes are safe enough to not have any concurrency issues within the codebase. 
Q3) Why go with RDB snapshots instead of AOL file?
Ans: RDB snapshots are faster upon restarting and are faster than AOL operations that add every single call to the file. As such, AOLs aren’t recommended alone, only in combination with RDB snapshots. 
The issue, of course, remains any data written after a 5 minute window between backups can be lost if node goes down. 
In short, I have prioritised speed of functionality and tried to keep the system as durable as possible within that constraint. I have tried to keep network calls to a minimum so that latency is as low as possible, as well as reducing links-in-chain wherever possible. The system is eventually-consistent as a result, but nonetheless still highly durable. 
