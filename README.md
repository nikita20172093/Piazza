# Piazza
Distributed Key Value Store

    
	DISTRIBUTED KEY VALUE STORE


INTRODUCTION

Multiple clients will be communicating with a single coordinating server in a given messaging format (JSON Format) . The coordinator contains a write-through set-associative cache , and it uses the cache to serve GET requests without going to the slave servers (Key-Value servers), it coordinates. The slave servers are contacted for a GET only upon a cache miss on the coordinator. The coordinator will select the server on the basis of consistent hashing to forward client requests for PUT and DEL to multiple slave servers and follow the 2PC protocol for atomic PUT and DEL operations across multiple slave servers.








About project in brief:


 Each key will be stored using 2PC in two key-value servers; the first of them will be selected using consistent hashing, while the second will be placed in the successor of the first one. There will be at least two key-value servers(Slave servers) in the systems.



 Key-value servers will have 256-bytes globally unique IDs , and they will register with the coordinator with that ID when they start. The total number of key-value servers are fixed, at any moment there will be at most one down server, and they always come back with the same ID if they crash.




Update operations irrespective of which key they are working on (i.e., 2PC PUT and DEL operations are performed one after another), but retrieval operations (i.e., GET) of different keys must be concurrent unless restricted by an ongoing update operation on the same set.



The coordinator server includes a write-through set-associative cache, which do have the same semantics as the write-through cache we used before. Caches at key-value servers will still remain write-through.



Individual key-value i.e. slave servers must use random ports assigned upon creating respective SocketServers for listening to 2PC requests .




TERMINOLOGIES:

CLIENT
Client is an entity which asks various services from the coordinator.
Multiple Clients can communicate with the single coordinating server at the same time.
The services that a client can ask for are:-
I. GET
Client asks for a particular value from the coordinating server by providing a key. The response that a client expects is a value corresponding to the provided key.
II. PUT
Clients share a same repository of values.Each client can share a value using the PUT command. The client shares the value by sending a PUT(K,V) message where K is Key and V is Value corresponding to that key.
There is one-to-one mapping between keys and values stored at the server side.
III. DELETE
If a client wishes to delete a particular value, it requests coordinator with the  DEL(K) message.
The value will not be available for future GET(K) requests unless a PUT(K,V) request is made.







CO-ORDINATOR SERVER:
   
1. This will be coordinator server which would work as interface between clients and slave servers as well as this maintains a write through cache in it(to reduce average effective time to search for an value against a particular key).
2.  As soon as it gets a request from client,it performs following tasks:
    

I. PUT:  When server receives a PUT request from client, it hashes the key and 	redirect key along its value(message) to a certain slave server (and its 	successor)accordingly in JSON format. Here 2PC mechanism works in	 which if any of above two slave servers is down then none of these 	changes should get commited or updated.

II. GET: When any of the clients request for value for a key(K),this server 	receives a GET(K) request.As soon as server gets this request,it starts 	searching in its own cache,If cache hits then server replies by then itself 	otherwise this request will be redirected to intended slave server and 	response will be sended to client as reply for the query.

III. DEL: DEL(K) keyword is used for a request from client to delete a entry for a particular key from shared data source. This key K will be hashed in server side and send to both slave server as per distributed criteria(consistent hashing) to delete entry for this key(K).(2PC mechanism works for DEL request as well)







SLAVE SERVER:
           
    There will be minimum two slave servers which will co-ordiante with keys.
                All servers are connected in a cyclic form to each other(last slave server is connected to first slave server). Each server has duplicate keys of its predecessor as secondary  storage.


Duplication storage of keys for less data loss: For each slave server, there exists its 	succesor which will maintain same keys as predecessor as secondary data. So        	that if any slave gets down,it can still manage to recover those keys as well as 	can be able to response for clients query as clients have already shared these 	data with server. When slave gets up again it recovers its data from its 	succesor and sends a notification to coordiantor server to let know that server is 	now ready for data exchange.   


Working: As soon as slave server gets up,it sends message to coordinate server for 	its successor address,after getting information about its successor which is 	having complelte set of data as backup of its predecessor, send message to get 	recovery of data. Successor provided complete bunch of data relevant to that 	slave server and process continues as before.










HEARTBEAT MESSAGE:
            
                      Heart beat message is used to monitor whether slave is alive.

	In a particular interval slave servers send a dummy message or notification to 	main server. When server senses that for any slave server’s heart beat is not updated, it gets to know that this slave server is down and it can not commit or update any value which were supposed to store in the same slave server.


I.        We have used UDP connection to maintain heart beat messages(reason behind this is: server need not to reply for each heart beat message but only updation at server side is good enough).


II. There is one thread in client side which will send heart beat message to server every now and then(in our project it is 5 seconds).
Similarly we maintained a thread at server side which monitors these heart beat messages and keeps record whether client is in existence.
Consistent hashing:
		DHTs use consistent hashing to partition a key space among a distributed set of nodes. Consistent hashing is based on mapping each object to a point on the edge of a circle (or equivalently, mapping each object to a real angle). The system maps each available machine (or other storage bucket) to many pseudo-randomly distributed points on the edge of the same circle.
	To find where an object should be placed, the system finds the location of that object's key on the edge of the circle; then walks around the circle until falling into the first bucket it encounters (or equivalently, the first available bucket with a higher angle). The result is that each bucket contains all the resources located between each one of its points and the previous points that belong to other buckets.



SC (Second Chance):(REPLACEMENT POLICY FOR CACHE IN SERVER SIDE)
	
	In the Second Chance page replacement policy, the candidate pages for
	removal are consider in a round robin matter, and a page that has been
	accessed between consecutive considerations will not be replaced.
	The page replaced is the one that - considered in a round robin matter - has not
	been accessed since its last consideration.

 Implementation:
1.     Add a "second chance" bit to each memory frame.
2.     Each time a memory frame is referenced, set the "second chance" bit to
 	1 this will give the frame a second chance.
3.      A new page read into a memory frame has the second chance bit set to 	0.
4.       When you need to find a page for removal, look in a round robin     	manner
in the memory frames:
 If the second chance bit is 1, reset its second chance bit (to 0) and
 	continue.
 If the second chance bit is 0, replace the page in that memory
frame.

Advantage:-
Better Complexity than LRU policy.



ASSUMPTIONS that we made:
1. Only one slave can get down at a time(or terminates).
2. Coordiantor server never get terminated till program execution ends.
3. Max key size and message size is 256 bytes.
4. Cache size is fixed at server side.
5. TCP connection till end except heart beat messages.
