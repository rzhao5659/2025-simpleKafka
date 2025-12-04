

## Build
```
mkdir -p build
cd build
cmake ..
make
```

```
cmake -DCMAKE_BUILD_TYPE=Debug ..
make
```
## Usage

Create a uber driver as producer
```
./bin/producer 1 "Bob" 127.0.0.1 10000 2    

gdb ./bin/log_test
```

Need to remove log.dat every single time sorry and contrller id must be 0, and all ids must be diff between prod cons broker
```
./bin/controller 0 127.0.0.1 8000 2 simple_topic.json log.dat
./bin/broker 1 127.0.0.1 9001 0 127.0.0.1 8000 ./broker1_logs/
./bin/broker 2 127.0.0.1 9002 0 127.0.0.1 8000 ./broker2_logs/
./bin/producer 100 127.0.0.1 9001
./bin/consumer 200 127.0.0.1 9001 chat
```


```
./bin/controller 0 127.0.0.1 8000 3 simple_topic.json log.dat
./bin/broker 1 127.0.0.1 9001 0 127.0.0.1 8000 ./broker1_logs/ 1
./bin/broker 2 127.0.0.1 9002 0 127.0.0.1 8000 ./broker2_logs/ 1
./bin/broker 3 127.0.0.1 9003 0 127.0.0.1 8000 ./broker3_logs/ 1
./bin/producer 100 127.0.0.1 9001
./bin/consumer 200 127.0.0.1 9001 chat
./bin/consumer 201 127.0.0.1 9001 chat
```




# Other personal notes


The current combo only allows topci addition,. not removal. 
broker -> consumer is entire clustermetarequest
controller -> broker is just changes in emtadatatopic. 
## Producer



## Tested
- marshal and unmarshal all messages/objects
- tested log 
- 

## Code reading verified
- records
- network messages (except fetch)
- cluster


## TODO
- for some reason consumer is one step behind when fetching from the broker that is follower (but when its leader its fast).



- i dont think the controller watchdog is working...

    it keeps spamming Detected failure of brokers [2, 3]
    Removing them from cluster.
    Detected failure of brokers [2, 3]
    Removing them from cluster. 

1.24pm

- 1 hour to finish replication (fix this follower lag, fix truncation of log)
- 1 hour to clean up the mains, new producer main and client main for throughput.
- 1 hour of evaluation.
- 0.5 hour of cleaning code, cmake.
- 2.5 hour of report
- 1 hour of presentation
- 1 hour of video.

10 pm



- remove prints. 

Test:
- test replication works when broker fails (doesnt work, if kill leader, all brokers dies)
- Still have no idea if the replication thread works properly or general replication.
- test that consumer and producer failing wont affect it.





- throughput test, replication test, (idk whats a good way to measure throughput.) 








[monday debug/test everything]. 

[tuesday write report...]




                // Find live broker
                bool found_live;
                while (!found_live) {
                    found_live = brokers_state[broker_assignment_idx_].first;
                    if (!found_live) {
                        broker_assignment_idx_++;
                    }
                }



## Evaluation Plan

- General requirement:  

Show the interactive versions
<!-- 
Use interactive producer and client, Take a picture for each main of producer and client, see how simple it is.
Now show a picture of 3 producers outputting to a certain topic with a certain key (user1, user2, user3). Use timestamp as value! (on 3 different processes on same computer (just use same main, they should all do with some configurable timeperiod))
Now show a picture of 3 consumers receiving (add configurable option to append to file.txt (so fast or not)).   For latency and throughput, measure timestamp and current time. 
 -->


- Failure evaluation: Verify that a topic with replication factor of R will not fail if R − 1 brokers fail, assuming the leader never fails.

1. Setup: 4 brokers, 2 topic, 2 partitions, 4 replication factor.  broker 1 is leader.
2. Run two producers (super slow to make it interactive), run two consumers (consumes from both topics and prints out)
3. Kill one consumer, kill one producer. to show that it tolerates these failures,  add back producer and consumer.
4. Kill 2 replicas, and see the flow from producer to consumer stays the same.
5. Spawn another consume just to see it consumes same message.
6. Kill last replica will make leader not commit anymore as it has no replication, so new messages dont go to consumers.  Spawn another consumer to show.



- Throughput Evaluation

Lets assume no failure, so i will change my load balancing strategy in controller to distribute.
measure throughput (msg/s) for producer (total) and consumers (total).

Use only 1 topic.

 Basic benchmark:
Test case 1.: 1 producers -> 1 broker 1 partition 2 replicas factor -> 1 consumer
Test case 2.: 4 producers -> 1 broker 1 partition 2 replicas factor -> 4 consumer
Test cases3. : 8 producers -> 1 broker 1 partition 2 replicas factor -> 8 consumer
This suffers from same broker getting queried by 8 different consumers, and produced by 8 different producers, so thorughput should suffer.


Now increase number of brokers: 
Increasing number of brokers basically allows replications to live across brokers, so 2 brokers are active (all 4 consumers consumes from this)
- THis increases consumer throughput since all 2 replicas live across different brokers, and consumers pick brokere by load balancing random.  
- However, producer throgguhput should still be limited as they all write to the same broker that is the leader. 
Test cases3. : 1 producers -> 4 broker 1 partition 2 replicas factor -> 1 consumer     
Test cases4. : 4 producers -> 4 broker 1 partition 2 replicas factor -> 4 consumer   
Test cases5. : 8 producers -> 4 broker 1 partition 2 replicas factor -> 8 consumer  

Now increase number of partitions: 
Increasing partitions basically allows. partitions are basically data horizontal sharing, since technically each tp holds different data.  
- allows producers to write to different brokers
- allows distribution of load across all 8 brokers, without sacrificing latency issue.  now all 8 consumers actually consume form all 4 brokers!
Test cases3. : 1 producers -> 4 broker 2 partition 2 replicas factor -> 1 consumer     
Test cases4. : 4 producers -> 4 broker 2 partition 2 replicas factor -> 4 consumer   
Test cases5. : 8 producers -> 4 broker 2 partition 2 replicas factor -> 8 consumer  


I need to have a main for creating N producers writing messages at configurable (interval).  Fixed keys like user1,... values monotonous
have a main that creates N consumers, consuming same topic, and uses all partitions. suppose each consumer writes to a file (configurable either writes or prints)


in each terminal spawn a broker. 










