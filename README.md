## Build
```
mkdir -p build
cd build
cmake ..
make
```

## Usage

Create a topic config JSON that contains an array of topic, each with (topic name, number of partitions, replication factor)
```
[
    {
      "name": "chat",
      "partitions": 2,
      "replication_factor":3
    }, 
    {
      "name": "event",
      "partitions": 2,
      "replication_factor":3
    }
]
```

Create a controller (must have id=0) that waits for 2 brokers.  The controller will read this topic config to create and do partition assignment of topic-partitions.
```
./bin/controller <my_id> <my_ip> <my_port> <num_brokers> <topic json path> <log file path> <load_balance_leaders_flag>

Example:
./bin/controller 0 127.0.0.1 8000 2 simple_topic.json log.dat 1
```

Create 2 broker:
```
./bin/broker <my_id> <my_ip> <my_port> <controller_id> <controller_ip> <controller_port> <broker_log_folder> <debug_print_flag>

Example:
./bin/broker 1 127.0.0.1 9001 0 127.0.0.1 8000 ./broker1_logs/ 1
./bin/broker 2 127.0.0.1 9002 0 127.0.0.1 8000 ./broker2_logs/ 1
```

The Kafka Cluster is initialized by starting controller, then starting all `num_brokers` brokers.


To interact with Kafka Cluster:

Create an interactive producer:  The bootstrap broker can be any broker, it's only used once for fetching cluster metadata 
```
./bin/producer <my_id> <bootstrap_broker_ip> <bootstrap_broker_port> 

Example:
./bin/producer 100 127.0.0.1 9001
```

Create a consumer:  The bootstrap broker can be any broker, it's only used once for fetching cluster metadata 
```
./bin/consumer <my_id> <bootstrap_broker_ip> <bootstrap_broker_port> <topic>

Example:
./bin/consumer 200 127.0.0.1 9001 chat
```


## Evaluation

I was only able to verify in a very limited and interactive setting for the usage of the system (all in the same machine as different processes)  
I didn't have time to find the bug that is causing segmentation fault that can be seen when spawning large number of producers and consumers.


### Usage Evaluation

Goal: Multiple producers and multiple consumers can send each other messages, having multiple topics and partitions.

Steps:

1. Create and edit topic json:

```
cd build/
touch topic.json
```
```
[
    {
      "name": "chat",
      "partitions": 2,
      "replication_factor": 4 
    },
    {
      "name": "event",
      "partitions": 2,
      "replication_factor": 4 
    }
]
```

2. Run controller (final arg = 1 assuming no leader failure and allow load balancing)

```
./bin/controller 0 127.0.0.1 8000 4 topic.json log.dat 1
```

3. Run 4 brokers:

```
./bin/broker 1 127.0.0.1 9001 0 127.0.0.1 8000 ./broker1_logs/ 0
./bin/broker 2 127.0.0.1 9002 0 127.0.0.1 8000 ./broker2_logs/ 0
./bin/broker 3 127.0.0.1 9003 0 127.0.0.1 8000 ./broker3_logs/ 0
./bin/broker 4 127.0.0.1 9004 0 127.0.0.1 8000 ./broker4_logs/ 0
```

You should see the cluster metadata on the controller side, where it shows all brokers should have topic partitions well balanced. 

4. Run the producer evaluation program that periodically sends messages  
```
./bin/producer_eval 1 500 chat 127.0.0.1 9001
```

OR run an interactive producer
```
./bin/producer 101 127.0.0.1 9001
```

5. Run 2 consumers 
```
./bin/consumer 201 127.0.0.1 9001 chat
./bin/consumer 202 127.0.0.1 9001 chat
```

6. IF using interactive producer, send messages via producer to any topic, with any key and value.  E.g., `send chat user1 hi` 
You should see all consumers receiving these messages.

7. Spawn another consumer and see that it also receive these messages.
```
./bin/consumer 203 127.0.0.1 9001 chat
```

8. Spawn a consumer that consumes from topic
```
./bin/consumer 204 127.0.0.1 9001 topic
```


### Failure Tolerance.

Goal: Verify that a topic with replication factor of R will not fail if R − 1 followers brokers (not leader/primary) fail.


The requirement that only followers fail is due to an unknown bug that crashes the cluster whenever a leader fails. For this we will use a secondary (just for this evaluation) partition assignment strategy that assigns one broker as leader of all partitions, and assume it never fails.


1. Steps are the same as before except for controller with last argument = 0. 
```
./bin/controller 0 127.0.0.1 8000 4 topic.json log.dat 0
```

2. To test failure tolerance of followers, kill any R - 2 followers, and see the consumer and producer still work. 

    You can still kill the last follower, making it tolerate R - 1 failures, but at that point the leader (with no replicas) will not update its commit offset/index, so any new messages from producer can't be retrieved by the consumer.








