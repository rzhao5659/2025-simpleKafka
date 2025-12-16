## Overview

This is a 3 weeks project for distributed system course.

It's a simplified implementation of Kafka that is a pub-sub distributed message streaming system, with similar producer and consumer APIs. The system has 4 components: Producer, Consumer, Broker, and Controller. 

There is a cluster formed by multiple **Brokers** and a **Controller**, which together act as a coherent server that store durably records and stream records.

**Producers** and **Consumers** are clients that interact with this cluster:
- Producers publish **records** to a **topic**. 
- Consumers subscribe to one or more topics and pull records with low latency.


## Design

Presentation, report, and demo can be found here:


Due to time constraint, there are tons of simplifications from real Kafka implementation and weaker assumptions. 

Simplifications:
- No log compaction
- No consumer group
- No consumer offset tracking by the cluster.
- No automatic log deletion with some retention policy
- No dynamic addition/removal of topic.
- No dynamic addition of brokers. 

Assumptions:
- Failure stop model:  Once a broker fails, it never comes back up. 
- No network partitioning. 
- No message corruption while they are sent through network or persist on disk.
- Controller never fails.

Not implemented:
- Handling leader failure:  Requires Raft-style leader epoch and log reconciliation. 
- Consumer storing their offset durably on disk.


## Build
```
mkdir -p build
cd build
cmake ..
make
```

## Usage

### Cluster
1. Create a topic config JSON that contains an array of topic, each with (topic name, number of partitions, replication factor)
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

2. Create a controller that waits for 4 brokers. 
The controller will read this topic config to create and do partition assignment of topic-partitions.
The `load_balance_leaders_flag` being 1 allows a broker to hold replicas of topic-partitions both as follower and as leader. 
This requires handling leader failure which is not implemented, hence for failure tolerance evaluation, use 0, which assigns one broker to be the leader for all topic partitions, and assume it never fails. 
```
./bin/controller <my_id> <my_ip> <my_port> <num_brokers> <topic json path> <log file path> <load_balance_leaders_flag>

Example:
./bin/controller 0 127.0.0.1 8000 4 topic.json metadata.log 0
```

3. Create 4 brokers:
```
./bin/broker <my_id> <my_ip> <my_port> <controller_id> <controller_ip> <controller_port> <broker_log_folder> <debug_print_flag>

Example:
./bin/broker 1 127.0.0.1 9001 0 127.0.0.1 8000 ./broker1_logs/ 0
./bin/broker 2 127.0.0.1 9002 0 127.0.0.1 8000 ./broker2_logs/ 0
./bin/broker 3 127.0.0.1 9003 0 127.0.0.1 8000 ./broker2_logs/ 0
./bin/broker 4 127.0.0.1 9004 0 127.0.0.1 8000 ./broker2_logs/ 0
```

### Clients

To interact with the cluster:

Create an interactive producer that can send records to a topic.
The bootstrap broker can be any broker, it's only used once for fetching cluster metadata 
```
./bin/producer <my_id> <bootstrap_broker_ip> <bootstrap_broker_port> 

Example:
./bin/producer 100 127.0.0.1 9001
```

Create a consumer that pulls and prints records from a specific topic.
```
./bin/consumer <my_id> <bootstrap_broker_ip> <bootstrap_broker_port> <topic>

Example:
./bin/consumer 200 127.0.0.1 9001 chat
```

For evaluation, you can spawn multiple producers and multiple consumers with:

```
./bin/producer_eval <num_producer> <interval_ms> <topic> <bootstrap broker ip> <bootstrap broker port>
./bin/consumer_eval <num_consumer> <topic> <write_to_file(true) or print(false)> <file dir> <bootstrap broker ip> <bootstrap broker port>
```


