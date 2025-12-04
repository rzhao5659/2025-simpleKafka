#ifndef CLUSTER_METADATA_HPP
#define CLUSTER_METADATA_HPP

#include <vector>
#include <map>
#include <string>

#include <cstring>
#include <iostream>
#include <arpa/inet.h>
#include <ostream>
#include <algorithm>

class Partition {
public:
    /* Binary Format:
    * +------------------+------------------+------------------------+
    * | Field            | Type             | Size (bytes)           |
    * +------------------+------------------+------------------------+
    * | id               | int32            | 4                      |
    * | broker_leader_id | int32            | 4                      |
    * | num_followers    | int32            | 4                      |
    * | follower_ids     | int32[]          | 4 * num_followers      |
    * +------------------+------------------+------------------------+
    */
    int id;
    int broker_leader_id;
    std::vector<int> broker_followers_id;

    void unmarshal(const char* buffer);
    void marshal(char* buffer) const;
    int getLength() const;
};

class Topic {
public:
    /* Binary Format:
    * +------------------+------------------+------------------------+
    * | Field            | Type             | Size (bytes)           |
    * +------------------+------------------+------------------------+
    * | id               | byte[]           | 32                     |
    * | num_partitions   | int32            | 4                      |
    * | partitions       | Partition[]      | variable               |
    * +------------------+------------------+------------------------+
    */
    std::string id;
    std::vector<Partition> partitions;

    void unmarshal(const char* buffer);
    void marshal(char* buffer) const;
    int getLength() const;
    int getNumPartitions() const { return partitions.size(); }
};

class ClusterMetaData {
public:
    /* Binary Format:
    * +------------------+------------------+------------------------+
    * | Field            | Type             | Size (bytes)           |
    * +------------------+------------------+------------------------+
    * | num_topics       | int32            | 4                      |
    * | topics           | Topic[]          | variable               |
    * | num_brokers      | int32            | 4                      |
    * | broker_id        | int32            | 4                      |
    * | broker_ip        | byte[]           | 4                      |
    * | broker_port      | int16            | 2                      |
    * | (repeated for each broker)                                   |
    * +------------------+------------------+------------------------+
    */
    std::map<std::string, Topic> topics;
    std::map<int, std::pair<std::string, uint16_t>> broker_conn_info; // ip and port 

    void unmarshal(const char* buffer);
    void marshal(char* buffer) const;

    // Auxiliary methods.
    int getLength() const;
    int getNumTopics() const { return topics.size(); }

    friend std::ostream& operator<<(std::ostream& os, const ClusterMetaData& obj);
    bool containsTopicPartition(const std::string& topic, int partition) {
        return topics.count(topic) > 0 && partition < topics[topic].partitions.size();
    }

    int getBrokerLeader(const std::string& topic, int partition) {
        if (!containsTopicPartition(topic, partition)) {
            return -1;
        }
        return topics[topic].partitions[partition].broker_leader_id;
    }

    std::vector<int> getBrokerFollowers(const std::string& topic, int partition) {
        std::vector<int> followers;
        if (!containsTopicPartition(topic, partition)) {
            return followers;
        }
        return topics[topic].partitions[partition].broker_followers_id;
    }

    bool isBrokerALeader(int broker_id, const std::string& topic, int partition) {
        return broker_id == getBrokerLeader(topic, partition);
    }

    bool isBrokerAFollower(int broker_id, const std::string& topic, int partition) {
        std::vector<int> followers = getBrokerFollowers(topic, partition);
        auto it = std::find(followers.begin(), followers.end(), broker_id);
        if (it != followers.end()) {
            return true;
        }
        return false;
    }


};

#endif