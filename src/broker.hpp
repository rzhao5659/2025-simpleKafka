


// Broker:
// - 


// For simplicity, creates dedicated thread per client (consumer/producer)
// Each thread just wait and respond back.  Identify by type fetch, cluster, or produce. 
// Keeps a dedicated connection to controller. 

// For simplicity, absolutely no async, all request/response are sync. if fetch, send large bytes as specified by max if avail, if not, waits, based on fetch request metadata. Each thread do this waiting so its completely fine and still relatively sync. (notice no async config like max wait or min bytes)


// Initialize controller in the clustermetadata as part of init of broker.
// Create a vector of tps (as log) that it holds replica for. e.g., [follower, follower, leader]
// Create a consumer from controller and parse its requests to build up clustermetadata. helps so all records from same metadata topic.
// - Create a vector of consumers (easier to individiually destroy and add) to fetch from leaders brokers.  
//  In order for these to only fetch from leaders, 
//   1. config consumer to only consume from leader, 
//   2. might need consumer to not have background thread(have it std thread, and have a desutrctor need to join with atomic bool shutdown signal). 
//   3. allow raw record batch fetch, so the brokers that fetched as consumer can just store them in log. 
//   4. You know what... the consumer offset should only update by batches (since followers fetch from leader sby batches) 
//      meaning it should always point to the start of next rb batch.... commit offset also should do that. 
//   5. allow subscribe fetch from a specific partition.... (allow only leader)
// Need to parse producereq, fetchreq, clustermetadatareq.


// Replication protocol:
// Invariants/Assumptions:
// - A broker assigned to a tp, can only change F -> L, instead of L -> F. (i.e., if leader, permanently stays leader). 
//   When promoted to leader, reset followers_fetch_offset (empty them or just set them to 0).
// - The broker must distinguish whether a fetch is from broker follower or consumer. because for follower, it must allow fetching beyond commit offset, since im leader and have all the GT data.
//    However, when im being fetched by consumer, i only allow fetching up to commit offset. 
#ifndef BROKER_HPP
#define BROKER_HPP

#include "consumer.hpp"
#include <vector>
#include <thread>
#include <string>
#include <ifaddrs.h>       
#include <sys/types.h>      
#include <sys/socket.h>    
#include <arpa/inet.h>     
#include <netinet/in.h>    
#include <cstdio>           
#include "cluster_metadata.hpp"
#include "ServerStub.hpp"
#include "records.hpp"
#include "network_messages.hpp"
#include "option.hpp"
#include <mutex>
#include "assert.h"
#include <algorithm>

class AssignedTopicPartition {
public:
    enum class Role {
        NONE = 0,
        LEADER = 1,
        FOLLOWER = 2
    };

    std::string topic;
    int partition;
    Role role;
    Log log;

    // Used if follower
    std::shared_ptr<KafkaConsumer> consumer;  // fetch from leader broker
    int broker_leader_id;

    // Used if leader
    std::map<int, int> follower_fetch_offsets; // maps broker_follower_id -> last fetch offset. 

    AssignedTopicPartition(const std::string& log_path, const std::string& topic_name, int partition_id)
        : role(Role::NONE), broker_leader_id(-1), log(log_path), topic(topic_name), partition(partition_id) {
    }
};

std::string roleToString(AssignedTopicPartition::Role role);

class Broker {
private:
    int id_;
    std::string ip_;
    uint16_t port_;

    ClusterMetaData metadata_;
    std::mutex mtx_;
    ServerSocket acceptor_socket_;

    // Acting as consumer: 
    // - The design have multiple consumers, instead of one single subscribed to multiple tp, because having them separated them allows easier dynamic 
    //  removal and addition of consumers, since topic removal is not yet supported.
    KafkaConsumer controller_fetcher;   // Fetches ClusterMetadata changes from controller.

    std::thread metadata_poll_thread_;
    std::thread debug_thread_;
    std::thread replication_poll_thread_;

    // Server-related variables.
    std::string tp_log_dir;
    std::map<std::string, std::unique_ptr<AssignedTopicPartition>> assigned_tps;  // Maps topic-partition ("<topic>_<partition>" string) to its structure containing log and replication state.

    std::string getTopicPartitionName(const std::string& topic, int partition) {
        return topic + "_" + std::to_string(partition);
    }

    void onPartitionAssignment(PartitionAssignmentRecord& rec);

    std::atomic<bool> shutdown_signal_{ false };

public:
    using Role = AssignedTopicPartition::Role;

    Broker(int my_id, std::string my_ip, uint16_t my_port,
        int controller_id, std::string controller_ip, uint16_t controller_port,
        std::string log_folder_path, bool debug);

    ~Broker() {
        shutdown_signal_ = true;
        if (metadata_poll_thread_.joinable()) {
            metadata_poll_thread_.join();
        }
        if (replication_poll_thread_.joinable()) {
            replication_poll_thread_.join();
        }
        if (debug_thread_.joinable()) {
            debug_thread_.join();
        }
    }
    void pollMetadataThread();
    void clientHandlerThread(std::unique_ptr<ServerSocket> socket);
    void listenForClientConnections();
    void replicationThread();
    void debugThread();


    void shutdown() {
        shutdown_signal_ = true;
    }
};



#endif