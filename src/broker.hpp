

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

#include <vector>
#include <thread>
#include <string>
#include <algorithm>
#include <shared_mutex>
#include "consumer.hpp"
#include "cluster_metadata.hpp"
#include "ServerStub.hpp"
#include "records.hpp"
#include "network_messages.hpp"
#include "option.hpp"
#include "assert.h"

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
    std::shared_ptr<KafkaConsumer> leader_consumer;  // fetch from leader broker
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
    std::string tp_log_dir;

    ServerSocket acceptor_socket_;

    std::thread metadata_poll_thread_;
    std::thread debug_thread_;
    std::thread replication_poll_thread_;
    std::vector<std::thread> client_handler_threads_;
    std::atomic<bool> shutdown_signal_{ false };

    KafkaConsumer controller_consumer;   // Fetches ClusterMetadata changes from controller.

    ClusterMetaData metadata_;
    std::map<std::string, std::unique_ptr<AssignedTopicPartition>> assigned_tps;  // Maps topic-partition ("<topic>_<partition>" string) to its structure containing log and replication state.
    std::shared_mutex metadata_tps_mtx_; // read write lock for both metadata_ and assigned_tps since they need to be synchronized. 

    std::string getTopicPartitionName(const std::string& topic, int partition) {
        return topic + "_" + std::to_string(partition);
    }

    void onPartitionAssignment(PartitionAssignmentRecord& rec);


public:
    using Role = AssignedTopicPartition::Role;

    Broker(int my_id, std::string my_ip, uint16_t my_port,
        int controller_id, std::string controller_ip, uint16_t controller_port,
        std::string log_folder_path, bool debug);

    ~Broker() {
        shutdown_signal_ = true;
        if (debug_thread_.joinable()) {
            debug_thread_.join();
        }

        if (replication_poll_thread_.joinable()) {
            replication_poll_thread_.join();
        }
        if (metadata_poll_thread_.joinable()) {
            metadata_poll_thread_.join();
        }

        for (auto& thread : client_handler_threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }


    }

    void pollMetadataThread();
    void clientHandlerThread(std::unique_ptr<ServerSocket> socket);
    void replicationThread();
    void debugThread();

    /**
     * @brief Listens indefinitely for client connection and spawns dedicated
     * thread for each.
     *
     */
    void listenForClientConnections();

    void shutdown() {
        shutdown_signal_ = true;
    }
};



#endif