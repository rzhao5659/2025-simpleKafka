#ifndef __CONTROLLER_HPP__
#define __CONTROLLER_HPP__
#include <ifaddrs.h>       
#include <sys/types.h>      
#include <sys/socket.h>    
#include <arpa/inet.h>     
#include <netinet/in.h>    
#include <string>
#include <vector>
#include <thread>
#include "ServerSocket.hpp"
#include <sys/stat.h>
#include <stdexcept>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <iostream>
#include "cluster_metadata.hpp"
#include <mutex>
#include <nlohmann_json.hpp>
#include <fstream>
#include "option.hpp"
#include <assert.h>
#include "network_messages.hpp"
#include "records.hpp"
#include "ServerStub.hpp"
#include "debug_stream.hpp"
#include <atomic>
#include <ctime>

// Dedicated "master" that manages and stores cluster metadata. 
// In Kafka, this is typically a Raft-quorum of brokers with an additional controller role,
// Here, this is implemented as a single dedicated controller node (no replication), assumed to never fail.

// The controller is the only one that changes cluster metadata (e.g., broker failure, partition state change)
// The cluster metadata is treated as a topic with single partition (single append-only log of changes)
// The controller changes cluster metadata by changing its log. 
// Other brokers must initially register itself to the controller and periodically fetch metadata topic.
// Hence, controller acts purely as a server. 
using json = nlohmann::json;

class Controller {
private:
    int id_;
    int port_;
    std::string ip_;
    ServerSocket acceptor_socket_;
    int leader_assignment_idx_ = 0;

    // Controller's configuration parameters 
    int debug_print_period_s_;
    int num_expected_brokers_;
    std::string topic_cfg_file_;
    int hearbeat_timeout_s_;
    bool load_balance_leaders_;

    // Shared variables that tracks live brokers and cluster metadata.
    std::atomic<int> num_connected_brokers_{ 0 };
    std::vector<std::pair<bool, int>> brokers_state; // fixed size vector of (alive, broker_id)
    std::vector<bool> removed_broker; // fixed size vector that marks one time broker removed. Seems redundant but allows behavior to occur when first failed.  
    std::vector<std::chrono::system_clock::time_point> brokers_last_req_time; // fixed size vector.
    ClusterMetaData metadata_;
    std::mutex mtx_;

    Log metadata_log_;

    bool fileExists(const std::string& path);
    bool createTopic(const std::string& topic_name, int num_partitions, int replication_factor, bool load_balance_leaders = false);
    bool createMetadataTopic();
    Option<json> read_from_topic_cfg_file();
    void addSelfToClusterMetadata();

    void brokerHandlerThread(std::unique_ptr<ServerSocket> broker_socket);
    void configMonitorThread();
    void debugThread();
    void watchdogThread();

public:
    Controller(int my_id, std::string my_ip, int my_port, int num_brokers, int hearbeat_timeout_s, int debug_print_period_s, const std::string& topic_cfg_file, const std::string& metadata_log_file, bool load_balance_leaders);
    void listenForBrokerConnections();
};

#endif