
#include "controller.hpp"      
using json = nlohmann::json;

Controller::Controller(int my_id, std::string my_ip, int my_port, int num_brokers,
    int hearbeat_timeout_s, int debug_print_period_s, const std::string& topic_cfg_file,
    const std::string& metadata_log_file, bool load_balance_leaders) :
    num_expected_brokers_(num_brokers), topic_cfg_file_(topic_cfg_file), metadata_log_(metadata_log_file),
    debug_print_period_s_(debug_print_period_s), hearbeat_timeout_s_(hearbeat_timeout_s) {
    id_ = my_id;
    ip_ = my_ip;
    port_ = my_port;
    load_balance_leaders_ = load_balance_leaders;

    // Initialize socket that listens connection from brokers.
    acceptor_socket_.Init(port_);

    createMetadataTopic();
    addSelfToClusterMetadata();

    // Spawn a background thread that parses the topic config file. 
    if (!fileExists(topic_cfg_file)) {
        throw std::runtime_error("Couldn't find topic configuration file.");
    }
    std::thread cfg_thread(&Controller::configMonitorThread, this);
    cfg_thread.detach();

    // Spawn a thread to monitor cluster metadata with print.
    std::thread debug_thread(&Controller::debugThread, this);
    debug_thread.detach();

    // Spawn a thread that manages the list of live brokers and do partition reassignment.
    std::thread watchdog_thread(&Controller::watchdogThread, this);
    watchdog_thread.detach();
}

bool Controller::fileExists(const std::string& path) {
    struct stat buffer;
    return (stat(path.c_str(), &buffer) == 0);
}

bool Controller::createTopic(const std::string& topic_name, int num_partitions, int replication_factor, bool load_balance_leaders) {
    // Ensure num_replicas_per_partition <= num_brokers. Having more doesn't help as some replicas will live in the same broker.
    int num_replicas_per_partition = std::min(replication_factor, num_expected_brokers_);
    int num_brokers = num_connected_brokers_;
    assert(num_connected_brokers_ == num_expected_brokers_);

    Topic topic;
    topic.id = topic_name;
    topic.partitions.reserve(num_partitions);

    // Assign brokers to each topic-partition replica. 
    if (load_balance_leaders) {
        // For better load balancing, these are the desired properties: 
        // - All brokers will have roughly equal numbers of partitions, with same proportion of leader and follower partitions replicas.
        // - Each partition replica must live in different broker. 
        // This is roughly achieved through a two-stage round robin algorithm.
        // 1. Assign leaders of each partition to brokers in round-robin fashion. Persist this as leader_assign_idx for future topic creation.
        // 2. Assign followers of each partition to brokers in round-robin fashion. This starts from leader_assign_idx, and skips over the broker which its leader is assigned to. 

        // Assigning leaders
        for (int i = 0; i < num_partitions; i++) {
            Partition p;
            p.id = i;

            p.broker_leader_id = brokers_state[leader_assignment_idx_].second;
            p.broker_followers_id.reserve(num_replicas_per_partition - 1);
            leader_assignment_idx_ = (leader_assignment_idx_ + 1) % num_brokers;

            topic.partitions.push_back(p);
        }

        // Assigning followers
        int rr_idx = leader_assignment_idx_;
        for (int i = 0; i < num_partitions; i++) {
            Partition& p = topic.partitions[i];
            int leader_id = p.broker_leader_id;

            int num_followers_to_assign = num_replicas_per_partition - 1;
            while (num_followers_to_assign > 0) {
                int chosen_id = brokers_state[rr_idx].second;

                if (chosen_id != leader_id) {
                    p.broker_followers_id.push_back(chosen_id);
                    num_followers_to_assign -= 1;
                }

                rr_idx = (rr_idx + 1) % num_brokers;
            }
        }
    } else {

        // Alternative load balancing strategy that assigns one broker (first) to be the leader for all tps.
        // Necessary if leader failure log reconciliation isn't implemented and assume leader never fails.

        // Assign all leaders to the first broker. 
        for (int i = 0; i < num_partitions; i++) {
            Partition p;
            p.id = i;
            p.broker_leader_id = brokers_state[0].second;
            p.broker_followers_id.reserve(num_replicas_per_partition - 1);
            topic.partitions.push_back(p);
        }
        // Assigning followers in round robin to all other brokers.
        int rr_idx = 1;
        for (int i = 0; i < num_partitions; i++) {
            Partition& p = topic.partitions[i];
            int leader_id = p.broker_leader_id;
            int num_followers_to_assign = num_replicas_per_partition - 1;
            while (num_followers_to_assign > 0) {
                int chosen_id = brokers_state[rr_idx].second;
                if (chosen_id != leader_id) {
                    p.broker_followers_id.push_back(chosen_id);
                    num_followers_to_assign -= 1;
                }
                rr_idx = (rr_idx + 1) % num_brokers;
            }
        }
    }

    {
        std::unique_lock lk(mtx_);

        // Add topic to in-memory metadata
        metadata_.topics[topic_name] = topic;

        // Add topic creation event to metadata log/topic.
        RecordBatch rb;
        for (int i = 0; i < num_partitions; i++) {
            Partition& p = topic.partitions[i];
            PartitionAssignmentRecord event_record(topic_name, p.id, p.broker_leader_id, p.broker_followers_id);
            rb.addRecord(event_record);
        }
        metadata_log_.append(rb);

        // Since metadata log/topic has no replication, set commit offset equal to written offset
        metadata_log_.setCommitOffset(metadata_log_.getLastWrittenOffset());
    }

    std::cout << "Created Topic " << topic_name << "with " << num_partitions << " partitions and " << num_replicas_per_partition << " per-partition replicas" << std::endl;
    return true;
}

bool Controller::createMetadataTopic() {
    std::string topic_name = "cluster_metadata";
    int num_partitions = 1;

    Partition p;
    p.id = 0;
    p.broker_leader_id = id_;

    Topic topic;
    topic.id = topic_name;
    topic.partitions.push_back(p);

    {
        std::unique_lock lk(mtx_);
        // Add topic to in-memory metadata
        metadata_.topics[topic_name] = topic;

        // Add topic creation event to metadata log/topic.
        RecordBatch rb;
        PartitionAssignmentRecord event_record(topic_name, p.id, p.broker_leader_id, p.broker_followers_id);
        rb.addRecord(event_record);
        metadata_log_.append(rb);

        // Since metadata log/topic has no replication, set commit offset equal to written offset
        metadata_log_.setCommitOffset(metadata_log_.getLastWrittenOffset());
    }

    std::cout << "Created cluster_metadata topic" << std::endl;
    return true;
}

Option<json> Controller::read_from_topic_cfg_file() {
    /**
     * Expected JSON array describing topics
     *
     * [
     *     {
     *         "name": "orders",
     *         "partitions": 3,
     *         "replication_factor": 2
     *     },
     *     {
     *         "name": "payments",
     *         "partitions": 5,
     *         "replication_factor": 3
     *     }
     * ]
     */
    Option<json> cfg_opt;
    std::ifstream f(topic_cfg_file_);
    if (f) {
        json cfg = json::parse(f);
        cfg_opt.setValue(cfg);
    }
    return cfg_opt;
}

void Controller::brokerHandlerThread(std::unique_ptr<ServerSocket> broker_socket) {
    ServerStub stub;
    stub.init(std::move(broker_socket), id_);

    // Wait for broker registration. 
    std::unique_ptr<Request> req = stub.receiveRequest();
    if (req == nullptr) return; // disconnected.
    RequestType req_type = req->getType();
    assert(req_type == RequestType::BROKER_REGISTRATION);
    auto* broker_reg = static_cast<BrokerRegistrationRequest*>(req.get());

    // Acknowledge registration.
    BrokerRegistrationResponse resp;
    resp.setResponderId(id_);
    resp.setStatus(StatusCode::SUCCESS);
    bool sent_success = stub.sendResponse(resp);

    // Add broker to the cluster metadata
    int broker_id;
    int broker_idx;
    auto last_req_time = std::chrono::system_clock::now();
    {
        std::unique_lock<std::mutex> lk(mtx_);
        // - live broker list
        broker_id = broker_reg->getRequesterId();
        broker_idx = brokers_state.size();
        brokers_state.push_back(std::make_pair(true, broker_id));
        removed_broker.push_back(false);
        brokers_last_req_time.push_back(last_req_time);

        // - in-memory metadata 
        metadata_.broker_conn_info[broker_id] = std::make_pair(broker_reg->ip, broker_reg->port);
    }

    // Add broker registration event to the metadata log/topic (log has its own mutex)
    RecordBatch rb;
    BrokerRegistrationRecord reg_event_record(broker_id, broker_reg->ip, broker_reg->port);
    rb.addRecord(reg_event_record);
    metadata_log_.append(rb);

    // Since metadata log/topic has no replication, set commit offset equal to written offset
    metadata_log_.setCommitOffset(metadata_log_.getLastWrittenOffset());

    num_connected_brokers_++;
    debugstream << "Broker " << broker_id << " registered. Handling its requests now." << std::endl;

    while (stub.isAlive()) {
        // Handle broker fetch requests for metadata
        std::unique_ptr<Request> req = stub.receiveRequest();
        if (req == nullptr) {
            break;
        }

        // Update its liveliness.
        last_req_time = std::chrono::system_clock::now();
        {
            std::unique_lock<std::mutex> lk(mtx_);
            brokers_last_req_time[broker_idx] = last_req_time;
        }

        RequestType req_type = req->getType();
        assert(req_type == RequestType::FETCH);
        auto* fetch_req = static_cast<FetchRequest*>(req.get());

        std::string& topic_name = fetch_req->topic;
        int partition = fetch_req->partition;
        int fetch_offset = fetch_req->fetch_offset;
        assert(topic_name == "cluster_metadata");
        assert(partition == 0);

        bool success = stub.sendFetchResponse(metadata_log_, fetch_offset, fetch_req->fetch_max_bytes, false);
        if (!success) {
            // std::cerr << "Failed to send fetch response " << std::endl;
            break;
        }

    }

}

void Controller::configMonitorThread() {
    // Handle topic-partition static assignment after all brokers register.
    // TODO: Currently its only done once after initialization and quits, so it doesn't handle dynamic topic addition or removal
    std::cout << "Waiting for all " << num_expected_brokers_ << " Brokers to register." << std::endl;
    while (num_connected_brokers_ != num_expected_brokers_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    std::cout << "Reading topic config file to create topics..." << std::endl;
    Option<json> cfg_opt = read_from_topic_cfg_file();

    if (!cfg_opt.hasValue()) {
        throw std::runtime_error("Failed to read topic config file.");
    }

    json cfg = cfg_opt.getValue();
    for (const auto& topic : cfg) {
        createTopic(topic["name"], topic["partitions"], topic["replication_factor"], load_balance_leaders_);
    }
}

void Controller::debugThread() {
    // Periodically prints to console the current cluster metadata.
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(debug_print_period_s_));
        {
            std::unique_lock<std::mutex> lk(mtx_);
            std::cout << metadata_ << std::endl;
        }
    }
}

void Controller::addSelfToClusterMetadata() {
    std::unique_lock<std::mutex> lk(mtx_);
    metadata_.broker_conn_info[id_] = std::make_pair(ip_, port_);

    // Add broker registration event to the metadata log/topic (log has its own mutex)
    RecordBatch rb;
    BrokerRegistrationRecord reg_event_record(id_, ip_, port_);
    rb.addRecord(reg_event_record);
    metadata_log_.append(rb);

    // Since metadata log/topic has no replication, set commit offset equal to written offset
    metadata_log_.setCommitOffset(metadata_log_.getLastWrittenOffset());
}

void Controller::watchdogThread() {
    while (true) {
        // Detects broker failure through timeout.
        std::vector<int> failed_brokers_id;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            auto current_time = std::chrono::system_clock::now();

            for (size_t i = 0; i < brokers_state.size(); i++) {
                if (removed_broker[i]) {
                    continue;
                }
                if ((current_time - brokers_last_req_time[i]) > std::chrono::seconds(hearbeat_timeout_s_)) {
                    brokers_state[i].first = false;
                    removed_broker[i] = true;
                    int failed_broker_id = brokers_state[i].second;
                    failed_brokers_id.push_back(failed_broker_id);
                }
            }
        }

        // Remove failed brokers from partition assignment 
        if (!failed_brokers_id.empty()) {
            std::cout << "Detected failure of brokers " << failed_brokers_id << ". Removing them from cluster" << std::endl;

            {
                std::unique_lock<std::mutex> lk(mtx_);

                // Read through cluster metadata and find tp that it holds.
                for (auto& kv : metadata_.topics) {
                    Topic& topic = kv.second;
                    for (int i = 0; i < topic.getNumPartitions(); i++) {
                        Partition& p = topic.partitions[i];
                        bool tp_changed = false;

                        for (auto& failed_broker_id : failed_brokers_id) {
                            if (failed_broker_id == p.broker_leader_id) {
                                // Remove it from leader and promote first follower as leader
                                if (!p.broker_followers_id.empty()) {
                                    p.broker_leader_id = p.broker_followers_id[0];
                                    p.broker_followers_id.erase(p.broker_followers_id.begin());
                                    tp_changed = true;
                                }
                                continue;
                            }

                            // Remove it from followers.
                            if (!p.broker_followers_id.empty()) {
                                auto it = std::find(p.broker_followers_id.begin(), p.broker_followers_id.end(), failed_broker_id);
                                if (it != p.broker_followers_id.end()) {
                                    p.broker_followers_id.erase(it);
                                    tp_changed = true;
                                }
                            }
                        }

                        // Emit this topic partition reassignment to metadata topic. 
                        if (tp_changed) {
                            RecordBatch rb;
                            PartitionAssignmentRecord event_record(topic.id, p.id, p.broker_leader_id, p.broker_followers_id);
                            rb.addRecord(event_record);
                            metadata_log_.append(rb);
                            metadata_log_.setCommitOffset(metadata_log_.getLastWrittenOffset());
                        }
                    }
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }  // end of while(true)

}






void Controller::listenForBrokerConnections() {
    // Indefinitely listens for broker connection and spawns dedicated thread for each.
    while (true) {
        std::unique_ptr<ServerSocket> broker_socket;
        broker_socket = acceptor_socket_.Accept();
        if (broker_socket == nullptr) continue;
        std::thread broker_thread(&Controller::brokerHandlerThread, this, std::move(broker_socket));
        broker_thread.detach();
    }
}

