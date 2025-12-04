#include "consumer.hpp"
#include <thread>
#include <chrono>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <mutex>
#include <assert.h>
#include "debug_stream.hpp"
#include <random>

#define FIRST_RECORD_OFFSET 0
#define LAST_RECORD_OFFSET -1

void KafkaConsumer::setClusterMetadata(ClusterMetaData& metadata) {
    // Update in-memory cluster metadata 
    std::unique_lock<std::mutex> lk(mtx_);
    metadata_ = metadata;

    // Initialize stubs for all brokers in the cluster metadata
    for (const auto& broker_info_pair : metadata_.broker_conn_info) {
        int broker_id = broker_info_pair.first;

        // broker/controller may use an instance of consumer, avoid initializing clientstub for self.
        // Note that it's not connected yet, that's done lazily when required.
        if (broker_id != id_) {
            if (broker_sockets_.find(broker_id) == broker_sockets_.end()) {
                std::unique_ptr<ClientStub> stub(new ClientStub(id_));
                broker_sockets_[broker_id] = std::move(stub);
            }
        }
    }
}

void KafkaConsumer::fetchClusterMetadata(const std::string& broker_ip, int broker_port) {
    // Initialize connection to the broker
    ClientStub broker_stub(id_);
    if (!broker_stub.connect(broker_ip, broker_port)) {
        throw std::runtime_error("Unable to connect to the bootstrap broker");
    }

    // Fetch cluster metadata
    Option<ClusterMetaDataResponse> resp_opt = broker_stub.sendClusterMetadataRequest();
    if (!resp_opt.hasValue()) {
        throw std::runtime_error("Failed to fetch cluster metadata from broker");
    }
    ClusterMetaDataResponse resp = resp_opt.getValue();

    // Update in-memory cluster metadata and initialize broker stubs
    setClusterMetadata(resp.cluster_metadata);
}


int KafkaConsumer::chooseFetchBroker(const std::string& topic_name, int partition) {
    // Assumes locked mutex already.
    // Find the list of brokers that holds replicas for this topic-partition
    Partition& p = metadata_.topics[topic_name].partitions[partition];
    std::vector<int> replicas = p.broker_followers_id;
    replicas.push_back(p.broker_leader_id);

    // Choose randomly among healthy broker that holds a replica of the failed tp.
    removeFailedBrokers(replicas);
    if (replicas.empty()) {
        return -1;
    }

    std::uniform_int_distribution<int> dist(0, replicas.size() - 1);
    return replicas[dist(rng_)];
}


// TODO: This is pretty badly written, but as long as you setMetadata with controller as one of the broker. this works.
bool KafkaConsumer::_sendBrokerRegistration(std::string ip, uint16_t port, int controller_id) {
    // Initialize connection if necessary.
    if (broker_sockets_.find(controller_id) == broker_sockets_.end()) {
        std::unique_ptr<ClientStub> stub(new ClientStub(id_));
        broker_sockets_[controller_id] = std::move(stub);
    }
    std::unique_ptr<ClientStub>& controller_stub = broker_sockets_[controller_id];

    if (!controller_stub->isConnected()) {
        std::string controller_ip = metadata_.broker_conn_info[controller_id].first;
        int controller_port = metadata_.broker_conn_info[controller_id].second;
        if (!controller_stub->connect(controller_ip, controller_port)) {
            return false;
        }
    }

    // Send registration request
    Option<BrokerRegistrationResponse> resp = controller_stub->sendBrokerRegistrationRequest(id_, ip, port);
    if (!resp.hasValue()) {
        return false;
    }
    return true;
}



// Subscribe to all partitions of a topic
bool KafkaConsumer::subscribe(const std::string& topic_name, bool read_from_start, bool leader_only) {
    std::unique_lock<std::mutex> lk(mtx_);

    // Check if the topic exists from cluster metadata.
    auto it = metadata_.topics.find(topic_name);
    if (it == metadata_.topics.end()) {
        std::cerr << "Topic " << topic_name << " doesn't exist." << std::endl;
        return false;
    }

    // Get its info from cluster metadata: num_partitions and location (broker) of each replica.
    Topic& topic = metadata_.topics[topic_name];

    // Create TopicPartition for each partition
    std::cout << "Subscribed to topic " << topic.id << " with " << topic.getNumPartitions() << " partitions" << std::endl;

    for (int i = 0; i < topic.getNumPartitions(); i++) {
        Partition p = topic.partitions[i];

        subscribed_tps.emplace_back();
        TopicPartition& tp = subscribed_tps.back();
        tp.topic = topic.id;
        tp.partition = p.id;
        tp.fetch_trigger_size = fetch_trigger_size_;
        tp.fetch_max_bytes = fetch_max_bytes_;
        tp.fetch_from_leader_only = leader_only;

        // Choose replica (broker) to fetch from.
        int fetch_broker_id = -1;
        if (leader_only) {
            fetch_broker_id = p.broker_leader_id;
        } else {
            // Choose a random healthy replica (broker) to fetch from for load balancing.
            fetch_broker_id = chooseFetchBroker(tp.topic, tp.partition);
            if (fetch_broker_id == -1) {
                std::cerr << "All brokers holding replicas of topic-partition(" << topic.id << "," << p.id << ")" << " have failed. Skipping..." << std::endl;
                continue;
            }
        }
        tp.fetch_broker_id = fetch_broker_id;
        std::cout << "\t-Fetching partition(" << p.id << ") from Broker " << tp.fetch_broker_id << std::endl;

        // Initialize consumer offset (`fetch_offset`). 
        // Use locally stored if found (TODO). Otherwise starts from first or latest record of tp. 
        if (read_from_start) {
            tp.fetch_offset = FIRST_RECORD_OFFSET;
        } else {
            tp.fetch_offset = LAST_RECORD_OFFSET;
        }
    }



    return true;
}


bool KafkaConsumer::subscribe(const std::string& topic_name, int partition, bool read_from_start, bool leader_only) {
    std::unique_lock<std::mutex> lk(mtx_);

    // Check if the topic exists from cluster metadata.
    auto it = metadata_.topics.find(topic_name);
    if (it == metadata_.topics.end()) {
        std::cerr << "Topic " << topic_name << " doesn't exist." << std::endl;
        return false;
    }


    // Get its info from cluster metadata: num_partitions and location (broker) of each replica.
    Topic& topic = metadata_.topics[topic_name];
    Partition p = topic.partitions[partition];

    // Create TopicPartition
    subscribed_tps.emplace_back();
    TopicPartition& tp = subscribed_tps.back();
    tp.topic = topic.id;
    tp.partition = p.id;
    tp.fetch_trigger_size = fetch_trigger_size_;
    tp.fetch_max_bytes = fetch_max_bytes_;
    tp.fetch_from_leader_only = leader_only;

    // Choose replica (broker) to fetch from.
    int fetch_broker_id = -1;
    if (leader_only) {
        fetch_broker_id = p.broker_leader_id;
    } else {
        // Choose a random healthy replica (broker) to fetch from for load balancing.
        fetch_broker_id = chooseFetchBroker(tp.topic, tp.partition);
    }
    tp.fetch_broker_id = fetch_broker_id;

    if (fetch_broker_id == -1) {
        std::cerr << "All brokers holding replicas of topic-partition(" << topic.id << "," << p.id << ")" << " have failed. Skipping..." << std::endl;
        return false;
    }

    // Initialize consumer offset (`fetch_offset`). 
    // Use locally stored if found (TODO). Otherwise starts from first or latest record of tp. 
    if (read_from_start) {
        tp.fetch_offset = FIRST_RECORD_OFFSET;
    } else {
        tp.fetch_offset = LAST_RECORD_OFFSET;
    }

    std::cout << "Subscribed to topic-partition(" << topic.id << "," << p.id << ") from Broker " << tp.fetch_broker_id << std::endl;
    return true;
}



void KafkaConsumer::removeFailedBrokers(std::vector<int>& brokers_id) {
    // Remove brokers ids that appear in failed brokers.
    brokers_id.erase(
        std::remove_if(brokers_id.begin(), brokers_id.end(),
            [&](int x) {
                return failed_brokers_.find(x) != failed_brokers_.end();  // exists in set
            }),
        brokers_id.end()
    );
}

// Pull one record batch from each tp's local buffer of fetched records in round-robin fashion.
// If not enough record batches, it will block until the background fetch thread retrieves enough.
// Change: this no longer blocks. it will return empty record batch if necessary
std::vector<ConsumerRecordBatch> KafkaConsumer::poll(int num_rbs, int timeout_ms) {
    std::vector<ConsumerRecordBatch> result;

    // Start fetch thread
    if (!shutdown_signal_ && !fetch_thread_.joinable()) {
        std::thread fetch_thread(&KafkaConsumer::recordsFetchThread, this);
        fetch_thread_ = std::move(fetch_thread);
    }

    int num_tps = 0;
    {
        std::unique_lock<std::mutex> lk(mtx_);
        num_tps = subscribed_tps.size();
    }

    if (num_tps == 0) {
        return result;
    }

    result.reserve(num_rbs);

    auto start_time = std::chrono::steady_clock::now();

    int fetched = 0;
    while (fetched < num_rbs) {
        // Check if timeout expired
        auto elapsed = std::chrono::steady_clock::now() - start_time;
        if (elapsed >= std::chrono::milliseconds(timeout_ms)) {
            break;
        }

        // Try to pop from current topic partition (round robin fashion)
        TopicPartition& tp = subscribed_tps[poll_tp_idx_];
        ConsumerRecordBatch popped_rb;

        bool success = tp.fetched_rbs_queue->try_pop(0, popped_rb);
        if (success) {
            result.push_back(popped_rb);
            fetched++;
        }

        poll_tp_idx_ = (poll_tp_idx_ + 1) % num_tps;
    }
    return result;
}

// Background thread that fetches records from brokers
void KafkaConsumer::recordsFetchThread() {

    try {
        while (!shutdown_signal_) {
            // Get a list of topic-partitions that needs fetching
            std::vector<int> fetch_tp_idxs;
            {
                std::unique_lock<std::mutex> lk(mtx_);
                for (int i = 0; i < subscribed_tps.size(); i++) {
                    if (subscribed_tps[i].needFetch()) {
                        fetch_tp_idxs.push_back(i);
                    }
                }
            }

            // Sleep if no fetching needed.
            if (fetch_tp_idxs.empty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }

            // Create FetchRequest
            std::vector<FetchRequest> fetch_reqs;
            fetch_reqs.reserve(fetch_tp_idxs.size());
            for (int i = 0; i < fetch_tp_idxs.size(); i++) {
                int idx = fetch_tp_idxs[i];
                {
                    std::unique_lock<std::mutex> lk(mtx_);
                    TopicPartition& tp = subscribed_tps[idx];

                    FetchRequest fetch_req;
                    fetch_req.setRequesterId(id_);
                    fetch_req.topic = tp.topic;
                    fetch_req.partition = tp.partition;
                    fetch_req.fetch_offset = tp.fetch_offset;
                    fetch_req.fetch_max_bytes = tp.fetch_max_bytes;

                    fetch_reqs.push_back(fetch_req);
                }

            }

            // Send FetchRequest
            // Mark any failed request.
            std::vector<bool> fetch_reqs_failed(fetch_reqs.size(), false);
            for (int i = 0; i < fetch_reqs.size(); i++) {
                int idx = fetch_tp_idxs[i];
                FetchRequest& req = fetch_reqs[i];

                {
                    std::unique_lock<std::mutex> lk(mtx_);
                    TopicPartition& tp = subscribed_tps[idx];

                    // Find the corresponding broker
                    int fetch_broker_id = tp.fetch_broker_id;
                    if (broker_sockets_.find(fetch_broker_id) == broker_sockets_.end()) {
                        std::unique_ptr<ClientStub> stub(new ClientStub(id_));
                        broker_sockets_[fetch_broker_id] = std::move(stub);
                    }
                    std::unique_ptr<ClientStub>& fetch_broker_stub = broker_sockets_[fetch_broker_id];

                    // Initialize connection if necessary.
                    if (!fetch_broker_stub->isConnected()) {
                        std::string broker_ip = metadata_.broker_conn_info[fetch_broker_id].first;
                        int broker_port = metadata_.broker_conn_info[fetch_broker_id].second;
                        if (!fetch_broker_stub->connect(broker_ip, broker_port)) {
                            fetch_reqs_failed[i] = true;
                            continue;
                        }
                    }

                    // Send request
                    bool success = fetch_broker_stub->sendAsyncFetchRequest(req);
                    if (!success) {
                        fetch_reqs_failed[i] = true;
                        continue;
                    }
                }

            }


            // Receive response
            // Mark any failed response.
            for (int i = 0; i < fetch_reqs.size(); i++) {
                int idx = fetch_tp_idxs[i];
                std::string topic_name = subscribed_tps[idx].topic;
                int partition = subscribed_tps[idx].partition;

                if (fetch_reqs_failed[i]) {
                    continue;
                }

                Option<FetchResponse> resp_opt;
                {
                    std::unique_lock<std::mutex> lk(mtx_);
                    TopicPartition& tp = subscribed_tps[idx];

                    // Find the corresponding broker
                    int fetch_broker_id = tp.fetch_broker_id;
                    if (broker_sockets_.find(fetch_broker_id) == broker_sockets_.end()) {
                        std::unique_ptr<ClientStub> stub(new ClientStub(id_));
                        broker_sockets_[fetch_broker_id] = std::move(stub);
                    }
                    std::unique_ptr<ClientStub>& fetch_broker_stub = broker_sockets_[fetch_broker_id];

                    // Receive response
                    resp_opt = fetch_broker_stub->receiveAsyncFetchResponse();
                }

                if (!resp_opt.hasValue()) {
                    fetch_reqs_failed[i] = true;
                    continue;
                }

                FetchResponse resp = resp_opt.getValue();
                if (resp.getStatus() != StatusCode::SUCCESS) {
                    fetch_reqs_failed[i] = true;
                    continue;
                }

                // Push record batches to the local polled buffer of this tp.
                ConsumerRecordBatch consumer_rb;
                consumer_rb.topic = topic_name;
                consumer_rb.partition = partition;
                consumer_rb.commit_offset = resp.commit_offset;

                int num_rbs = resp.record_batches.size();
                if (num_rbs == 0) {
                    // If no record batches, still add a ConsumerRecordBatch to provide follower information about leader's fetch offset.
                    {
                        std::unique_lock<std::mutex> lk(mtx_);
                        TopicPartition& tp = subscribed_tps[idx];
                        tp.fetched_rbs_queue->push(consumer_rb);
                    }
                } else {
                    for (int j = 0; j < num_rbs; j++) {
                        RecordBatch& rb = resp.record_batches[j];
                        consumer_rb.record_batch = rb;
                        {
                            std::unique_lock<std::mutex> lk(mtx_);
                            TopicPartition& tp = subscribed_tps[idx];
                            tp.fetched_rbs_queue->push(consumer_rb);
                            tp.fetch_offset = rb.base_offset + rb.getNumRecords(); // update consumer fetch offset.
                        }
                    }
                }

            }

            // Deal with fetch failure of tps: 
            for (int i = 0; i < fetch_reqs_failed.size(); i++) {
                if (fetch_reqs_failed[i]) {
                    int idx = fetch_tp_idxs[i];
                    int fetch_broker_id = -1;
                    std::unique_lock<std::mutex> lk(mtx_);
                    TopicPartition& tp = subscribed_tps[idx];

                    // Mark brokers responsible for these failed tps retrival as failed, 
                    failed_brokers_.insert(tp.fetch_broker_id);

                    // Choose other replica (broker) if allowed 
                    if (tp.fetch_from_leader_only) {
                        continue;
                    } else {
                        // Choose another healthy broker (randomly) that holds a replica of the failed tp.
                        fetch_broker_id = chooseFetchBroker(tp.topic, tp.partition);
                        if (fetch_broker_id == -1) {
                            std::cerr << "All brokers holding replicas of topic-partition(" << tp.topic << "," << tp.partition << ")" << " have failed. Skipping..." << std::endl;
                            continue;
                        }

                        std::cout << "Broker " << tp.fetch_broker_id << " that is being fetched for topic-partition(" << tp.topic << "," << tp.partition << ")" << " have failed. " << std::endl;
                        std::cout << "Fetching from other broker " << fetch_broker_id << " that holds replica for this topic-partition" << std::endl;
                        tp.fetch_broker_id = fetch_broker_id;
                    }
                }
            }


            // Sleep before going to next fetch iteration
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

        }
    }
    catch (const std::exception& e) {
        std::cerr << "consumer recordfetchthread crashed: " << e.what() << std::endl;
    }
}






