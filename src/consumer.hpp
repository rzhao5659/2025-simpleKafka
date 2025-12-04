
#ifndef __CONSUMER_HPP__
#define __CONSUMER_HPP__

#include "records.hpp"
#include "network_messages.hpp"
#include <queue>
#include <string>
#include <map> 
#include "ClientStub.hpp"
#include "cluster_metadata.hpp"
#include "thread_safe_queue.hpp"
#include <unordered_set>
#include <atomic>
#include <thread>
#include <memory>
#include <random>

class ConsumerRecordBatch {
public:
    std::string topic;
    int partition;
    int commit_offset; // the responder's commit offset.
    RecordBatch record_batch;

    void print(std::ostream& os = std::cout) const {
        os << "ConsumerRecordBatch["
            << "topic=" << topic
            << ", partition=" << partition
            << ", commit_offset=" << commit_offset << ", ";
        record_batch.print(os);
    }
};

class TopicPartition {
public:
    int fetch_broker_id = -1;
    std::string topic;
    int partition = -1;
    std::unique_ptr<ThreadSafeQueue<ConsumerRecordBatch>> fetched_rbs_queue;
    int fetch_offset = -1;

    TopicPartition()
        : fetched_rbs_queue(new ThreadSafeQueue<ConsumerRecordBatch>()) {
    }


    // Configuration parameters 
    int fetch_trigger_size; // When num records in queue is smaller than this, fetch.
    int fetch_max_bytes; // How much to fetch.
    bool fetch_from_leader_only = false;

    bool needFetch() {
        return fetched_rbs_queue->size() <= fetch_trigger_size;
    }

    void print(std::ostream& os = std::cout) const {
        os << "TopicPartition["
            << "topic=" << topic
            << ", partition=" << partition
            << ", fetch_broker_id=" << fetch_broker_id
            << ", fetch_offset=" << fetch_offset
            << ", queue_size=" << fetched_rbs_queue->size()
            << ", fetch_trigger_size=" << fetch_trigger_size
            << ", fetch_max_bytes=" << fetch_max_bytes
            << ", leader_only=" << (fetch_from_leader_only ? "true" : "false")
            << "]" << std::endl;
    }

};

class KafkaConsumer {
private:
    int id_;
    std::mt19937 rng_{ std::random_device{}() };


    // Consumer configuration parameters
    int fetch_trigger_size_;
    int fetch_max_bytes_;

    std::thread fetch_thread_;
    std::atomic<bool> shutdown_signal_;

    // Shared variables.
    int poll_tp_idx_ = 0;
    std::vector<TopicPartition> subscribed_tps;
    std::map<int, std::unique_ptr<ClientStub>> broker_sockets_; // maps broker id to their sockets. lazy connection.
    std::unordered_set<int> failed_brokers_;
    ClusterMetaData metadata_;
    std::mutex mtx_;

    void recordsFetchThread();

    void removeFailedBrokers(std::vector<int>& brokers_id);
    int chooseFetchBroker(const std::string& topic_name, int partition); // return -1 if no valid broker.

public:
    KafkaConsumer(int id, int fetch_trigger_size, int fetch_max_bytes) : id_(id),
        fetch_trigger_size_(fetch_trigger_size),
        fetch_max_bytes_(fetch_max_bytes),
        shutdown_signal_(false) {

    }

    ~KafkaConsumer() {
        shutdown_signal_ = true;
        if (fetch_thread_.joinable()) {
            fetch_thread_.join();
        }
    }

    std::vector<ConsumerRecordBatch> poll(int num_rbs, int timeout_ms = 1000);
    bool subscribe(const std::string& topic, bool read_from_start, bool leader_only);
    bool subscribe(const std::string& topic, int partition, bool read_from_start, bool leader_only);

    // Metadata management: either fetched through broker or updated manually.
    void fetchClusterMetadata(const std::string& broker_ip, int broker_port);
    void setClusterMetadata(ClusterMetaData& metadata);

    // void startBackgroundFetchThread();

    // TOOD: this is pretty specific, but whatever...
    // Special function when broker instantiate a Consumer to fetch from controller.
    // It needs to send an initial broker registration request.
    bool _sendBrokerRegistration(std::string broker_ip, uint16_t broker_port, int controller_id);
};

#endif