
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

    friend std::ostream& operator<<(std::ostream& os, const ConsumerRecordBatch& batch) {
        os << "ConsumerRecordBatch["
            << "topic=" << batch.topic
            << ", partition=" << batch.partition
            << ", commit_offset=" << batch.commit_offset
            << ", record_batch=" << batch.record_batch
            << "]";
        return os;
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
    int fetch_trigger_size; // When num record batches in queue is smaller than this, fetch.
    int fetch_max_bytes; // How much to fetch.
    bool fetch_from_leader_only = false;

    bool needFetch() {
        return fetched_rbs_queue->size() <= fetch_trigger_size;
    }

    friend std::ostream& operator<<(std::ostream& os, const TopicPartition& tp) {
        os << "TopicPartition["
            << "topic=" << tp.topic
            << ", partition=" << tp.partition
            << ", fetch_broker_id=" << tp.fetch_broker_id
            << ", fetch_offset=" << tp.fetch_offset
            << ", queue_size=" << tp.fetched_rbs_queue->size()
            << ", fetch_trigger_size=" << tp.fetch_trigger_size
            << ", fetch_max_bytes=" << tp.fetch_max_bytes
            << ", leader_only=" << (tp.fetch_from_leader_only ? "true" : "false")
            << "]";
        return os;
    }

};



/**
 * @brief Thread-safe Kafka consumer that spawns a background fetch thread.
 * Usage pattern (must be called in order from a single thread):
 * 1. fetch/set metadata
 * 2. subscribe(topic_partitions)
 * 3. poll() repeatedly to consume records.
 *
 * By assuming this pattern, the number of subscribed topics stays constant once polling starts.
 * For concurrency, the only shared variable is the cluster metadata, which is protected by mutex.
 * - Attributes (aside queue) of TopicPartition can only be written by fetch thread.
 * - failed_brokers_ is only accessed by fetch thread.
 * - Each tp's queue is accessed by polling and fetch thread, but the queue is already thread-safe
 */
class KafkaConsumer {
private:
    int id_;
    std::mt19937 rng_{ std::random_device{}() };

    // Consumer configuration parameters
    int fetch_trigger_size_;
    int fetch_max_bytes_;

    std::thread fetch_thread_; // starts lazily when poll() is first called.
    std::atomic<bool> shutdown_signal_{ false };

    // Round robin index to poll 1 record batch from each subscribed tps.
    int poll_tp_idx_ = 0;

    // Set of brokers that has failed any fetch request/response. When choosing another broker to fetch from, ignores those in this set.
    std::unordered_set<int> failed_brokers_;

    // Subscribed topic partitions, each tp containing its own queue where fetch pushes and poll pops.
    std::vector<TopicPartition> subscribed_tps;

    // Maps broker id to their sockets. Only connects to those fetch brokers in subscribed_tps.
    std::map<int, std::unique_ptr<ClientStub>> broker_sockets_;

    ClusterMetaData metadata_;
    std::mutex metadata_mtx_;

    void recordsFetchThread();

    /**
     * @brief Removes ids that appear in `failed_brokers_` from `brokers_id` in-place.
     */
    void removeFailedBrokers(std::vector<int>& brokers_id);

    /**
     * @brief Returns the id of random healthy broker to fetch from for a desired topic-partition.
     * If no broker available, return -1.
     *
     * Details: This internally looks through `metadata_` to find list of brokers that
     * holds replica of this tp, and ignores those failed in `failed_brokers_`
     */
    int chooseFetchBroker(const std::string& topic_name, int partition);

public:
    KafkaConsumer(int id, int fetch_trigger_size, int fetch_max_bytes) : id_(id),
        fetch_trigger_size_(fetch_trigger_size),
        fetch_max_bytes_(fetch_max_bytes) {
    }

    ~KafkaConsumer() {
        shutdown_signal_ = true;
        if (fetch_thread_.joinable()) {
            fetch_thread_.join();
        }
    }

    /**
     * @brief Pulls `num_rbs` record batches from all topic-partitions queue buffers that stores fetched record batches.
     * This blocks until either `num_rbs` are popped or timeout happens, returning either empty or partially filled vector.
     *
     * Details:
     * It pops one record batch from each tp's queue in round-robin fashion.
     * @param num_rbs
     * @param timeout_ms
     * @return std::vector<ConsumerRecordBatch>
     */
    std::vector<ConsumerRecordBatch> poll(int num_rbs, int timeout_ms = 1000);

    /**
     * @brief Subscribe to all partitions from a topic.
     */
    bool subscribe(const std::string& topic, bool read_from_start, bool leader_only);

    /**
     * @brief Subscribe to a topic-partition.
     */
    bool subscribe(const std::string& topic, int partition, bool read_from_start, bool leader_only);

    // Metadata management: either fetched through broker or set manually.
    void fetchClusterMetadata(const std::string& broker_ip, int broker_port);
    void setClusterMetadata(ClusterMetaData& metadata);

    // Special function used by a broker to instantiate a consumer to fetch from controller. 
    bool _sendBrokerRegistration(std::string broker_ip, uint16_t broker_port, int controller_id);
};

#endif