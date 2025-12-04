
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

// KafkaConsumer is a single threaded object that pulls records from topics.
// API:
// poll() returns a list of records that may come from multiple topic-partitions.
// subscribe(std::string topic) to subscribe to all partitions of a topic.

// There is a fetch and poll process.  fetch is done at large size, whereas poll is smaller size that comes from local buffer first.
// We overlap/pipeline fetch and poll.  While processing data during poll, fetch is done in the background.

// Implementation:
// - Records Data structure: have a vector of subbed-topic-partition (broker id to extract from , topic, partition, queue of records, need_Fetch_status), where each tp has a queue of records (discard recordbatch), 
// - A map of broker id to broker socket.
// - An array of failed broker ids to avoid. 

// In the main thread: When you poll(10), you extract 10 records in round robin fashion, one from each topic-partition. persist an index.  If no topic-parittion has data, it will just block by going through the vector repeatedly.
// In the fetch thread: Build FetchRequest for all brokers -> Send all requests -> Wait for responses for each.  
//      Maybe scan through vector of topic-partition, check queue size, if smaller than half, mark it as need_fetching [send max_bytes as half of queue size].
//      Then the infinite loop is now, build fetchrequest, send, and wait only for those with need_fetch = true. 
//      When receiving response, if one fails, choose other replica (that is not from arr from failed brokers) RANDOMLY for stateless load balancing.  
// Have a ClusterMetadataFetchThread that fetches periodically, i only think this is useful if dynamic topics, and brokers. since im not, its not that useful. 
//      It can also be useful when cluster removed one broker, but somehow that broker connection with client still works.  ALl these edge cases are not necessary for my project.
//      For now the only benefit on me is that consumer fetches this to know about topic creation. Otherwise dont allow client to consume from certain topic.  
//      Btw, when periodically refreshed, its possible that a topic-partition original broker fetched from is now gone.  This detection can be done lazily in fetch thread (which will just choose other), so this thread dont need to do this.

//  Consumer offset: a folder in ``consumer_offsets` with a file `id`. That file should contains for each topic-partition a consumer offset.
//  Append consumer offset then process it.  (makes sure its possilbe to skip, but never process twice or more)  
//  During initialization: load from this if exists. else: consumer can request broker for the latest offset or start at earliest record (offset = 0).


// Notes: All records here are guaranteed to be commited.
// The usage must be first subscribe, then poll.  No multithreading until first poll(), and subscribed topic-partitions are 

// tp = topic-partition.


// Due to time,  wont implement this consumer offset local tracking.
// ALlow consumer either start from 0 or latest.

// I will also assume consumers are ran after the brokers and topics are added. this way consumer only need to do an initial fetch to the cluster metadata, and not periodically refresh it. 
// The periodic refresh is somewhat an issue since it kinda locks other from fetch request.  This avoids all this locking bs. 

// ALSO FOR DEBUG PURPOSE, INStead of randomly choose one, chooose always the leader. This also makes it so i dont need to implement replication if time ends. 

// Consumers may be passed an initial cluster metadata, or do an initial fetch for cluster metadata. so the bootstrap should be optional.
// Have a manual setCLusterMetadata method. 
// This way brokers can use this class by passing initial cluster metadata, and as it receives cluster metadata topic, updates it through set. 

// TODO: ConsuimerRecrodbatches, queue of record batches, dont worry about consumer offset or commit offset being in the middle as it can only be at start rb.

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