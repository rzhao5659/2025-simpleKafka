
#include <string>
#include <queue>
#include <map> 
#include "ClientStub.hpp"
#include "network_messages.hpp"
#include "cluster_metadata.hpp"
#include <stdexcept>
#include "option.hpp"
#include "debug_stream.hpp"
#include "producer.hpp"

bool KafkaProducer::send(const std::string& topic, const std::string& key, const std::string& value) {
    return send_batch(topic, key, std::vector<std::string>{value});
}

bool KafkaProducer::send_batch(const std::string& topic, const std::string& key, const std::vector<std::string>& values) {
    // Refresh cluster metadata if stale. This could happen when leader broker changes and send fails.
    if (is_metadata_stale_) {
        if (!fetch_cluster_metadata()) {
            debugstream << "Failed to fetch cluster metadata." << std::endl;
            return false;
        }
        is_metadata_stale_ = false;
    }

    // Get partition id
    int partition_id = get_partition(topic, key);
    if (partition_id == -1) {
        debugstream << "Topic not found" << std::endl;
        return false;
    }

    // Create record batch
    RecordBatch record_batch;
    record_batch.base_offset = -1;  // filled by broker 
    for (const auto& value : values) {
        Record record;
        record.key = key;
        record.value = value;
        record_batch.addRecord(record);
    }

    // Find the leader broker for this partition
    int broker_leader_id = metadata_.getBrokerLeader(topic, partition_id);
    if (broker_leader_id == -1) {
        is_metadata_stale_ = true;
        debugstream << "Metadata is stale: couldn't find topic-partition" << std::endl;
        return false;
    }

    // Connect if necessary
    ClientStub* broker_leader_stub_ = getOrCreateStub(broker_leader_id);
    if (!broker_leader_stub_->isConnected()) {
        std::string broker_ip = metadata_.broker_conn_info[broker_leader_id].first;
        int broker_port = metadata_.broker_conn_info[broker_leader_id].second;
        if (!broker_leader_stub_->connect(broker_ip, broker_port)) {
            // When connection fails, mark cluster metadata as stale to retry next time, 
            // as kafka cluster should detect failure and change leader.
            debugstream << "Failed to connect to leader broker " << broker_leader_id << std::endl;
            is_metadata_stale_ = true;
            return false;
        }
    }

    // Send ProduceRequest (write request) synchronously
    Option<ProduceResponse> resp_opt = broker_leader_stub_->sendProduceRequest(topic, partition_id, record_batch);
    if (!resp_opt.hasValue()) {
        debugstream << "Failed to send/receive produce request to broker " << broker_leader_id << std::endl;
        return false;
    }
    ProduceResponse resp = resp_opt.getValue();
    if (resp.getStatus() == StatusCode::NOT_LEADER_ERROR) {
        debugstream << "Broker " << broker_leader_id << " is not leader for topic " << topic << " partition " << partition_id << std::endl;
        is_metadata_stale_ = true;
        return false;
    }

    return true;
}


KafkaProducer::KafkaProducer(int my_id, const std::string& broker_ip, int broker_port) : id_(my_id) {
    {
        // Initialize connection to a broker to fetch cluster metadata.
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
        metadata_ = resp.cluster_metadata;
    }

    // Initialize stubs for all brokers in the cluster metadata
    for (const auto& broker_info_pair : metadata_.broker_conn_info) {
        int broker_id = broker_info_pair.first;
        if (broker_sockets_.find(broker_id) == broker_sockets_.end()) {
            std::unique_ptr<ClientStub> stub(new ClientStub(id_));
            broker_sockets_[broker_id] = std::move(stub);
        }
    }
    is_metadata_stale_ = false;
}

ClientStub* KafkaProducer::getOrCreateStub(int broker_id) {
    if (broker_sockets_.find(broker_id) == broker_sockets_.end()) {
        std::unique_ptr<ClientStub> stub(new ClientStub(id_));
        broker_sockets_[broker_id] = std::move(stub);
    }
    return broker_sockets_[broker_id].get();
}

bool KafkaProducer::fetch_cluster_metadata() {
    // Unlike constructor, where we only have one bootstrap broker, 
    // here we can try any broker. Need to avoid controller due to bad implementation of having it included as a broker (let me just assume it's the 0th id).
    int CONTROLLER_ID = 0;

    for (auto& kv : broker_sockets_) {
        int broker_id = kv.first;
        std::unique_ptr<ClientStub>& broker_stub = kv.second;

        // Don't allow producer connect to controller, as it's assumed that only brokers connect to it.
        if (broker_id == CONTROLLER_ID) {
            continue;
        }

        // Connect if necessary
        if (!broker_stub->isConnected()) {
            std::string broker_ip = metadata_.broker_conn_info[broker_id].first;
            int broker_port = metadata_.broker_conn_info[broker_id].second;
            if (!broker_stub->connect(broker_ip, broker_port)) {
                continue; // try next broker
            }
        }

        // Send cluster metadata request
        Option<ClusterMetaDataResponse> resp_opt = broker_stub->sendClusterMetadataRequest();
        if (resp_opt.hasValue()) {
            ClusterMetaDataResponse resp = resp_opt.getValue();
            metadata_ = resp.cluster_metadata;
            return true;
        }
    }
    return false;
}


int KafkaProducer::get_partition(const std::string& topic, const std::string& key) {
    // Partition is assigned by key hashing.
    if (metadata_.topics.find(topic) == metadata_.topics.end()) {
        return -1;
    }
    std::hash<std::string> hash_fn;
    Topic topic_ = metadata_.topics[topic];
    int partition_id = (int)(hash_fn(key) % topic_.getNumPartitions());
    return partition_id;
}


