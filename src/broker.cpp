#include "broker.hpp"
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
#include "debug_stream.hpp"
#include <shared_mutex>


std::string roleToString(AssignedTopicPartition::Role role) {
    switch (role) {
    case AssignedTopicPartition::Role::NONE: return "NONE";
    case AssignedTopicPartition::Role::LEADER: return "LEADER";
    case AssignedTopicPartition::Role::FOLLOWER: return "FOLLOWER";
    default: return "UNKNOWN";
    }
}


void Broker::debugThread() {
    // Periodically prints to console all assigned tp state of the broker
    while (!shutdown_signal_) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        {
            // Lock as reader
            std::shared_lock lk(metadata_tps_mtx_);
            std::cout << "Broker " << id_ << " Assigned Topic Partitions: " << std::endl;
            for (auto& kv : assigned_tps) {
                std::string tp_name = kv.first;
                auto& tp = kv.second;
                if (tp != nullptr) {
                    std::cout << "\t-" << tp_name << ": Role(" << roleToString(tp->role) << "), CommitOffset(" << tp->log.getCommitOffset()
                        << "), LastWrittenOffset(" << tp->log.getLastWrittenOffset() << ")" << std::endl;
                }
            }
        }
    }
}


Broker::Broker(int my_id, std::string my_ip, uint16_t my_port,
    int controller_id, std::string controller_ip, uint16_t controller_port,
    std::string log_folder_path, bool debug) :
    id_(my_id),
    ip_(my_ip),
    port_(my_port),
    tp_log_dir(log_folder_path),
    controller_consumer(my_id, 10, 4096)   // id, fetch_trigger_size, fetch_max_bytes
{
    // Initialize cluster metadata: add controller conn info and metadata topic
    metadata_.broker_conn_info[controller_id] = std::make_pair(controller_ip, controller_port);
    Topic metadata_topic;
    metadata_topic.id = "cluster_metadata";
    Partition p;
    p.id = 0;
    p.broker_leader_id = controller_id;
    metadata_topic.partitions.push_back(p);
    metadata_.topics["cluster_metadata"] = metadata_topic;

    // Initialize socket that listens connection from clients (producer/consumer)
    acceptor_socket_.Init(port_);

    // Set up an consumer instance to fetch from controller.
    controller_consumer.setClusterMetadata(metadata_);
    controller_consumer._sendBrokerRegistration(my_ip, my_port, controller_id);
    controller_consumer.subscribe("cluster_metadata", 0, true, true);

    // Initialize background threads
    std::thread metadata_poll_thread(&Broker::pollMetadataThread, this);
    metadata_poll_thread_ = std::move(metadata_poll_thread);

    std::thread replication_poll_thread(&Broker::replicationThread, this);
    replication_poll_thread_ = std::move(replication_poll_thread);

    if (debug) {
        std::thread debug_thread(&Broker::debugThread, this);
        debug_thread_ = std::move(debug_thread);
    }
}

void Broker::onPartitionAssignment(PartitionAssignmentRecord& rec) {
    // Lock as writer. 
    std::unique_lock lk(metadata_tps_mtx_);
    std::string tp_name = getTopicPartitionName(rec.topic, rec.partition);

    // Update in-memory cluster metadata:
    // 1. Add topic if new
    metadata_.topics.insert(std::make_pair(rec.topic, Topic()));
    Topic& topic = metadata_.topics[rec.topic];
    topic.id = rec.topic;

    // 2. Add partition if new (assume ordered)
    if (rec.partition == topic.partitions.size()) {
        Partition new_p;
        topic.partitions.push_back(new_p);
    }

    // 3. Update topic-partition info in metadata.
    Partition& p = topic.partitions[rec.partition];
    p.broker_followers_id = rec.broker_followers_id;
    p.broker_leader_id = rec.broker_leader_id;
    p.id = rec.partition;

    // Check if this broker is involved in this tp assignment.
    bool is_leader = (rec.broker_leader_id == id_);
    bool is_follower = std::find(rec.broker_followers_id.begin(),
        rec.broker_followers_id.end(),
        id_) != rec.broker_followers_id.end();

    if (!is_leader && !is_follower) {
        return;
    }

    // Create a topic partition in this broker (if not initialized)
    if (assigned_tps.find(tp_name) == assigned_tps.end()) {
        std::string log_path = tp_log_dir + tp_name;
        assigned_tps[tp_name] = std::unique_ptr<AssignedTopicPartition>(
            new AssignedTopicPartition(log_path, rec.topic, rec.partition)
        );
    }

    AssignedTopicPartition* tp = assigned_tps[tp_name].get();

    // Case 1. Broker is initialized as leader, remains as leader, or promoted from follower to leader (F -> L)
    Role old_role = tp->role;
    if (is_leader) {
        // Stop consuming from leader partition.
        tp->leader_consumer.reset();
        tp->role = Role::LEADER;

        // TODO: truncate log, written offset, index all to commited.

    } else if (is_follower) {
        // Case 2. Broker is initialized as follower or remains follower but the tp has a new leader.
        tp->role = Role::FOLLOWER;

        if (!tp->leader_consumer) {
            // Initialize consumer to fetch from leader.
            tp->leader_consumer = std::shared_ptr<KafkaConsumer>(new KafkaConsumer(id_, 5, 4096));
            tp->leader_consumer->setClusterMetadata(metadata_);
            tp->leader_consumer->subscribe(tp->topic, tp->partition, true, true);
            tp->broker_leader_id = rec.broker_leader_id;

        } else {
            // If it was already fetching from some leader, check if same or reinitialize to point to new leader.
            int old_leader = tp->broker_leader_id;
            if (rec.broker_leader_id != old_leader) {
                tp->leader_consumer.reset();
                tp->leader_consumer = std::shared_ptr<KafkaConsumer>(new KafkaConsumer(id_, 5, 4096));
                tp->leader_consumer->setClusterMetadata(metadata_);
                tp->leader_consumer->subscribe(tp->topic, tp->partition, true, true);
            }
        }
    } else {
        // Case 3. Broker drops from leader to follower (L -> F). This will never happen as a leader can only be dropped under failure.
    }

    if (old_role != tp->role) {
        std::cout << "Broker " << id_ << " changed role from " << roleToString(old_role) << " to " << roleToString(tp->role) << " for " << tp_name << std::endl;
    }

}


void Broker::pollMetadataThread() {
    while (!shutdown_signal_) {
        std::vector<ConsumerRecordBatch> metadata_rbs = controller_consumer.poll(1);

        for (auto& crb : metadata_rbs) {
            for (int i = 0; i < crb.record_batch.getNumRecords(); i++) {
                Record metadata_record = crb.record_batch.records[i];

                debugstream << "Received metadata change record: " << metadata_record << std::endl;

                // Parse metadata change record
                if (metadata_record.key == "BrokerRegistration") {
                    // Parse comma delimited value string into its attributes
                    BrokerRegistrationRecord broker_reg;
                    broker_reg.parseFromRecord(metadata_record);

                    // Add broker info to metadata 
                    {
                        std::unique_lock<std::shared_mutex> lk(metadata_tps_mtx_);
                        metadata_.broker_conn_info[broker_reg.broker_id] = std::make_pair(broker_reg.broker_ip, broker_reg.broker_port);
                    }

                } else if (metadata_record.key == "PartitionAssignment") {
                    // Parse comma delimited value string into its attributes
                    PartitionAssignmentRecord rec;
                    rec.parseFromRecord(metadata_record);

                    onPartitionAssignment(rec);
                }
            }
        }

        // debugstream << "New cluster metadata: \n" << metadata_ << std::endl;

    } // end of while(true)

}

void Broker::clientHandlerThread(std::unique_ptr<ServerSocket> socket) {
    ServerStub stub;
    stub.init(std::move(socket), id_);

    try {
        while (stub.isAlive() && !shutdown_signal_) {
            // Handle either Produce, Fetch, or ClusterMetadata requests.
            std::unique_ptr<Request> req = stub.receiveRequest();
            if (req == nullptr) {
                break;
            }

            RequestType req_type = req->getType();

            switch (req_type) {
            case RequestType::FETCH: {
                auto* fetch_req = static_cast<FetchRequest*>(req.get());
                int fetcher_id = fetch_req->getRequesterId();
                std::string tp_name = getTopicPartitionName(fetch_req->topic, fetch_req->partition);

                FetchResponse resp;
                resp.setResponderId(id_);

                // Lock as reader
                metadata_tps_mtx_.lock_shared();

                // Check if this tp exists 
                bool tp_exists = metadata_.containsTopicPartition(fetch_req->topic, fetch_req->partition);
                if (!tp_exists) {
                    resp.setStatus(StatusCode::UNKNOWN_TOPIC_PARTITION);
                    metadata_tps_mtx_.unlock_shared();
                    stub.sendResponse(resp);
                    break;
                }

                // Check if this broker is assigned to this tp.
                auto it = assigned_tps.find(tp_name);
                bool assigned = it != assigned_tps.end();
                if (!assigned) {
                    resp.setStatus(StatusCode::REPLICA_NOT_ASSIGNED);
                    metadata_tps_mtx_.unlock_shared();
                    stub.sendResponse(resp);
                    break;
                }

                AssignedTopicPartition* tp = it->second.get();
                Log& tp_log = tp->log;

                std::vector<int> followers = metadata_.getBrokerFollowers(fetch_req->topic, fetch_req->partition);
                bool is_fetcher_follower = metadata_.isBrokerAFollower(fetcher_id, fetch_req->topic, fetch_req->partition);
                if (tp->role == Role::LEADER && is_fetcher_follower) {
                    // Case 1. Broker (leader) is being fetched by another broker (follower)    

                    // Update this follower last fetch offset
                    tp->follower_fetch_offsets[fetcher_id] = fetch_req->fetch_offset;

                    // As a leader, update commit index if all broker followers have fetched offset > commit offset.
                    int min_follower_fetch_offset = fetch_req->fetch_offset;
                    for (int follower_id : followers) {
                        int other_follower_fetch_offset = tp->follower_fetch_offsets[follower_id];
                        min_follower_fetch_offset = std::min(min_follower_fetch_offset, other_follower_fetch_offset);
                    }

                    int min_follower_written_offset = min_follower_fetch_offset - 1;
                    if (min_follower_written_offset > tp_log.getCommitOffset()) {
                        tp_log.setCommitOffset(min_follower_written_offset);
                    }

                    // Fetch from tp_log and send a success response (internally built)
                    // As the requester is a follower, allow fetching up to last written offset.
                    metadata_tps_mtx_.unlock_shared();
                    stub.sendFetchResponse(tp_log, fetch_req->fetch_offset, fetch_req->fetch_max_bytes, true);

                } else {
                    // Case 2. Broker is being fetched by a consumer.

                    // Fetch from tp_log and send a success response (internally built)
                    // For consistency, only allow fetching up to commit offset.
                    metadata_tps_mtx_.unlock_shared();
                    stub.sendFetchResponse(tp_log, fetch_req->fetch_offset, fetch_req->fetch_max_bytes, false);
                }

                break;
            }


            case RequestType::PRODUCE: {

                auto* prod_req = static_cast<ProduceRequest*>(req.get());
                ProduceResponse resp;
                resp.setResponderId(id_);
                StatusCode status = StatusCode::SUCCESS;

                {
                    // Lock as reader (log has its own mutex for appending)
                    std::shared_lock lk(metadata_tps_mtx_);

                    // Check if this tp exists
                    if (!metadata_.containsTopicPartition(prod_req->topic, prod_req->partition)) {
                        status = StatusCode::UNKNOWN_TOPIC_PARTITION;

                        // Check if this broker is the leader for this tp
                    } else if (!metadata_.isBrokerALeader(id_, prod_req->topic, prod_req->partition)) {
                        status = StatusCode::NOT_LEADER_ERROR;

                        // Check if there is any follower, otherwise reject messages as no replication can be done
                    } else if (metadata_.getBrokerFollowers(prod_req->topic, prod_req->partition).empty()) {
                        status = StatusCode::NO_FOLLOWERS;

                    } else {
                        // Append the record batch to this tp log (updates automatically last written offset)
                        const std::string tp_name =
                            getTopicPartitionName(prod_req->topic, prod_req->partition);
                        Log& tp_log = assigned_tps[tp_name]->log;
                        tp_log.append(prod_req->record_batch);
                        status = StatusCode::SUCCESS;
                    }
                }

                resp.setStatus(status);
                stub.sendResponse(resp);
                break;

            }

            case RequestType::CLUSTER_METADATA: {

                ClusterMetaDataResponse resp;
                resp.setResponderId(id_);
                resp.setStatus(StatusCode::SUCCESS);
                {
                    std::shared_lock lk(metadata_tps_mtx_);
                    resp.cluster_metadata = metadata_;
                }

                stub.sendResponse(resp);
                break;
            }

            }  // end of switch
        } // end of while 
    }
    catch (const std::exception& e) {
        std::cerr << "clientHandlerThread exception: " << e.what() << std::endl;
    }

}


void Broker::listenForClientConnections() {
    while (!shutdown_signal_) {
        std::unique_ptr<ServerSocket> client_socket = acceptor_socket_.Accept();
        if (client_socket == nullptr) continue;
        client_handler_threads_.emplace_back(&Broker::clientHandlerThread, this, std::move(client_socket));
    }
}

void Broker::replicationThread() {
    while (!shutdown_signal_) {

        {
            std::shared_lock lk(metadata_tps_mtx_);

            // Collect all partitions where we're a follower
            std::vector<std::pair<std::string, std::shared_ptr<KafkaConsumer>>> followers_to_poll;
            for (auto& kv : assigned_tps) {
                std::unique_ptr<AssignedTopicPartition>& tp = kv.second;
                if (tp->role == Role::FOLLOWER) {
                    assert(tp->leader_consumer);
                    followers_to_poll.push_back(std::make_pair(kv.first, tp->leader_consumer));
                }
            }

            // Replication is by fetching from each tp's leader as a consumer. 
            for (auto& pair : followers_to_poll) {
                std::string tp_name = pair.first;
                std::shared_ptr<KafkaConsumer> consumer = pair.second;
                std::vector<ConsumerRecordBatch> batches = consumer->poll(1, 10); // Max wait 10ms

                for (auto& crb : batches) {
                    // Append to log
                    std::unique_ptr<AssignedTopicPartition>& tp = assigned_tps[tp_name];
                    if (crb.record_batch.getNumRecords() != 0) {
                        tp->log.append(crb.record_batch);

                    }

                    // As follower, update our commit offset based on leader's commit offset. 
                    int leader_commit_offset = crb.commit_offset;
                    if (tp->log.getCommitOffset() < leader_commit_offset &&
                        tp->log.getLastWrittenOffset() >= leader_commit_offset) {
                        tp->log.setCommitOffset(leader_commit_offset);
                    }
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }


}