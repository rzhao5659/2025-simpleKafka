


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
#include "broker.hpp"

std::string roleToString(AssignedTopicPartition::Role role) {
    switch (role) {
    case AssignedTopicPartition::Role::NONE: return "NONE";
    case AssignedTopicPartition::Role::LEADER: return "LEADER";
    case AssignedTopicPartition::Role::FOLLOWER: return "FOLLOWER";
    default: return "UNKNOWN";
    }
}

void Broker::debugThread() {
    // Periodically prints to console the all assigned tp state of the broker
    while (!shutdown_signal_) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        {
            std::unique_lock<std::mutex> lk(mtx_);
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
    controller_fetcher(my_id, 10, 4096)   // id, fetch_trigger_size, fetch_max_bytes

{

    // Initialize cluster metadata:
    // - Add controller connection info
    metadata_.broker_conn_info[controller_id] = std::make_pair(controller_ip, controller_port);
    // - Add metadata topic
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
    controller_fetcher.setClusterMetadata(metadata_);
    controller_fetcher._sendBrokerRegistration(my_ip, my_port, controller_id);
    controller_fetcher.subscribe("cluster_metadata", 0, true, true);

    // metadata_poll_thread_
    std::thread metadata_poll_thread(&Broker::pollMetadataThread, this);
    metadata_poll_thread_ = std::move(metadata_poll_thread);

    // replication thread.
    std::thread replication_poll_thread(&Broker::replicationThread, this);
    replication_poll_thread_ = std::move(replication_poll_thread);

    if (debug) {
        std::thread debug_thread(&Broker::debugThread, this);
        debug_thread_ = std::move(debug_thread);
    }
}

void Broker::onPartitionAssignment(PartitionAssignmentRecord& rec) {
    std::string tp_name = getTopicPartitionName(rec.topic, rec.partition);
    int cnt;
    // Update in-memory cluster metadata:
    {
        std::unique_lock<std::mutex> lk(mtx_);

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
    }

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

    std::unique_ptr<AssignedTopicPartition>& tp = assigned_tps[tp_name];

    // Case 1. Broker is initialized as leader or promoted from follower to leader (F -> L)
    Role old_role = tp->role;
    if (is_leader && (old_role == Role::NONE || old_role == Role::FOLLOWER)) {

        std::unique_lock<std::mutex> lk(mtx_);
        // Stop consuming from leader partition.
        tp->consumer.reset();
        tp->role = Role::LEADER;


        // TODO: truncate log, written offset, index all to commited.

    } else if (is_follower) {
        // Case 2. Broker is initialized as follower or remains follower but the tp has a new leader.
        std::unique_lock<std::mutex> lk(mtx_);
        tp->role = Role::FOLLOWER;
        std::cout << "meow " << cnt++ << std::endl;

        if (!tp->consumer) {
            // Initialize consumer to fetch from leader.
            tp->consumer = std::shared_ptr<KafkaConsumer>(new KafkaConsumer(id_, 5, 4096));
            tp->consumer->setClusterMetadata(metadata_);
            tp->consumer->subscribe(tp->topic, tp->partition, true, true);
            tp->broker_leader_id = rec.broker_leader_id;

        } else {
            // If it was already fetching from some leader, check if same or reinitialize to point to new leader.
            int old_leader = tp->broker_leader_id;
            if (rec.broker_leader_id != old_leader) {
                tp->consumer.reset();
                tp->consumer = std::shared_ptr<KafkaConsumer>(new KafkaConsumer(id_, 5, 4096));
                tp->consumer->setClusterMetadata(metadata_);
                tp->consumer->subscribe(tp->topic, tp->partition, true, true);

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
    try {
        while (!shutdown_signal_) {
            std::vector<ConsumerRecordBatch> metadata_rbs = controller_fetcher.poll(1);

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
                            std::unique_lock<std::mutex> lk(mtx_);
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
    catch (const std::exception& e) {
        std::cerr << "Poll metadata exception: " << e.what() << std::endl;
    }
}

void Broker::clientHandlerThread(std::unique_ptr<ServerSocket> socket) {
    ServerStub stub;
    stub.init(std::move(socket), id_);

    while (stub.isAlive()) {
        // Handle either Produce, Fetch, or ClusterMetadata requests.
        std::unique_ptr<Request> req = stub.receiveRequest();
        if (req == nullptr) {
            // debugstream << "Client disconnected" << std::endl;
            break;
        }

        RequestType req_type = req->getType();

        // // DEBUG
        // std::cout << "broker received request type of " << static_cast<int>(req_type) << std::endl;
        try {
            switch (req_type) {
            case RequestType::FETCH: {
                auto* fetch_req = static_cast<FetchRequest*>(req.get());
                int fetcher_id = fetch_req->getRequesterId();
                std::string tp_name = getTopicPartitionName(fetch_req->topic, fetch_req->partition);

                // DEBUG
                // std::cerr << "=== FETCH REQUEST ===" << std::endl;
                // std::cerr << "Broker ID: " << id_ << std::endl;
                // std::cerr << "Fetcher ID: " << fetcher_id << std::endl;
                // std::cerr << "Topic: " << fetch_req->topic << std::endl;
                // std::cerr << "Partition: " << fetch_req->partition << std::endl;
                // std::cerr << "Fetch offset: " << fetch_req->fetch_offset << std::endl;

                FetchResponse resp;
                resp.setResponderId(id_);

                {
                    std::unique_lock<std::mutex> lk(mtx_);

                    // Check if this tp exists 
                    bool tp_exists = metadata_.containsTopicPartition(fetch_req->topic, fetch_req->partition);

                    if (!tp_exists) {
                        // DEBUG
                        // std::cerr << "ERROR: Sending UNKNOWN_TOPIC_PARTITION for " << fetch_req->topic << fetch_req->partition << std::endl;
                        resp.setStatus(StatusCode::UNKNOWN_TOPIC_PARTITION);
                    } else {
                        // Check if this broker is assigned to this tp.
                        bool assigned = assigned_tps.find(tp_name) != assigned_tps.end();
                        if (!assigned) {
                            // DEBUG
                            // std::cerr << "ERROR: Sending REPLICA_NOT_ASSIGNED" << std::endl;
                            resp.setStatus(StatusCode::REPLICA_NOT_ASSIGNED);
                        } else {
                            // DEBUG
                            // std::cerr << "SUCCESS: Broker holds replica, processing fetch" << std::endl;

                            std::unique_ptr<AssignedTopicPartition>& tp = assigned_tps[tp_name];
                            Log& tp_log = tp->log;

                            // Update follower offset if this is a follower fetching from us (the leader)
                            std::vector<int> followers = metadata_.getBrokerFollowers(fetch_req->topic, fetch_req->partition);
                            bool is_fetcher_follower = metadata_.isBrokerAFollower(fetcher_id, fetch_req->topic, fetch_req->partition);
                            if (tp->role == Role::LEADER && is_fetcher_follower) {
                                // Broker (leader) is being fetched by a broker follower. 

                                // DEBUG
                                // std::cout << "im getting fetched by follower" << std::endl;

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

                                // Fetch from tp_log and send a success fetch response (internally built)
                                // The fetcher being a broker follower, allow fetch beyond commit offset. 
                                stub.sendFetchResponse(tp_log, fetch_req->fetch_offset, fetch_req->fetch_max_bytes, true);

                            } else {
                                // Broker is being fetched by a consumer.
                                // Fetch from tp log and send a success response (internally built)
                                // The fetcher is a consumer, for consistency do not allow beyond commit offset.
                                stub.sendFetchResponse(tp_log, fetch_req->fetch_offset, fetch_req->fetch_max_bytes, false);
                            }

                            break;
                        }
                    }

                    // Send back failed fetch response
                    assert(resp.getStatus() != StatusCode::SUCCESS);
                    stub.sendResponse(resp);
                }

                break;
            }

            case RequestType::PRODUCE: {

                auto* prod_req = static_cast<ProduceRequest*>(req.get());
                ProduceResponse resp;
                resp.setResponderId(id_);

                {
                    std::unique_lock<std::mutex> lk(mtx_);

                    // Check if this tp exists 
                    if (!metadata_.containsTopicPartition(prod_req->topic, prod_req->partition)) {
                        resp.setStatus(StatusCode::UNKNOWN_TOPIC_PARTITION);
                    } else {
                        // Check if this broker is the leader for this tp.
                        bool is_this_broker_leader = metadata_.isBrokerALeader(id_, prod_req->topic, prod_req->partition);
                        if (!is_this_broker_leader) {
                            resp.setStatus(StatusCode::NOT_LEADER_ERROR);
                        } else {
                            // Append the record batch to this tp log (updates automatically last written offset)
                            std::string tp_name = getTopicPartitionName(prod_req->topic, prod_req->partition);
                            Log& tp_log = assigned_tps[tp_name]->log;
                            tp_log.append(prod_req->record_batch);

                            resp.setStatus(StatusCode::SUCCESS);
                        }
                    }
                }

                stub.sendResponse(resp);
                break;

            }

            case RequestType::CLUSTER_METADATA: {

                ClusterMetaDataResponse resp;
                resp.setResponderId(id_);
                resp.setStatus(StatusCode::SUCCESS);
                {
                    std::unique_lock<std::mutex> lk(mtx_);
                    resp.cluster_metadata = metadata_;
                }

                stub.sendResponse(resp);
                break;
            }

            }  // end of switch


        }
        catch (const std::exception& e) {
            std::cerr << "Broker's Client handler thread crashed: " << e.what() << std::endl;
        }

    }

    debugstream << "clientHandlerThread died" << std::endl;
}


void Broker::listenForClientConnections() {
    // Indefinitely listens for client connection and spawns dedicated thread for each.
    while (true) {
        std::unique_ptr<ServerSocket> client_socket = acceptor_socket_.Accept();
        if (client_socket == nullptr) continue;
        std::thread client_thread(&Broker::clientHandlerThread, this, std::move(client_socket));
        client_thread.detach();
    }
}


void Broker::replicationThread() {
    try {
        while (!shutdown_signal_) {
            int cnt = 0;
            // Collect all partitions where we're a follower

            std::vector<std::pair<std::string, std::shared_ptr<KafkaConsumer>>> followers_to_poll;
            {
                std::unique_lock<std::mutex> lk(mtx_);
                for (auto& kv : assigned_tps) {
                    std::unique_ptr<AssignedTopicPartition>& tp = kv.second;
                    if (tp->role == Role::FOLLOWER && tp->consumer) {
                        followers_to_poll.push_back(std::make_pair(kv.first, tp->consumer));
                    }
                }

            }

            // Replication is by fetching from each tp's leader as a consumer. 
            for (auto& pair : followers_to_poll) {
                std::string tp_name = pair.first;
                std::shared_ptr<KafkaConsumer> consumer = pair.second;


                std::vector<ConsumerRecordBatch> batches = consumer->poll(1);


                for (auto& crb : batches) {
                    // Check if still a follower before processing
                    {

                        std::unique_lock<std::mutex> lk(mtx_);
                        std::unique_ptr<AssignedTopicPartition>& tp = assigned_tps[tp_name];

                        // Skip if no longer a follower (promoted to leader)
                        if (tp->role != Role::FOLLOWER) {
                            continue;
                        }
                    }

                    // Append to log (has its own mutex)

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

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    catch (const std::exception& e) {
        std::cerr << "rep died: " << e.what() << std::endl;
    }

}