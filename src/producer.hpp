
#ifndef PRODUCER_HPP
#define PRODUCER_HPP

#include <string>
#include <map> 
#include "ClientStub.hpp"
#include "cluster_metadata.hpp"
#include <memory>


// For simplicity, KafkaProducer will just synchronously send records to broker.
class KafkaProducer {
public:
    KafkaProducer(int my_id, const std::string& broker_ip, int broker_port);
    bool send(const std::string& topic, const std::string& key, const std::string& value);
    bool send_batch(const std::string& topic, const std::string& key, const std::vector<std::string>& values);

private:
    int id_;
    std::map<int, std::unique_ptr<ClientStub>> broker_sockets_; // maps broker id to their sockets. lazy connection.
    ClusterMetaData metadata_;
    bool is_metadata_stale_;

    int get_partition(const std::string& topic, const std::string& key);
    bool fetch_cluster_metadata();
    ClientStub* getOrCreateStub(int broker_id);

};

#endif