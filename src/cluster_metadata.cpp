#include "cluster_metadata.hpp"
#include <algorithm>

void Partition::unmarshal(const char* buffer) {
    int net_id;
    int net_broker_leader_id;
    int net_num_followers;

    int offset = 0;
    memcpy(&net_id, buffer + offset, sizeof(net_id));
    offset += sizeof(net_id);
    memcpy(&net_broker_leader_id, buffer + offset, sizeof(net_broker_leader_id));
    offset += sizeof(net_broker_leader_id);
    memcpy(&net_num_followers, buffer + offset, sizeof(net_num_followers));
    offset += sizeof(net_num_followers);

    id = ntohl(net_id);
    broker_leader_id = ntohl(net_broker_leader_id);
    int num_followers = ntohl(net_num_followers);

    broker_followers_id.clear();
    for (int i = 0; i < num_followers; i++) {
        int net_follower_id;
        memcpy(&net_follower_id, buffer + offset, sizeof(net_follower_id));
        offset += sizeof(net_follower_id);
        broker_followers_id.push_back(ntohl(net_follower_id));
    }
}

int Partition::getLength() const {
    int num_followers = broker_followers_id.size();
    return sizeof(id) + sizeof(broker_leader_id) + sizeof(num_followers) + sizeof(int) * num_followers;
}

void Partition::marshal(char* buffer) const {
    int net_id = htonl(id);
    int net_broker_leader_id = htonl(broker_leader_id);
    int net_num_followers = htonl((int)broker_followers_id.size());

    int offset = 0;
    memcpy(buffer + offset, &net_id, sizeof(net_id));
    offset += sizeof(net_id);
    memcpy(buffer + offset, &net_broker_leader_id, sizeof(net_broker_leader_id));
    offset += sizeof(net_broker_leader_id);
    memcpy(buffer + offset, &net_num_followers, sizeof(net_num_followers));
    offset += sizeof(net_num_followers);

    for (int follower_id : broker_followers_id) {
        int net_follower_id = htonl(follower_id);
        memcpy(buffer + offset, &net_follower_id, sizeof(net_follower_id));
        offset += sizeof(net_follower_id);
    }
}
void Topic::unmarshal(const char* buffer) {
    char id_buf[32];
    int net_num_partitions;

    int offset = 0;
    memcpy(id_buf, buffer + offset, 32);
    offset += 32;
    memcpy(&net_num_partitions, buffer + offset, sizeof(net_num_partitions));
    offset += sizeof(net_num_partitions);

    id = std::string(id_buf); // truncates trailing /0

    int num_partitions = ntohl(net_num_partitions);

    partitions.clear();
    for (int i = 0; i < num_partitions; i++) {
        Partition partition;
        partition.unmarshal(buffer + offset);
        partitions.push_back(partition);
        offset += partition.getLength();
    }
}

void Topic::marshal(char* buffer) const {
    char id_buf[32];
    memset(id_buf, 0, 32);
    memcpy(id_buf, id.c_str(), std::min((size_t)32, id.size()));
    int net_num_partitions = htonl((int)partitions.size());

    int offset = 0;
    memcpy(buffer + offset, id_buf, 32);
    offset += 32;
    memcpy(buffer + offset, &net_num_partitions, sizeof(net_num_partitions));
    offset += sizeof(net_num_partitions);

    for (const auto& partition : partitions) {
        partition.marshal(buffer + offset);
        offset += partition.getLength();
    }
}

int Topic::getLength() const {
    int total = 32 + sizeof(int);
    for (const auto& p : partitions) total += p.getLength();
    return total;
}

void ClusterMetaData::unmarshal(const char* buffer) {
    int net_num_topics;

    int offset = 0;
    memcpy(&net_num_topics, buffer + offset, sizeof(net_num_topics));
    offset += sizeof(net_num_topics);

    int num_topics = ntohl(net_num_topics);

    topics.clear();
    for (int i = 0; i < num_topics; i++) {
        Topic topic;
        topic.unmarshal(buffer + offset);
        topics[topic.id] = topic;
        offset += topic.getLength();
    }

    // broker conn info
    int net_num_brokers;
    memcpy(&net_num_brokers, buffer + offset, sizeof(net_num_brokers));
    offset += sizeof(net_num_brokers);

    int num_brokers = ntohl(net_num_brokers);
    broker_conn_info.clear();
    for (int i = 0; i < num_brokers; ++i) {
        int net_broker_id;
        uint8_t ip_bytes[4];
        uint16_t net_port;

        memcpy(&net_broker_id, buffer + offset, sizeof(net_broker_id));
        offset += sizeof(net_broker_id);
        int broker_id = ntohl(net_broker_id);

        memcpy(ip_bytes, buffer + offset, sizeof(ip_bytes));
        offset += sizeof(ip_bytes);
        char ip_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, ip_bytes, ip_str, INET_ADDRSTRLEN);

        memcpy(&net_port, buffer + offset, sizeof(net_port));
        offset += sizeof(net_port);
        int port = ntohs(net_port);

        broker_conn_info[broker_id] = std::make_pair(std::string(ip_str), port);
    }
}

void ClusterMetaData::marshal(char* buffer) const {
    int net_num_topics = htonl((int)topics.size());

    int offset = 0;
    memcpy(buffer + offset, &net_num_topics, sizeof(net_num_topics));
    offset += sizeof(net_num_topics);

    for (auto& pair : topics) {
        pair.second.marshal(buffer + offset);
        offset += pair.second.getLength();
    }

    // broker conn info 
    int net_num_brokers = htonl((int)broker_conn_info.size());
    memcpy(buffer + offset, &net_num_brokers, sizeof(net_num_brokers));
    offset += sizeof(net_num_brokers);

    for (const auto& kv : broker_conn_info) {
        int broker_id = kv.first;
        std::string broker_ip = kv.second.first;
        uint16_t broker_port = kv.second.second;

        // broker_id
        int net_broker_id = htonl(broker_id);
        memcpy(buffer + offset, &net_broker_id, sizeof(net_broker_id));
        offset += sizeof(net_broker_id);

        // convert ip from string to bytes
        uint8_t ip_bytes[4];
        if (inet_pton(AF_INET, broker_ip.c_str(), ip_bytes) != 1) {
            std::cerr << "Invalid IP: " << broker_ip << "\n";
            continue;
        }
        memcpy(buffer + offset, ip_bytes, sizeof(ip_bytes));
        offset += sizeof(ip_bytes);

        // port
        uint16_t net_port = htons(broker_port);
        memcpy(buffer + offset, &net_port, sizeof(net_port));
        offset += sizeof(net_port);
    }
}

int ClusterMetaData::getLength() const {
    int total = sizeof(int);
    for (const auto& pair : topics) total += pair.second.getLength();
    // brokers: num_brokers int + each broker (4-byte ID + 4-byte IP + 2-byte port)
    int num_brokers = broker_conn_info.size();
    total += sizeof(num_brokers);
    total += (4 + 4 + 2) * num_brokers;
    return total;
}


std::ostream& operator<<(std::ostream& os, const ClusterMetaData& metadata) {
    os << "=== Cluster Metadata ===\n";

    // Print broker information
    os << "Brokers (" << metadata.broker_conn_info.size() << "):\n";
    for (const auto& id_conn_kv : metadata.broker_conn_info) {
        int broker_id = id_conn_kv.first;
        auto& conn_info = id_conn_kv.second;
        std::string ip = conn_info.first;
        uint16_t port = conn_info.second;
        os << "  Broker " << broker_id << ": " << ip << ":" << port << "\n";
    }

    // Print topic information
    os << "Topics (" << metadata.topics.size() << "):\n";
    for (const auto& kv : metadata.topics) {
        std::string topic_id = kv.first;
        Topic topic = kv.second;
        os << "  Topic: " << topic_id << " (" << topic.partitions.size() << " partitions)\n";

        for (const auto& partition : topic.partitions) {
            os << "    Partition " << partition.id
                << " - Leader: Broker " << partition.broker_leader_id;

            if (!partition.broker_followers_id.empty()) {
                os << ", Followers: [";
                for (size_t i = 0; i < partition.broker_followers_id.size(); ++i) {
                    os << "Broker " << partition.broker_followers_id[i];
                    if (i < partition.broker_followers_id.size() - 1) {
                        os << ", ";
                    }
                }
                os << "]";
            }
            os << "\n";
        }
    }

    return os;
}