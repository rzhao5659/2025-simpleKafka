#include "network_messages.hpp"
#include <cstring>
#include <iostream>
#include <arpa/inet.h>

constexpr size_t TOPIC_SIZE = 32;

// #### ProduceRequest ####
void ProduceRequest::marshal(char* buffer) const {
    int net_requester = htonl(requester_id_);
    int net_partition = htonl(partition);

    int offset = 0;
    memcpy(buffer + offset, &net_requester, sizeof(net_requester));
    offset += sizeof(net_requester);

    buffer[offset] = static_cast<uint8_t>(type_);
    offset++;

    size_t topic_len = topic.size();
    memcpy(buffer + offset, topic.c_str(), topic_len);
    // pad remaining bytes with zeros
    if (topic_len < TOPIC_SIZE) {
        memset(buffer + offset + topic_len, 0, TOPIC_SIZE - topic_len);
    }
    offset += TOPIC_SIZE;

    memcpy(buffer + offset, &net_partition, sizeof(net_partition));
    offset += sizeof(net_partition);

    record_batch.marshal(buffer + offset);
}

void ProduceRequest::unmarshal(const char* buffer) {
    int offset = 0;
    int net_requester;

    memcpy(&net_requester, buffer + offset, sizeof(net_requester));
    offset += sizeof(net_requester);
    requester_id_ = ntohl(net_requester);

    type_ = static_cast<RequestType>(buffer[offset]);
    offset++;

    topic = std::string(buffer + offset);
    offset += 32;

    int net_partition;
    memcpy(&net_partition, buffer + offset, sizeof(net_partition));
    offset += sizeof(net_partition);
    partition = ntohl(net_partition);

    record_batch.unmarshal(buffer + offset);
}

int ProduceRequest::getLength() const {
    return sizeof(requester_id_) + sizeof(RequestType) + 32 + sizeof(partition) + record_batch.getLength();
}

// #### ProduceResponse ####
void ProduceResponse::marshal(char* buffer) const {
    int net_responder = htonl(responder_id_);
    memcpy(buffer, &net_responder, sizeof(net_responder));
    buffer[sizeof(net_responder)] = static_cast<uint8_t>(status_);
}

void ProduceResponse::unmarshal(const char* buffer) {
    int net_responder;
    memcpy(&net_responder, buffer, sizeof(net_responder));
    responder_id_ = ntohl(net_responder);
    status_ = static_cast<StatusCode>(buffer[sizeof(net_responder)]);
}

int ProduceResponse::getLength() const {
    return sizeof(responder_id_) + sizeof(StatusCode);
}


// #### ClusterMetaDataRequest ####
void ClusterMetaDataRequest::marshal(char* buffer) const {
    int net_requester = htonl(requester_id_);
    memcpy(buffer, &net_requester, sizeof(net_requester));
    buffer[sizeof(net_requester)] = static_cast<uint8_t>(type_);
}

void ClusterMetaDataRequest::unmarshal(const char* buffer) {
    int net_requester;
    memcpy(&net_requester, buffer, sizeof(net_requester));
    requester_id_ = ntohl(net_requester);
    type_ = static_cast<RequestType>(buffer[sizeof(net_requester)]);
}

int ClusterMetaDataRequest::getLength() const {
    return sizeof(requester_id_) + sizeof(RequestType);
}


// #### ClusterMetaDataResponse ####
void ClusterMetaDataResponse::marshal(char* buffer) const {
    int net_responder = htonl(responder_id_);
    memcpy(buffer, &net_responder, sizeof(net_responder));
    int offset = sizeof(net_responder);
    buffer[offset] = static_cast<uint8_t>(status_);
    offset += sizeof(uint8_t);
    cluster_metadata.marshal(buffer + offset);
}

void ClusterMetaDataResponse::unmarshal(const char* buffer) {
    int net_responder;
    memcpy(&net_responder, buffer, sizeof(net_responder));
    responder_id_ = ntohl(net_responder);
    int offset = sizeof(net_responder);
    status_ = static_cast<StatusCode>(buffer[offset]);
    offset += sizeof(uint8_t);
    cluster_metadata.unmarshal(buffer + offset);
}

int ClusterMetaDataResponse::getLength() const {
    return sizeof(responder_id_) + sizeof(StatusCode) + cluster_metadata.getLength();
}

// #### BrokerRegistrationRequest ####
void BrokerRegistrationRequest::marshal(char* buffer) const {
    int offset = 0;

    int net_requester = htonl(requester_id_);
    memcpy(buffer + offset, &net_requester, sizeof(net_requester));
    offset += sizeof(net_requester);

    buffer[offset] = static_cast<uint8_t>(type_);
    offset++;

    // convert ip from string to bytes
    uint8_t ip_bytes[4];
    inet_pton(AF_INET, ip.c_str(), ip_bytes);
    memcpy(buffer + offset, ip_bytes, sizeof(ip_bytes));
    offset += sizeof(ip_bytes);

    // port
    uint16_t net_port = htons(port);
    memcpy(buffer + offset, &net_port, sizeof(net_port));
}

void BrokerRegistrationRequest::unmarshal(const char* buffer) {
    int offset = 0;

    int net_requester;
    memcpy(&net_requester, buffer + offset, sizeof(net_requester));
    offset += sizeof(net_requester);
    requester_id_ = ntohl(net_requester);

    type_ = static_cast<RequestType>(buffer[offset]);
    offset++;

    // ip
    uint8_t ip_bytes[4];
    memcpy(ip_bytes, buffer + offset, sizeof(ip_bytes));
    char ip_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, ip_bytes, ip_str, INET_ADDRSTRLEN);
    ip = std::string(ip_str);
    offset += sizeof(ip_bytes);

    // port
    uint16_t net_port;
    memcpy(&net_port, buffer + offset, sizeof(net_port));
    port = ntohs(net_port);
}

int BrokerRegistrationRequest::getLength() const {
    return sizeof(requester_id_) + sizeof(RequestType) + 4 + 2;
}

// #### BrokerRegistrationResponse ####
void BrokerRegistrationResponse::marshal(char* buffer) const {
    int net_responder = htonl(responder_id_);
    memcpy(buffer, &net_responder, sizeof(net_responder));
    buffer[sizeof(net_responder)] = static_cast<uint8_t>(status_);
}

void BrokerRegistrationResponse::unmarshal(const char* buffer) {
    int net_responder;
    memcpy(&net_responder, buffer, sizeof(net_responder));
    responder_id_ = ntohl(net_responder);
    status_ = static_cast<StatusCode>(buffer[sizeof(net_responder)]);
}

int BrokerRegistrationResponse::getLength() const {
    return sizeof(responder_id_) + sizeof(StatusCode);
}

// #### FetchRequest ####
void FetchRequest::marshal(char* buffer) const {
    int net_requester = htonl(requester_id_);
    int net_partition = htonl(partition);
    int net_fetch_offset = htonl(fetch_offset);
    int net_fetch_max_bytes = htonl(fetch_max_bytes);

    int offset = 0;
    memcpy(buffer + offset, &net_requester, sizeof(net_requester));
    offset += sizeof(net_requester);

    buffer[offset] = static_cast<uint8_t>(type_);
    offset++;

    size_t topic_len = topic.size();
    memcpy(buffer + offset, topic.c_str(), topic_len);
    // pad remaining bytes with zeros
    if (topic_len < TOPIC_SIZE) {
        memset(buffer + offset + topic_len, 0, TOPIC_SIZE - topic_len);
    }
    offset += TOPIC_SIZE;

    memcpy(buffer + offset, &net_partition, sizeof(net_partition));
    offset += sizeof(net_partition);

    memcpy(buffer + offset, &net_fetch_offset, sizeof(net_fetch_offset));
    offset += sizeof(net_fetch_offset);

    memcpy(buffer + offset, &net_fetch_max_bytes, sizeof(net_fetch_max_bytes));
}

void FetchRequest::unmarshal(const char* buffer) {
    int net_requester, net_partition, net_fetch_offset, net_fetch_max_bytes;

    int offset = 0;
    memcpy(&net_requester, buffer + offset, sizeof(net_requester));
    offset += sizeof(net_requester);
    requester_id_ = ntohl(net_requester);

    type_ = static_cast<RequestType>(buffer[offset]);
    offset++;

    topic = std::string(buffer + offset);
    offset += 32;

    memcpy(&net_partition, buffer + offset, sizeof(net_partition));
    offset += sizeof(net_partition);
    partition = ntohl(net_partition);

    memcpy(&net_fetch_offset, buffer + offset, sizeof(net_fetch_offset));
    offset += sizeof(net_fetch_offset);
    fetch_offset = ntohl(net_fetch_offset);

    memcpy(&net_fetch_max_bytes, buffer + offset, sizeof(net_fetch_max_bytes));
    fetch_max_bytes = ntohl(net_fetch_max_bytes);
}

int FetchRequest::getLength() const {
    return sizeof(requester_id_) + sizeof(RequestType) + 32 +
        sizeof(partition) + sizeof(fetch_offset) + sizeof(fetch_max_bytes);
}

// #### FetchResponse ####
void FetchResponse::marshal(char* buffer) const {
    int net_responder = htonl(responder_id_);
    memcpy(buffer, &net_responder, sizeof(net_responder));
    int offset = sizeof(net_responder);

    buffer[offset] = static_cast<uint8_t>(status_);
    offset += sizeof(uint8_t);

    int net_commit_offset = htonl(commit_offset);
    memcpy(buffer + offset, &net_commit_offset, sizeof(net_commit_offset));
    offset += sizeof(net_commit_offset);

    int net_num_rbs = htonl(record_batches.size());
    memcpy(buffer + offset, &net_num_rbs, sizeof(net_num_rbs));
    offset += sizeof(net_num_rbs);

    for (const auto& rb : record_batches) {
        rb.marshal(buffer + offset);
        offset += rb.getLength();
    }
}

void FetchResponse::unmarshal(const char* buffer) {
    int net_responder;
    memcpy(&net_responder, buffer, sizeof(net_responder));
    responder_id_ = ntohl(net_responder);
    int offset = sizeof(net_responder);

    status_ = static_cast<StatusCode>(buffer[offset]);
    offset += sizeof(uint8_t);

    int net_commit_offset;
    memcpy(&net_commit_offset, buffer + offset, sizeof(net_commit_offset));
    commit_offset = ntohl(net_commit_offset);
    offset += sizeof(net_commit_offset);

    int net_num_rbs;
    memcpy(&net_num_rbs, buffer + offset, sizeof(net_num_rbs));
    offset += sizeof(net_num_rbs);
    int num_rbs = ntohl(net_num_rbs);

    record_batches.clear();
    for (int i = 0; i < num_rbs; i++) {
        RecordBatch rb;
        rb.unmarshal(buffer + offset);
        record_batches.push_back(rb);
        offset += rb.getLength();
    }
}

int FetchResponse::getLength() const {
    int total = sizeof(responder_id_) + sizeof(StatusCode) + sizeof(commit_offset) + sizeof(int);
    for (const auto& rb : record_batches) {
        total += rb.getLength();
    }
    return total;
}