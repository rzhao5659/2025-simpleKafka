#ifndef __NETWORK_MESSAGES_H__
#define __NETWORK_MESSAGES_H__

#include <stdint.h>
#include <string>
#include <vector>
#include "cluster_metadata.hpp"
#include "records.hpp"

// ============================================================================
// BASE CLASSES - Common interface for all messages
// ============================================================================
enum RequestType : uint8_t {
    INVALID = 0,
    PRODUCE = 1,
    FETCH = 2,
    CLUSTER_METADATA = 3,
    BROKER_REGISTRATION = 4,
};

enum StatusCode : uint8_t {
    ERROR = 0,
    SUCCESS = 1,
    NOT_LEADER_ERROR = 2,
    UNKNOWN_TOPIC_PARTITION = 3,
    REPLICA_NOT_ASSIGNED = 4,
};

class NetworkMessage {
public:
    virtual ~NetworkMessage() = default;
    virtual void marshal(char* buffer) const = 0;
    virtual void unmarshal(const char* buffer) = 0;
    virtual int getLength() const = 0;
};

class Request : public NetworkMessage {
public:
    RequestType getType() const { return type_; }

    int getRequesterId() const { return requester_id_; }
    void setRequesterId(int requester_id) { requester_id_ = requester_id; }

protected:
    RequestType type_;
    int requester_id_;
};

class Response : public NetworkMessage {
public:
    StatusCode getStatus() const { return status_; }
    void setStatus(StatusCode status) { status_ = status; }

    int getResponderId() const { return status_; }
    void setResponderId(int responder_id) { responder_id_ = responder_id; }
protected:
    StatusCode status_;
    int responder_id_;
};

// ============================================================================
// PRODUCE - Writing data
// ============================================================================
class ProduceRequest : public Request {
public:
    /* Binary Format:
     * +------------------+------------------+------------------------+
     * | Field            | Type             | Size (bytes)           |
     * +------------------+------------------+------------------------+
     * | requester_id     | int32            | 4                      |
     * | type             | uint8_t          | 1                      |
     * | topic            | byte[]           | 32                     |
     * | partition        | int32            | 4                      |
     * | record_batch     | RecordBatch      | variable               |
     * +------------------+------------------+------------------------+
     */
    ProduceRequest() { type_ = RequestType::PRODUCE; }

    std::string topic;
    int partition;
    RecordBatch record_batch;

    void marshal(char* buffer) const override;
    void unmarshal(const char* buffer) override;
    int getLength() const override;
};

class ProduceResponse : public Response {
public:
    /* Binary Format:
     * +------------------+------------------+------------------------+
     * | Field            | Type             | Size (bytes)           |
     * +------------------+------------------+------------------------+
     * | responder_id     | int32            | 4                      |
     * | status           | uint8_t          | 1                      |
     * +------------------+------------------+------------------------+
     */
    void marshal(char* buffer) const override;
    void unmarshal(const char* buffer) override;
    int getLength() const override;
};


// ============================================================================
// CLUSTER METADATA - Retrieves all broker connection info and their topic-partitions.
// ============================================================================

class ClusterMetaDataRequest : public Request {
public:
    /* Binary Format:
     * +------------------+------------------+------------------------+
     * | Field            | Type             | Size (bytes)           |
     * +------------------+------------------+------------------------+
     * | requester_id     | int32            | 4                      |
     * | type             | uint8_t          | 1                      |
     * +------------------+------------------+------------------------+
     */

    ClusterMetaDataRequest() { type_ = RequestType::CLUSTER_METADATA; }

    void marshal(char* buffer) const override;
    void unmarshal(const char* buffer) override;
    int getLength() const override;
};

class ClusterMetaDataResponse : public Response {
public:
    /* Binary Format:
     * +------------------+------------------+------------------------+
     * | Field            | Type             | Size (bytes)           |
     * +------------------+------------------+------------------------+
     * | responder_id     | int32            | 4                      |
     * | status           | uint8_t          | 1                      |
     * | cluster_metadata | ClusterMetaData  | variable               |
     * +------------------+------------------+------------------------+
     */

    ClusterMetaData cluster_metadata;

    void marshal(char* buffer) const override;
    void unmarshal(const char* buffer) override;
    int getLength() const override;
};

// ============================================================================
// Broker Registration Request - Broker must send this to the controller to register itself as part of cluster.
// ============================================================================

class BrokerRegistrationRequest : public Request {
public:
    /* Binary Format:
     * +------------------+------------------+------------------------+
     * | Field            | Type             | Size (bytes)           |
     * +------------------+------------------+------------------------+
     * | requester_id     | int32            | 4                      |
     * | type             | uint8_t          | 1                      |
     * | requester_ip     | bytes[]          | 4                      |
     * | requester_port   | uint16_t         | 2                      |
     * +------------------+------------------+------------------------+
     */

    BrokerRegistrationRequest() { type_ = RequestType::BROKER_REGISTRATION; }
    std::string ip;
    uint16_t port;

    void marshal(char* buffer) const override;
    void unmarshal(const char* buffer) override;
    int getLength() const override;
};

class BrokerRegistrationResponse : public Response {
public:
    /* Binary Format:
     * +------------------+------------------+------------------------+
     * | Field            | Type             | Size (bytes)           |
     * +------------------+------------------+------------------------+
     * | responder_id     | int32            | 4                      |
     * | status           | uint8_t          | 1                      |
     * +------------------+------------------+------------------------+
     */
    void marshal(char* buffer) const override;
    void unmarshal(const char* buffer) override;
    int getLength() const override;
};


// ============================================================================
// FETCH - Reading data 
// ============================================================================
class FetchRequest : public Request {
public:
    /* Binary Format:
     * +------------------+------------------+------------------------+
     * | Field            | Type             | Size (bytes)           |
     * +------------------+------------------+------------------------+
     * | requester_id     | int32            | 4                      |
     * | type             | uint8_t          | 1                      |
     * | topic            | char[32]         | 32                     |
     * | partition        | int32            | 4                      |
     * | fetch_offset     | int32            | 4                      |
     * | fetch_max_bytes  | int32            | 4                      |
     * +------------------+------------------+------------------------+
     */

    FetchRequest() { type_ = RequestType::FETCH; }

    std::string topic;
    int partition;
    int fetch_offset;  // -1 reserved to be latest 
    int fetch_max_bytes;

    void marshal(char* buffer) const override;
    void unmarshal(const char* buffer) override;
    int getLength() const override;
};

class FetchResponse : public Response {
public:
    /* Binary Format:
     * +------------------+------------------+------------------------+
     * | Field            | Type             | Size (bytes)           |
     * +------------------+------------------+------------------------+
     * | responder_id     | int32            | 4                      |
     * | status           | uint8_t          | 1                      |
     * | commit offset    | int32            | 4                      |
     * | num_record_batch | int32            | 4                      |
     * | record_batches   | RecordBatch[]    | variable               |
     * +------------------+------------------+------------------------+
     */
    int commit_offset;
    std::vector<RecordBatch> record_batches;

    void marshal(char* buffer) const override;
    void unmarshal(const char* buffer) override;
    int getLength() const override;
};



#endif 
