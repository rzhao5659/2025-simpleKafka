#ifndef __RECORDS_HPP__
#define __RECORDS_HPP__

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <unistd.h>    
#include <fcntl.h>     
#include <stdexcept>
#include <map>
#include <mutex>
#include <cstring>
#include <arpa/inet.h>

class Record {
public:
    /* Binary Format:
     * +------------------+------------------+------------------------+
     * | Field            | Type             | Size (bytes)           |
     * +------------------+------------------+------------------------+
     * | rel_offset       | int32            | 4                      |
     * | key_length       | int32            | 4                      |
     * | key              | byte[]           | key_length             |
     * | value_length     | int32            | 4                      |
     * | value            | byte[]           | value_length           |
     * +------------------+------------------+------------------------+
     */
    int rel_offset;
    std::string key;
    std::string value;

    void marshal(char* buffer) const;
    void unmarshal(const char* buffer);
    int getLength() const;

    friend std::ostream& operator<<(std::ostream& os, const Record& obj) {
        os << "(" << obj.key << ":" << obj.value << ")";
        return os;
    }

};

class RecordBatch {
public:
    /* Binary Format:
     * +------------------+------------------+------------------------+
     * | Field            | Type             | Size (bytes)           |
     * +------------------+------------------+------------------------+
     * | base_offset      | int32            | 4                      |
     * | num_records      | int32            | 4                      |
     * | records          | Record[]         | variable               |
     * +------------------+------------------+------------------------+
     */
    int base_offset;
    std::vector<Record> records;

    void marshal(char* buffer) const;
    void unmarshal(const char* buffer);
    int getLength() const;

    void addRecord(Record& record) {
        record.rel_offset = records.size(); // automatically computes rel offset.
        records.push_back(record);
    }
    int getNumRecords() const { return records.size(); }

    // Extract number of records from serialized bytes representing a record batch.
    // Used by broker to update its last written offset before append to log. 
    static int getNumRecords(char* record_batch_buffer) {
        int net_num_records;
        memcpy(&net_num_records, record_batch_buffer + 4, sizeof(net_num_records));
        return ntohl(net_num_records);
    }

    // Set base offset of the serialized record batch. Used by broker before append to log. 
    static void setBaseOffset(char* record_batch_buffer, int base_offset) {
        int net_base_offset = htonl(base_offset);
        memcpy(record_batch_buffer, &net_base_offset, sizeof(net_base_offset));
    }

    friend std::ostream& operator<<(std::ostream& os, const RecordBatch& batch) {
        os << "RecordBatch[base=" << batch.base_offset
            << ", records=" << batch.records.size() << "] { ";
        for (size_t i = 0; i < batch.records.size(); i++) {
            os << batch.records[i];
            if (i < batch.records.size() - 1) os << ", ";
        }
        os << "}";
        return os;
    }
};


// Fixed record templates used for metadata topic.
// PartitionAssignmentRecord allows brokers to know partition runtime assignment
// The additional attributes that aren't key and value are just for parsing purposes, they aren't transmitted in wire.
class PartitionAssignmentRecord : public Record {
public:
    std::string topic;
    int partition;
    int broker_leader_id;
    std::vector<int> broker_followers_id;

    PartitionAssignmentRecord() {
        key = "PartitionAssignment";
    }
    PartitionAssignmentRecord(const std::string& topic, int partition, int broker_leader_id, std::vector<int> broker_followers_id) {
        key = "PartitionAssignment";

        std::ostringstream val_stream;
        val_stream << topic << ",";
        val_stream << partition << ",";
        val_stream << broker_leader_id;
        for (int follower_id : broker_followers_id) {
            val_stream << "," << follower_id;
        }
        value = val_stream.str();
    }

    void parseFromRecord(const Record& record) {
        value = record.value;
        std::istringstream ss(value);
        std::string token;

        // topic
        if (!std::getline(ss, token, ',')) throw std::runtime_error("Invalid PartitionAssignment value");
        topic = token;

        // partition
        if (!std::getline(ss, token, ',')) throw std::runtime_error("Invalid PartitionAssignment value");
        partition = std::stoi(token);

        // broker_leader_id
        if (!std::getline(ss, token, ',')) throw std::runtime_error("Invalid PartitionAssignment value");
        broker_leader_id = std::stoi(token);

        // broker_followers_id
        broker_followers_id.clear();
        while (std::getline(ss, token, ',')) {
            broker_followers_id.push_back(std::stoi(token));
        }
    }
};


// BrokerRegistrationRecord allows brokers to discover other brokers in the cluster.
class BrokerRegistrationRecord : public Record {
public:
    int broker_id;
    std::string broker_ip;
    uint16_t broker_port;

    BrokerRegistrationRecord() {
        key = "BrokerRegistration";
    }
    BrokerRegistrationRecord(int broker_id, std::string broker_ip, uint16_t broker_port) {
        key = "BrokerRegistration";

        std::ostringstream val_stream;
        val_stream << broker_id << ",";
        val_stream << broker_ip << ",";
        val_stream << broker_port;
        value = val_stream.str();
    }

    void parseFromRecord(const Record& record) {
        value = record.value;
        std::istringstream ss(value);
        std::string token;

        // broker_id
        if (!std::getline(ss, token, ','))
            throw std::runtime_error("Invalid BrokerRegistration value: " + value);
        broker_id = std::stoi(token);

        // broker_ip
        if (!std::getline(ss, token, ','))
            throw std::runtime_error("Invalid BrokerRegistration value: " + value);
        broker_ip = token;

        // broker_port
        if (!std::getline(ss, token, ','))
            throw std::runtime_error("Invalid BrokerRegistration value: " + value);
        broker_port = static_cast<uint16_t>(std::stoi(token));
    }
};




// Thread-safe class that opens and manages a R/W atomic append-only file
// Binary format of log:  list of record batch
class Log {
private:
    int fd_ = -1;
    std::string path_;
    int last_written_offset_ = -1;
    int commit_offset_ = -1;
    std::vector<std::pair<int, uint64_t>> index_; // Sparse sorted array that maps from record (identified by its offset) to its physical byte address in the log file. This contains only the first records from each record batch.
    std::mutex mtx_;

public:
    Log() {}
    Log(const std::string& log_path);
    ~Log();

    // Remove copy constructor/assignment
    Log(const Log&) = delete;
    Log& operator=(const Log&) = delete;

    void setLogPath(const std::string& log_path);

    bool append(RecordBatch rb);
    bool append(char* rb_buf, int rb_len);

    // TODO: for proper replication.
    bool truncate_log_to_commited();

    // Get start address and number of bytes to read a list of record batches, starting with the one that starts with record `offset`, up to `max_bytes` size (can't exceed)
    // return number of rbs 
    int lookupOffsetInLog(int offset, int max_bytes, uint64_t& output_start_addr, int& output_read_bytes, bool allow_beyond_commit);

    int getCommitOffset() {
        std::unique_lock<std::mutex> lk(mtx_);
        return commit_offset_;
    }
    void setCommitOffset(int new_commit_offset) {
        std::unique_lock<std::mutex> lk(mtx_);
        commit_offset_ = new_commit_offset;
    }
    int getLastWrittenOffset() {
        std::unique_lock<std::mutex> lk(mtx_);
        return last_written_offset_;
    }
    int getFileFd() {
        return fd_;
    }


    void debugPrintLog();
};




#endif