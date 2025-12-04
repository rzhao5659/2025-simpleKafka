#include "records.hpp"
#include <cstring>
#include <arpa/inet.h>
#include <unistd.h>
#include <assert.h>
#include <algorithm>
#include <iostream> 

#define LAST_OFFSET -1

void Record::marshal(char* buffer) const {
    int key_length = key.length();
    int value_length = value.length();

    int net_rel_offset = htonl(rel_offset);
    int net_key_length = htonl(key_length);
    int net_value_length = htonl(value_length);

    int offset = 0;
    memcpy(buffer + offset, &net_rel_offset, sizeof(net_rel_offset));
    offset += sizeof(net_rel_offset);
    memcpy(buffer + offset, &net_key_length, sizeof(net_key_length));
    offset += sizeof(net_key_length);
    memcpy(buffer + offset, key.data(), key_length);
    offset += key_length;
    memcpy(buffer + offset, &net_value_length, sizeof(net_value_length));
    offset += sizeof(net_value_length);
    memcpy(buffer + offset, value.data(), value_length);
}

void Record::unmarshal(const char* buffer) {
    int net_rel_offset;
    int net_key_length;
    int net_value_length;

    int offset = 0;
    memcpy(&net_rel_offset, buffer + offset, sizeof(net_rel_offset));
    offset += sizeof(net_rel_offset);
    rel_offset = ntohl(net_rel_offset);

    memcpy(&net_key_length, buffer + offset, sizeof(net_key_length));
    int key_length = ntohl(net_key_length);
    offset += sizeof(net_key_length);
    key = std::string(buffer + offset, key_length);
    offset += key_length;

    memcpy(&net_value_length, buffer + offset, sizeof(net_value_length));
    int value_length = ntohl(net_value_length);
    offset += sizeof(net_value_length);
    value = std::string(buffer + offset, value_length);
}

int Record::getLength() const {
    int key_length = key.length();
    int value_length = value.length();
    return sizeof(rel_offset) + sizeof(key_length) + key_length + sizeof(value_length) + value_length;
}

void RecordBatch::marshal(char* buffer) const {
    int net_base_offset = htonl(base_offset);
    int net_num_records = htonl(records.size());

    int offset = 0;
    memcpy(buffer + offset, &net_base_offset, sizeof(net_base_offset));
    offset += sizeof(net_base_offset);
    memcpy(buffer + offset, &net_num_records, sizeof(net_num_records));
    offset += sizeof(net_num_records);

    for (const auto& record : records) {
        record.marshal(buffer + offset);
        offset += record.getLength();
    }
}

void RecordBatch::unmarshal(const char* buffer) {
    int net_base_offset;
    int net_num_records;

    int offset = 0;
    memcpy(&net_base_offset, buffer + offset, sizeof(net_base_offset));
    offset += sizeof(net_base_offset);
    memcpy(&net_num_records, buffer + offset, sizeof(net_num_records));
    offset += sizeof(net_num_records);

    base_offset = ntohl(net_base_offset);
    int num_records = ntohl(net_num_records);

    records.clear();
    for (int i = 0; i < num_records; i++) {
        Record record;
        record.unmarshal(buffer + offset);
        records.push_back(record);
        offset += record.getLength();
    }
}

int RecordBatch::getLength() const {
    int total_length = sizeof(base_offset) + sizeof(records.size());
    for (const auto& record : records) {
        total_length += record.getLength();
    }
    return total_length;
}


// Log
Log::Log(const std::string& log_path) : path_(log_path) {
    fd_ = open(log_path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) {
        throw std::runtime_error("Couldn't create metadata log file.");
    }
}

bool Log::append(RecordBatch rb) {
    int rb_len = rb.getLength();
    std::vector<char> rb_buf(rb_len);
    rb.marshal(rb_buf.data());
    return this->append(rb_buf.data(), rb_len);
}

bool Log::append(char* rb_buffer, int rb_len) {
    std::unique_lock<std::mutex> lk(mtx_);

    // Get current log file size in bytes
    uint64_t before_append_log_size = static_cast<uint64_t>(lseek(fd_, 0, SEEK_END));

    // Assign logical offset to this batch 
    int base_offset = last_written_offset_ + 1;
    int num_records = RecordBatch::getNumRecords(rb_buffer);
    assert(num_records > 0);
    RecordBatch::setBaseOffset(rb_buffer, base_offset);

    // Append the serialized record batch 
    ssize_t written = write(fd_, rb_buffer, rb_len);
    if (written != rb_len) {
        perror("append log error");
        return false;
    }

    // Update in-memory index and last written offset
    index_.push_back(std::make_pair(base_offset, before_append_log_size));
    last_written_offset_ += RecordBatch::getNumRecords(rb_buffer);
    return true;
}

Log::~Log() {
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
}
int Log::lookupOffsetInLog(int offset, int max_bytes, uint64_t& output_start_addr, int& output_read_bytes, bool allow_beyond_commit) {
    std::unique_lock<std::mutex> lk(mtx_);

    int upper_offset = last_written_offset_;
    if (!allow_beyond_commit) {
        upper_offset = commit_offset_;
    }

    if (offset == LAST_OFFSET) {
        offset = upper_offset;
    }
    if (offset > upper_offset) {
        output_read_bytes = 0;
        return 0;
    }

    // no messages in log
    if (index_.empty()) {
        output_read_bytes = 0;
        return 0;
    }

    // // 1. Find the first RecordBatch that contains `offset`. This gives us the start address.
    // //    TODO: Currently it's done in linear fashion, should be done in binary search.
    // int start_rb_idx = -1;
    // for (size_t i = 0; i < index_.size(); i++) {
    //     int rb_base_offset = index_[i].first;
    //     if (rb_base_offset <= offset) {
    //         start_rb_idx = i;
    //     } else {
    //         break;
    //     }
    // }

    // 1. Binary search to find the RecordBatch that contains `offset`
    //    We want the largest index i where index_[i].first <= offset
    int start_rb_idx = -1;
    int left = 0;
    int right = index_.size() - 1;
    while (left <= right) {
        int mid = left + (right - left) / 2;
        int rb_base_offset = index_[mid].first;
        if (rb_base_offset <= offset) {
            start_rb_idx = mid;
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    // If no record batch contains such offset
    if (start_rb_idx == -1) {
        output_read_bytes = 0;
        return 0;
    }

    uint64_t start_addr = index_[start_rb_idx].second;

    // 2. We want to send a list of record batches starting at that physical address. 
    //    Add record batches to the list until adding another RB will exceed either max_bytes or upper_offset.
    //    Hence, find the final RecordBatch.
    //    Assumption: upper_offset, offset can be at the start of a RB (never in between records within a RB). 
    int end_rb_idx = start_rb_idx;
    uint64_t accum_bytes = 0;

    for (size_t i = start_rb_idx; i < index_.size(); i++) {
        int base_offset_i = index_[i].first;
        if ((base_offset_i > upper_offset)) {
            break;
        }

        // Compute size including the ith record batch.
        uint64_t accum_bytes_if_included;
        if (i == index_.size() - 1) {
            accum_bytes_if_included = static_cast<uint64_t>(lseek(fd_, 0, SEEK_END)) - index_[start_rb_idx].second; // EOF
        } else {
            accum_bytes_if_included = index_[i + 1].second - index_[start_rb_idx].second;
        }

        // Check if including this batch would exceed max_bytes
        if (accum_bytes_if_included > static_cast<uint64_t>(max_bytes)) {
            break;
        }

        end_rb_idx = i;
        accum_bytes = accum_bytes_if_included;
    }

    output_start_addr = start_addr;
    output_read_bytes = accum_bytes;
    int num_rbs = end_rb_idx - start_rb_idx + 1;
    return num_rbs;
}

// Create and open append-only log.
void Log::setLogPath(const std::string& log_path) {
    if (fd_ < 0) {
        path_ = log_path;
        fd_ = open(log_path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) {
            throw std::runtime_error("Couldn't create metadata log file.");
        }
    }
}

void Log::debugPrintLog() {
    std::unique_lock<std::mutex> lk(mtx_);

    std::cout << "\n========================================" << std::endl;
    std::cout << "DEBUG: Log File Contents" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Path: " << path_ << std::endl;
    std::cout << "Last Written Offset: " << last_written_offset_ << std::endl;
    std::cout << "Commit Offset: " << commit_offset_ << std::endl;
    std::cout << "Index size: " << index_.size() << " record batches" << std::endl;

    // Get file size
    off_t file_size = lseek(fd_, 0, SEEK_END);
    std::cout << "File size: " << file_size << " bytes" << std::endl;
    std::cout << "========================================\n" << std::endl;

    int batch_num = 0;
    int total_records = 0;

    char buffer[4096];

    // Iterate through all record batches using the in-memory index
    for (size_t i = 0; i < index_.size(); i++) {
        uint64_t physical_addr = index_[i].second;

        // Seek to the physical address of this record batch
        lseek(fd_, physical_addr, SEEK_SET);

        // Determine the size of this record batch
        int rb_size;
        if (i == index_.size() - 1) {
            // Last batch: read until EOF
            rb_size = file_size - physical_addr;
        } else {
            // Not last batch: read until next batch starts
            rb_size = index_[i + 1].second - physical_addr;
        }

        // Make sure buffer is large enough and size is reasonable
        if (rb_size <= 0 || rb_size > 4096) {
            std::cout << "Warning: RecordBatch size (" << rb_size
                << " bytes) is invalid or exceeds buffer. Skipping..." << std::endl;
            continue;
        }

        // Read the entire record batch into buffer
        ssize_t bytes_read = read(fd_, buffer, rb_size);
        if (bytes_read != rb_size) {
            std::cout << "Error: Failed to read record batch " << i
                << " (expected " << rb_size << " bytes, got " << bytes_read << ")" << std::endl;
            perror("read");
            break;
        }

        // Unmarshal the record batch
        RecordBatch rb;
        rb.unmarshal(buffer);

        // Print record batch info
        std::cout << "RecordBatch #" << batch_num << std::endl;
        std::cout << "  Base Offset: " << rb.base_offset << std::endl;
        std::cout << "  Num Records: " << rb.getNumRecords() << std::endl;
        std::cout << "  Physical Address: " << physical_addr << std::endl;
        std::cout << "  Size: " << rb_size << " bytes" << std::endl;

        // Print each record in the batch
        for (size_t j = 0; j < rb.records.size(); j++) {
            const Record& r = rb.records[j];
            int absolute_offset = rb.base_offset + r.rel_offset;

            std::cout << "    Record #" << j << " (offset " << absolute_offset << ")" << std::endl;
            std::cout << "      rel_offset: " << r.rel_offset << std::endl;
            std::cout << "      key: \"" << r.key << "\"" << std::endl;
            std::cout << "      value: \"" << r.value << "\"" << std::endl;

            total_records++;
        }
        std::cout << std::endl;
        batch_num++;
    }

    std::cout << "========================================" << std::endl;
    std::cout << "Total: " << batch_num << " batches, " << total_records << " records" << std::endl;
    std::cout << "========================================\n" << std::endl;

    // Reset file position
    lseek(fd_, 0, SEEK_END);
}