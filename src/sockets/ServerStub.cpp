#include <iostream>
#include "ServerStub.hpp"
#include <sys/sendfile.h>

ServerStub::ServerStub() {}

void ServerStub::init(std::unique_ptr<ServerSocket> socket, int host_id) {
	this->host_id = host_id;
	this->socket = std::move(socket);
}

std::unique_ptr<Request> ServerStub::receiveRequest() {
	// Receive length
	int net_req_len;
	if (!socket->Recv((char*)&net_req_len, sizeof(net_req_len), 0)) {
		return nullptr;
	}
	int req_len = ntohl(net_req_len);

	// Receive request
	if (!socket->Recv(buffer, req_len, 0)) {
		return nullptr;
	}

	// Extract type from the buffer (at offset 4, after requester_id)
	RequestType type = static_cast<RequestType>(buffer[4]);
	std::unique_ptr<Request> request;

	switch (type) {
	case RequestType::PRODUCE:
		request = std::unique_ptr<ProduceRequest>(new ProduceRequest());
		break;
	case RequestType::FETCH:
		request = std::unique_ptr<FetchRequest>(new FetchRequest());
		break;
	case RequestType::CLUSTER_METADATA:
		request = std::unique_ptr<ClusterMetaDataRequest>(new ClusterMetaDataRequest());
		break;
	case RequestType::BROKER_REGISTRATION:
		request = std::unique_ptr<BrokerRegistrationRequest>(new BrokerRegistrationRequest());
		break;
	default:
		return nullptr;
	}

	request->unmarshal(buffer);
	return request;
}

bool ServerStub::sendResponse(const Response& resp) {
	// Send length
	int resp_len = resp.getLength();
	int net_resp_len = htonl(resp_len);

	if (!socket->Send((char*)&net_resp_len, sizeof(net_resp_len))) {
		return false;
	}

	// Send response
	resp.marshal(buffer);
	if (!socket->Send(buffer, resp_len)) {
		return false;
	}
	return true;
}



// This still sends back a fetch response even if no record batches are read (that list will just be empty).
bool ServerStub::sendFetchResponse(Log& tp_log, int fetch_offset, int fetch_max_bytes, bool allow_beyond_commit) {
	// 1. Lookup physical address `fetch_offset` in file and how many bytes to fetch 
	//    based on configured max fetch bytes and commit offset.
	uint64_t file_addr;
	int read_size;
	int num_rbs_read = tp_log.lookupOffsetInLog(fetch_offset, fetch_max_bytes, file_addr, read_size, allow_beyond_commit);

	// 2. Create fetch response by constructing its fields individually
	//    This is because the list of record batches will be sent zero-copy fashion from file_fd to socket_fd through linux sendFile api.
	int responder_id = host_id;
	uint8_t status = static_cast<uint8_t>(StatusCode::SUCCESS);
	int commit_offset = tp_log.getCommitOffset();

	// Send length first
	int resp_len = sizeof(responder_id) + sizeof(status) + sizeof(commit_offset) + sizeof(num_rbs_read) + read_size;
	int net_resp_len = htonl(resp_len);
	if (!socket->Send((char*)&net_resp_len, sizeof(net_resp_len))) {
		return false;
	}

	// Marshal all fields in fetch response except list of record batches
	int net_responder = htonl(responder_id);
	memcpy(buffer, &net_responder, sizeof(net_responder));
	int buf_size = sizeof(net_responder);

	buffer[buf_size] = status;
	buf_size += sizeof(uint8_t);

	int net_commit_offset = htonl(commit_offset);
	memcpy(buffer + buf_size, &net_commit_offset, sizeof(net_commit_offset));
	buf_size += sizeof(net_commit_offset);

	int net_num_rbs = htonl(num_rbs_read);
	memcpy(buffer + buf_size, &net_num_rbs, sizeof(net_num_rbs));
	buf_size += sizeof(net_num_rbs);

	// Send all fields in fetch response except list of record batches
	if (!socket->Send(buffer, buf_size)) {
		return false;
	}

	// Send list of record batches in zero-copy fashion through sendfile call
	// Skip if no batches read
	if (num_rbs_read != 0) {
		ssize_t total = 0;
		off_t off = file_addr;
		while (total < read_size) {
			ssize_t n = sendfile(socket->getFd(), tp_log.getFileFd(), &off, read_size - total);
			if (n <= 0) return false;
			total += n;
		}
	}

	return true;
}