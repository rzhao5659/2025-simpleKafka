#ifndef __SERVER_STUB_H__
#define __SERVER_STUB_H__

#include <memory>
#include <arpa/inet.h>
#include "ServerSocket.hpp"
#include "network_messages.hpp"
#include "records.hpp"

class ServerStub {
private:
	std::unique_ptr<ServerSocket> socket;
	char buffer[4096];
public:
	int host_id;

	ServerStub();
	void init(std::unique_ptr<ServerSocket> socket, int host_id);

	std::unique_ptr<Request> receiveRequest();
	bool sendResponse(const Response& resp);

	// FetchResponse is treated differently due to zero-copy sendFile api
	bool sendFetchResponse(Log& tp_log, int fetch_offset, int fetch_max_bytes, bool allow_beyond_commit);

	bool isAlive() {
		return socket->IsFdValid();
	}
};

#endif 
