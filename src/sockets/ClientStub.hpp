#ifndef __CLIENT_STUB_H__
#define __CLIENT_STUB_H__

#include <string>
#include <vector>

#include "ClientSocket.hpp"
#include "network_messages.hpp"
#include "option.hpp"

class ClientStub {
private:
	ClientSocket socket_;
	std::vector<char> buffer_;

public:
	int server_id;
	int client_id;

	ClientStub(int client_id) : server_id(-1), client_id(client_id) {};

	// Move constructor and assignment
	ClientStub(ClientStub&&) = default;
	ClientStub& operator=(ClientStub&&) = default;

	// Delete copy operations
	ClientStub(const ClientStub&) = delete;
	ClientStub& operator=(const ClientStub&) = delete;

	bool connect(const std::string& ip, int port);
	bool isConnected() { return socket_.IsFdValid(); }

	// All requests send its length then itself, as most of these are variable-length. 
	// Same as response.
	Option<ClusterMetaDataResponse> sendClusterMetadataRequest();
	Option<ProduceResponse> sendProduceRequest(const std::string& topic, int partition, const RecordBatch& record_batch);
	Option<BrokerRegistrationResponse> sendBrokerRegistrationRequest(int broker_id, std::string broker_ip, uint16_t broker_port);

	bool sendAsyncFetchRequest(const FetchRequest& req);
	Option<FetchResponse> receiveAsyncFetchResponse();

};


#endif
