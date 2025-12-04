#include "ClientStub.hpp"
#include <arpa/inet.h>


bool ClientStub::connect(const std::string& ip, int port) {
	if (socket_.IsFdValid()) {
		return false;
	}
	return socket_.Init(ip, port);
}

Option<ClusterMetaDataResponse> ClientStub::sendClusterMetadataRequest() {
	ClusterMetaDataRequest req;
	req.setRequesterId(client_id);
	int req_length = req.getLength();

	Option<ClusterMetaDataResponse> resp;

	// Send request's length then itself
	int net_req_length = htonl(req_length);
	if (!socket_.Send((char*)&net_req_length, sizeof(net_req_length))) {
		return resp;
	}

	// Send request itself
	req.marshal(buffer_);
	if (socket_.Send(buffer_, req_length)) {
		// Receive response's length
		int net_resp_length;
		if (socket_.Recv((char*)&net_resp_length, sizeof(net_resp_length))) {
			int resp_length = ntohl(net_resp_length);

			if (socket_.Recv(buffer_, resp_length)) {
				ClusterMetaDataResponse response;

				// Receive response itself
				response.unmarshal(buffer_);
				resp.setValue(response);
			}
		}
	}
	return resp;
}

Option<BrokerRegistrationResponse> ClientStub::sendBrokerRegistrationRequest(int broker_id, std::string broker_ip, uint16_t broker_port) {
	BrokerRegistrationRequest req;
	req.setRequesterId(broker_id);
	req.ip = broker_ip;
	req.port = broker_port;
	int req_length = req.getLength();

	Option<BrokerRegistrationResponse> resp;

	// Send request's length then itself
	int net_req_length = htonl(req_length);
	if (!socket_.Send((char*)&net_req_length, sizeof(net_req_length))) {
		return resp;
	}

	// Send request itself
	req.marshal(buffer_);
	if (socket_.Send(buffer_, req_length)) {
		// Receive response's length
		int net_resp_length;
		if (socket_.Recv((char*)&net_resp_length, sizeof(net_resp_length))) {
			int resp_length = ntohl(net_resp_length);

			if (socket_.Recv(buffer_, resp_length)) {
				BrokerRegistrationResponse response;
				response.unmarshal(buffer_);
				resp.setValue(response);
			}
		}
	}
	return resp;
}




Option<ProduceResponse> ClientStub::sendProduceRequest(const std::string& topic, int partition, const RecordBatch& record_batch) {
	ProduceRequest req;
	req.setRequesterId(client_id);
	req.topic = topic;
	req.partition = partition;
	req.record_batch = record_batch;
	int req_length = req.getLength();

	Option<ProduceResponse> resp;

	// Send request's length then itself
	int net_req_length = htonl(req_length);
	if (!socket_.Send((char*)&net_req_length, sizeof(net_req_length))) {
		return resp;
	}

	// Send request itself
	req.marshal(buffer_);
	if (socket_.Send(buffer_, req_length)) {
		// Receive response's length
		int net_resp_length;
		if (socket_.Recv((char*)&net_resp_length, sizeof(net_resp_length))) {
			int resp_length = ntohl(net_resp_length);
			// Receive response itself
			if (socket_.Recv(buffer_, resp_length)) {
				ProduceResponse response;
				response.unmarshal(buffer_);
				resp.setValue(response);
			}
		}
	}
	return resp;
}


bool ClientStub::sendAsyncFetchRequest(const FetchRequest& req) {
	// Send request's length
	int req_length = req.getLength();
	int net_req_length = htonl(req_length);
	if (!socket_.Send((char*)&net_req_length, sizeof(net_req_length))) {
		return false;
	}

	// Send request 
	req.marshal(buffer_);
	if (!socket_.Send(buffer_, req_length)) {
		return false;
	}
	return true;
}


Option<FetchResponse> ClientStub::receiveAsyncFetchResponse() {
	Option<FetchResponse> resp;

	// Receive response's length
	int net_resp_length;
	if (socket_.Recv((char*)&net_resp_length, sizeof(net_resp_length))) {
		int resp_length = ntohl(net_resp_length);
		// Receive response itself
		if (socket_.Recv(buffer_, resp_length)) {
			FetchResponse response;
			response.unmarshal(buffer_);
			resp.setValue(response);
		}
	}

	return resp;
}
