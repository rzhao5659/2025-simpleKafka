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

	// Grow buffer if needed
	if (buffer_.size() < req_length) {
		buffer_.resize(req_length);
	}

	// Send request itself
	req.marshal(buffer_.data());
	if (socket_.Send(buffer_.data(), req_length)) {
		// Receive response's length
		int net_resp_length;
		if (socket_.Recv((char*)&net_resp_length, sizeof(net_resp_length))) {
			int resp_length = ntohl(net_resp_length);

			// Grow buffer if needed.
			if (buffer_.size() < resp_length) {
				buffer_.resize(resp_length);
			}
			if (socket_.Recv(buffer_.data(), resp_length)) {
				ClusterMetaDataResponse response;

				// Receive response itself
				response.unmarshal(buffer_.data());
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
	// Grow buffer if needed.
	if (buffer_.size() < req_length) {
		buffer_.resize(req_length);
	}
	req.marshal(buffer_.data());
	if (socket_.Send(buffer_.data(), req_length)) {
		// Receive response's length
		int net_resp_length;
		if (socket_.Recv((char*)&net_resp_length, sizeof(net_resp_length))) {
			int resp_length = ntohl(net_resp_length);

			// Grow buffer if needed.
			if (buffer_.size() < resp_length) {
				buffer_.resize(resp_length);
			}

			if (socket_.Recv(buffer_.data(), resp_length)) {
				BrokerRegistrationResponse response;
				response.unmarshal(buffer_.data());
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
	// Grow buffer if needed.
	if (buffer_.size() < req_length) {
		buffer_.resize(req_length);
	}
	req.marshal(buffer_.data());
	if (socket_.Send(buffer_.data(), req_length)) {
		// Receive response's length
		int net_resp_length;
		if (socket_.Recv((char*)&net_resp_length, sizeof(net_resp_length))) {
			int resp_length = ntohl(net_resp_length);

			// Grow buffer if needed.
			if (buffer_.size() < resp_length) {
				buffer_.resize(resp_length);
			}

			// Receive response itself
			if (socket_.Recv(buffer_.data(), resp_length)) {
				ProduceResponse response;
				response.unmarshal(buffer_.data());
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

	// Grow buffer if needed.
	if (buffer_.size() < req_length) {
		buffer_.resize(req_length);
	}

	// Send request 
	req.marshal(buffer_.data());
	if (!socket_.Send(buffer_.data(), req_length)) {
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

		// Grow buffer if needed.
		if (buffer_.size() < resp_length) {
			buffer_.resize(resp_length);
		}

		// Receive response itself
		if (socket_.Recv(buffer_.data(), resp_length)) {
			FetchResponse response;
			response.unmarshal(buffer_.data());
			resp.setValue(response);
		}
	}

	return resp;
}
