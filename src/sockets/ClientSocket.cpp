#include <string.h>
#include <arpa/inet.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/types.h>

#include "ClientSocket.hpp"

int ClientSocket::Init(std::string ip, int port) {
	if (IsFdValid()) {
		return 0;
	}
	struct sockaddr_in addr;
	fd_ = socket(AF_INET, SOCK_STREAM, 0);
	if (fd_ < 0) {
		// perror("ERROR: failed to create a socket");
		return 0;
	}

	memset(&addr, '\0', sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = inet_addr(ip.c_str());
	addr.sin_port = htons(port);

	if ((connect(fd_, (struct sockaddr*)&addr, sizeof(addr))) < 0) {
		// perror("ERROR: failed to connect");
		fd_ = -1;
		return 0;
	}
	return 1;
}