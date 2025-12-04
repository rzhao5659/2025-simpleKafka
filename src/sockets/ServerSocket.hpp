#ifndef __SERVERSOCKET_H__
#define __SERVERSOCKET_H__

#include <memory>

#include "Socket.hpp"

class ServerSocket : public Socket {
public:
	ServerSocket() {}
	~ServerSocket() {}

	ServerSocket(int fd, bool nagle_on = NAGLE_ON);

	/**
	 * @brief Construct a socket that listens on the given port.
	 * Should be called if default constructed.
	 */
	bool Init(int port);


	/**
	 * @brief Return a socket that handles the accepted connection.
	 */
	std::unique_ptr<ServerSocket> Accept();

};


#endif // end of #ifndef __SERVERSOCKET_H__
