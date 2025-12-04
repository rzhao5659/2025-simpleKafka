#include "Socket.hpp"

#include <iostream>

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/ioctl.h>
#include <sys/types.h>

Socket::Socket() : fd_(-1), nagle_(NAGLE_ON) {}
Socket::~Socket() {
	Close();
}

bool Socket::IsFdValid() {
	return fd_ >= 0;
}

int Socket::Send(char* buffer, int size, int flags) {
	if (fd_ < 0) {
		return 0;
	}

	int bytes_written = 0;
	int offset = 0;
	while (size > 0) {
		bytes_written = send(fd_, buffer + offset, size, flags);
		if (bytes_written < 0) {
			/* This only occur for nonblocking send.
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				perror("ERROR: send retry");
				continue;
			}
			*/
			perror("ERROR: failed to send");
			Close();
			return 0;
		}
		size -= bytes_written;
		offset += bytes_written;
		assert(size >= 0);
	}
	return 1;
}

int Socket::Recv(char* buffer, int size, int flags) {
	if (fd_ < 0) {
		return 0;
	}

	int bytes_read = 0;
	int offset = 0;
	while (size > 0) {
		bytes_read = recv(fd_, buffer + offset, size, flags);
		if (bytes_read <= 0) {
			/*
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				//perror("ERROR: recv retry");
				continue;
			}
			*/
			perror("ERROR: failed to recv");
			Close();
			return 0;
		}
		assert(bytes_read != 0);

		size -= bytes_read;
		offset += bytes_read;
		assert(size >= 0);
	}
	return 1;
}

int Socket::NagleOn(bool on_off) {
	if (fd_ < 0) {
		return 0;
	}

	nagle_ = (on_off ? NAGLE_ON : NAGLE_OFF);
	int result = setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY,
		(void*)&nagle_, sizeof(int));
	if (result < 0) {
		perror("ERROR: setsockopt failed");
		return 0;
	}
	return 1;
}

bool Socket::IsNagleOn() {
	return (nagle_ == NAGLE_ON) ? true : false;
}

void Socket::Close() {
	if (fd_ < 0) {
		return;
	}
	shutdown(fd_, SHUT_RDWR);
	close(fd_);
	fd_ = -1;
	//perror("Socket closed");
}





