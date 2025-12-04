#ifndef __SOCKET_H__
#define __SOCKET_H__

#include <string>

#include <sys/poll.h>
#include <sys/select.h>

#define NAGLE_ON	0
#define NAGLE_OFF	1


/**
 * @brief A helper class for sending/receiving over a given socket (fd_).
 * Assumes the given fd_ is already initialized.
 *
 *
 */
class Socket {
protected:
	int fd_;

private:
	int nagle_;

public:
	Socket();
	virtual ~Socket();

	int Send(char* buffer, int size, int flags = 0);
	int Recv(char* buffer, int size, int flags = 0);

	int NagleOn(bool on_off);
	bool IsNagleOn();

	bool IsFdValid();

	void Close();

	// Delete copy constructor/assignment
	Socket(const Socket&) = delete;
	Socket& operator=(const Socket&) = delete;

	// Move constructor
	Socket(Socket&& other) noexcept
		: fd_(other.fd_),
		nagle_(other.nagle_)
	{
		other.fd_ = -1;
	}

	int getFd() {
		return fd_;
	}
};


#endif // end of #ifndef __SOCKET_H__
