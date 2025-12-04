#ifndef __DEBUGSTREAM_H__
#define __DEBUGSTREAM_H__

#include <iostream>
#include <sstream>
#include <vector>

#define DEBUG 1 

class DebugStream : public std::ostream {
    bool enabled;
    std::streambuf* original_buf;

public:
    DebugStream(bool enabled = DEBUG, std::ostream& out = std::cout)
        : std::ostream(enabled ? out.rdbuf() : nullptr),
        enabled(enabled),
        original_buf(out.rdbuf()) {
    }

    void setEnabled(bool enable) {
        enabled = enable;
        if (enabled) {
            rdbuf(original_buf);
        } else {
            rdbuf(nullptr);
        }
    }
};

extern DebugStream debugstream;

// defining operator<< for vector
template<typename T>
std::ostream& operator<<(std::ostream& os, const std::vector<T>& vec) {
    os << "[";
    for (size_t i = 0; i < vec.size(); i++) {
        os << vec[i];
        if (i < vec.size() - 1) os << ", ";
    }
    os << "]";
    return os;
}

#endif