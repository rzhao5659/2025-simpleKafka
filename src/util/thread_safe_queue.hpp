
#ifndef __THREADSAFEQUEUE_H__
#define __THREADSAFEQUEUE_H__

#include <mutex>
#include <condition_variable>
#include <queue>
#include <chrono>
template<typename T>
class ThreadSafeQueue {
    std::queue<T> q;
    std::condition_variable new_entry_cv;
    std::mutex mtx;

public:
    bool empty() {
        std::unique_lock<std::mutex> lk(mtx);
        return q.empty();
    }

    bool try_pop(int timeout_s, T& result) {
        std::unique_lock<std::mutex> lk(mtx);
        auto sleep_until_time = std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
        bool not_timedout = new_entry_cv.wait_until(lk, sleep_until_time, [this] {return !q.empty();});
        if (!not_timedout) {
            return false;
        }

        result = std::move(q.front());
        q.pop();
        return true;
    }

    template <typename U>
    void push(U&& entry) {
        std::unique_lock<std::mutex> lk(mtx);
        q.push(std::forward<U>(entry));
        new_entry_cv.notify_one();
    }

    int size() {
        std::unique_lock<std::mutex> lk(mtx);
        return q.size();
    }
};

#endif