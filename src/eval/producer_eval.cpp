
// Program that spawns N producers, writing to the same topic T.  Each producer uses its own ID as key ("producer_<id>"), and values are monotonously increasing.
// Each producer sends at interval of P ms. 
// This program measures and print throughput every 10s.
#include "producer.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>

std::atomic<int> total_sent(0);

void producerThread(int producer_id, const std::string& broker_ip, int broker_port,
    const std::string& topic, int interval_ms) {
    try {
        KafkaProducer producer(producer_id, broker_ip, broker_port);

        std::string key = "producer_" + std::to_string(producer_id);
        int counter = 0;

        while (true) {
            std::string value = std::to_string(counter++);

            if (producer.send(topic, key, value)) {
                total_sent++;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
        }
    }
    catch (const std::exception& e) {
        std::cerr << "[Producer " << producer_id << "] Error: " << e.what() << std::endl;
    }
}

void statsThread() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(10));

        int count = total_sent.exchange(0);
        double throughput = count / 10.0;

        auto now = std::chrono::system_clock::now();
        auto timestamp = std::chrono::system_clock::to_time_t(now);

        std::cout << "[" << timestamp << "] Throughput: " << throughput << " msg/s" << std::endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc != 6) {
        std::cout << "Usage: " << argv[0] << " <N_producers> <P_interval_ms> <topic> <broker_ip> <broker_port>" << std::endl;
        std::cout << "Example: " << argv[0] << " 5 100 test-topic 127.0.0.1 9001" << std::endl;
        return 1;
    }

    int N = std::atoi(argv[1]);
    int P = std::atoi(argv[2]);
    std::string topic = argv[3];
    std::string broker_ip = argv[4];
    int broker_port = std::atoi(argv[5]);

    std::cout << "Spawning " << N << " producers, topic: " << topic
        << ", interval: " << P << " ms" << std::endl;

    std::thread stats(statsThread);
    stats.detach();

    std::vector<std::thread> producers;
    for (int i = 0; i < N; i++) {
        int producer_id = 100 + i;
        producers.emplace_back(producerThread, producer_id, broker_ip, broker_port, topic, P);
    }

    for (auto& t : producers) {
        t.join();
    }

    return 0;
}