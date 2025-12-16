
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

// Runs for 30s
void producerThread(int producer_id, const std::string& broker_ip, int broker_port,
    const std::string& topic, int interval_ms) {

    KafkaProducer producer(producer_id, broker_ip, broker_port);

    std::string key = "producer_" + std::to_string(producer_id);
    int counter = 0;

    std::vector<std::string> batch;
    batch.reserve(10);

    const auto start = std::chrono::steady_clock::now();
    const auto duration = std::chrono::seconds(30);

    while (std::chrono::steady_clock::now() - start < duration) {
        batch.emplace_back(std::to_string(counter++));

        // Every 10 records send it as batch
        if (batch.size() == 10) {
            if (producer.send_batch(topic, key, batch)) {
                total_sent += batch.size();
            }
            batch.clear();
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(interval_ms)
        );
    }

    // Flush remaining records (if any)
    if (!batch.empty()) {
        if (producer.send_batch(topic, key, batch)) {
            total_sent += batch.size();
        }
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

    std::vector<std::thread> producers;
    for (int i = 0; i < N; i++) {
        int producer_id = 100 + i;
        producers.emplace_back(producerThread, producer_id, broker_ip, broker_port, topic, P);
    }

    for (auto& t : producers) {
        t.join();
    }

    // Print throughput
    double throughput = total_sent / 30.0;
    std::cout << "Producer Throughput: " << throughput << " msg/s" << std::endl;

    return 0;
}