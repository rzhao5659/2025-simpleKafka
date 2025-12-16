
// Program that spawns N consumers, consuming the same topic T (all partitions).  There is a flag P that determines consumer behavior.
// If P is true, each consumer append to a file all records received in a folder F, file name being "consumer_<id>.txt". Each line in the file is comma separated row: Topic, Partition, Key, Value. 
// Otherwise, each consumer prints out in console each row (Topic, Partition, Key, Value)
// This program measures and print throughput every 10s.
#include "consumer.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>
#include <fstream>
#include <csignal>

std::atomic<int> total_consumed(0);
std::atomic<bool> shutdown_requested(false);


// Runs for 10s
void consumerThread(int consumer_id, const std::string& broker_ip, int broker_port,
    const std::string& topic, bool print_to_file, const std::string& folder) {

    KafkaConsumer consumer(consumer_id, 10, 4096);
    consumer.fetchClusterMetadata(broker_ip, broker_port);
    bool success = consumer.subscribe(topic, true, false);
    if (!success) {
        std::cerr << "[Consumer " << consumer_id << "] Failed to subscribe" << std::endl;
        return;
    }

    std::ofstream outfile;
    if (print_to_file) {
        std::string filename = folder + "/consumer_" + std::to_string(consumer_id) + ".txt";
        outfile.open(filename);
    }

    const auto start = std::chrono::steady_clock::now();
    const auto duration = std::chrono::seconds(10);

    while (std::chrono::steady_clock::now() - start < duration) {
        std::vector<ConsumerRecordBatch> record_batches = consumer.poll(1);

        for (const auto& consumer_rb : record_batches) {
            const RecordBatch& rb = consumer_rb.record_batch;

            if (rb.getNumRecords() != 0) {
                for (const auto& record : rb.records) {
                    if (print_to_file) {
                        outfile << consumer_rb.topic << ","
                            << consumer_rb.partition << ","
                            << record.key << ","
                            << record.value << std::endl;
                    } else {
                        std::cout << "[Consumer " << consumer_id << "] "
                            << consumer_rb.topic << ","
                            << consumer_rb.partition << ","
                            << record.key << ","
                            << record.value << std::endl;
                    }
                    total_consumed++;
                }
            }
        }
    }

    if (print_to_file) {
        outfile.close();
    }
}



int main(int argc, char* argv[]) {
    signal(SIGPIPE, SIG_IGN);

    if (argc != 7) {
        std::cout << "Usage: " << argv[0] << " <N_consumers> <topic> <print_to_file> <folder> <broker_ip> <broker_port>" << std::endl;
        std::cout << "\nExample:" << std::endl;
        std::cout << "  " << argv[0] << " 3 test-topic true output 127.0.0.1 9001" << std::endl;
        std::cout << "  " << argv[0] << " 3 test-topic false . 127.0.0.1 9001" << std::endl;
        return 1;
    }

    int N = std::atoi(argv[1]);
    std::string topic = argv[2];
    bool print_to_file = (std::string(argv[3]) == "true");
    std::string folder = argv[4];
    std::string broker_ip = argv[5];
    int broker_port = std::atoi(argv[6]);

    std::cout << "========================================" << std::endl;
    std::cout << "Kafka Multi-Consumer Benchmark" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Consumers: " << N << std::endl;
    std::cout << "Topic: " << topic << std::endl;
    std::cout << "Broker: " << broker_ip << ":" << broker_port << std::endl;
    if (print_to_file) {
        std::cout << "Output: Files in " << folder << "/" << std::endl;
    } else {
        std::cout << "Output: Console" << std::endl;
    }
    std::cout << "========================================\n" << std::endl;

    std::vector<std::thread> consumers;
    for (int i = 0; i < N; i++) {
        consumers.emplace_back(consumerThread, 200 + i, broker_ip, broker_port,
            topic, print_to_file, folder);
    }

    for (auto& t : consumers) {
        t.join();
    }

    double throughput = total_consumed / 10.0;
    std::cout << "Consumer Throughput: " << throughput << " msg/s" << std::endl;
    return 0;
}