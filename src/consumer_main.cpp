#include "consumer.hpp"
#include <iostream>
#include <string>
#include <cstdlib>
#include <csignal>
#include <thread>
#include <chrono>
#include <atomic> 


std::atomic<bool> shutdown_requested(false);

void printUsage(const char* program_name) {
    std::cout << "Usage: " << program_name << " <consumer_id> <broker_ip> <broker_port> <topic>" << std::endl;
    std::cout << "\nExample:" << std::endl;
    std::cout << "  " << program_name << " 200 127.0.0.1 9001 orders" << std::endl;
    std::cout << "\nParameters:" << std::endl;
    std::cout << "  consumer_id  - Unique ID for this consumer (e.g., 200)" << std::endl;
    std::cout << "  broker_ip    - Bootstrap broker IP address (e.g., 127.0.0.1)" << std::endl;
    std::cout << "  broker_port  - Bootstrap broker port (e.g., 9001)" << std::endl;
    std::cout << "  topic        - Topic to consume from (e.g., orders)" << std::endl;
}

void signal_handler(int signal) {
    std::cout << "\nShutting down consumer gracefully..." << std::endl;
    shutdown_requested = true;
}

int main(int argc, char* argv[]) {

    signal(SIGINT, signal_handler);   // Ctrl+C
    signal(SIGTERM, signal_handler);  // kill command

    // Check for help flag
    if (argc > 1 && (std::string(argv[1]) == "-h" || std::string(argv[1]) == "--help")) {
        printUsage(argv[0]);
        return 0;
    }

    // Check arguments
    if (argc != 5) {
        std::cerr << "Error: Invalid number of arguments." << std::endl;
        printUsage(argv[0]);
        return 1;
    }

    // Parse arguments
    int consumer_id = std::atoi(argv[1]);
    std::string broker_ip = argv[2];
    int broker_port = std::atoi(argv[3]);
    std::string topic = argv[4];

    std::cout << "========================================" << std::endl;
    std::cout << "Kafka Consumer" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Consumer ID: " << consumer_id << std::endl;
    std::cout << "Bootstrap Broker: " << broker_ip << ":" << broker_port << std::endl;
    std::cout << "Topic: " << topic << std::endl;
    std::cout << "========================================\n" << std::endl;

    // Create consumer
    KafkaConsumer consumer(consumer_id, 5, 4096);  // fetch_trigger_size=5, fetch_max_bytes=4096
    consumer.fetchClusterMetadata(broker_ip, broker_port);
    bool success = consumer.subscribe(topic, true, false);  // read_from_start=true, leader_only=false
    if (!success) {
        std::cerr << "Failed to subscribe to topic" << std::endl;
        return 1;
    }
    std::cout << "\nConsuming messages (Press Ctrl+C to stop)...\n" << std::endl;

    // Poll for messages
    try {
        while (!shutdown_requested) {
            std::vector<ConsumerRecordBatch> record_batches = consumer.poll(1);

            for (const auto& consumer_rb : record_batches) {
                const RecordBatch& rb = consumer_rb.record_batch;

                if (rb.getNumRecords() != 0) {
                    for (const auto& record : rb.records) {
                        std::cout << "Offset " << (rb.base_offset + record.rel_offset) <<
                            ": (" << record.key << "," << record.value << ")" << std::endl;
                    }
                }

            }

        }
    }
    catch (const std::exception& e) {
        std::cerr << "Consumer error: " << e.what() << std::endl;
        return 1;
    }



    return 0;
}