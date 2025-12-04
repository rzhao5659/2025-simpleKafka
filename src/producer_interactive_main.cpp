#include "producer.hpp"
#include <iostream>
#include <string>
#include <cstdlib>
#include <sstream>

void printUsage(const char* program_name) {
    std::cout << "Usage: " << program_name << " <producer_id> <broker_ip> <broker_port>" << std::endl;
    std::cout << "\nExample:" << std::endl;
    std::cout << "  " << program_name << " 100 127.0.0.1 9001" << std::endl;
    std::cout << "\nParameters:" << std::endl;
    std::cout << "  producer_id  - Unique ID for this producer (e.g., 100)" << std::endl;
    std::cout << "  broker_ip    - Bootstrap broker IP address (e.g., 127.0.0.1)" << std::endl;
    std::cout << "  broker_port  - Bootstrap broker port (e.g., 9001)" << std::endl;
}

int main(int argc, char* argv[]) {
    // Check for help flag
    if (argc > 1 && (std::string(argv[1]) == "-h" || std::string(argv[1]) == "--help")) {
        printUsage(argv[0]);
        return 0;
    }

    // Check arguments
    if (argc != 4) {
        std::cerr << "Error: Invalid number of arguments." << std::endl;
        printUsage(argv[0]);
        return 1;
    }

    // Parse arguments
    int producer_id = std::atoi(argv[1]);
    std::string broker_ip = argv[2];
    int broker_port = std::atoi(argv[3]);

    // Validate arguments
    if (broker_port <= 0 || broker_port > 65535) {
        std::cerr << "Error: Invalid broker port: " << broker_port << std::endl;
        return 1;
    }

    try {
        std::cout << "========================================" << std::endl;
        std::cout << "Kafka Producer" << std::endl;
        std::cout << "========================================" << std::endl;
        std::cout << "Producer ID: " << producer_id << std::endl;
        std::cout << "Bootstrap Broker: " << broker_ip << ":" << broker_port << std::endl;
        std::cout << "========================================\n" << std::endl;

        std::cout << "Connecting to broker..." << std::endl;
        KafkaProducer producer(producer_id, broker_ip, broker_port);
        std::cout << "Connected!\n" << std::endl;

        // Interactive mode
        std::cout << "Commands:" << std::endl;
        std::cout << "  send <topic> <key> <value>           - Send single message" << std::endl;
        std::cout << "  batch <topic> <key> <val1,val2,...>  - Send batch of messages" << std::endl;
        std::cout << "  quit\n" << std::endl;

        std::string command;
        while (true) {
            std::cout << "producer> ";
            std::cin >> command;

            if (command == "quit" || command == "exit") {
                std::cout << "Exiting..." << std::endl;
                break;
            }

            if (command == "send") {
                std::string topic, key, value;
                std::cin >> topic >> key;
                std::cin.ignore();
                std::getline(std::cin, value);

                std::cout << "Sending single message..." << std::endl;
                bool success = producer.send(topic, key, value);

                if (success) {
                    std::cout << "✓ Sent!" << std::endl;
                } else {
                    std::cout << "✗ Failed" << std::endl;
                }
            } else if (command == "batch") {
                std::string topic, key, values_str;
                std::cin >> topic >> key;
                std::cin.ignore();
                std::getline(std::cin, values_str);

                // Parse comma-separated values
                std::vector<std::string> values;
                std::stringstream ss(values_str);
                std::string value;

                while (std::getline(ss, value, ',')) {
                    // Trim leading/trailing whitespace
                    size_t start = value.find_first_not_of(" \t");
                    size_t end = value.find_last_not_of(" \t");
                    if (start != std::string::npos) {
                        values.push_back(value.substr(start, end - start + 1));
                    }
                }

                if (values.empty()) {
                    std::cout << "✗ No values provided" << std::endl;
                    continue;
                }

                std::cout << "Sending batch of " << values.size() << " messages..." << std::endl;
                bool success = producer.send_batch(topic, key, values);

                if (success) {
                    std::cout << "✓ Batch sent!" << std::endl;
                } else {
                    std::cout << "✗ Failed" << std::endl;
                }
            } else {
                std::cout << "Unknown command." << std::endl;
                std::cout << "Type 'send <topic> <key> <value>' or 'batch <topic> <key> <val1,val2,...>' or 'quit'" << std::endl;
            }
        }

    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}