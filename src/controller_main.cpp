#include "controller.hpp"
#include <iostream>
#include <cstdlib>

void printUsage(const char* program_name) {
    std::cout << "Usage: " << program_name << " <controller_id> <controller_ip> <listen_port> <num_brokers> "
        << "<topic_config_file> <metadata_log_file> <load_balance_leaders>" << std::endl;
    std::cout << "\nExample:" << std::endl;
    std::cout << "  " << program_name << " 0 127.0.0.1 8000 3 topics.json metadata.log 0" << std::endl;
    std::cout << "\nParameters:" << std::endl;
    std::cout << "  controller_id       - Unique ID for this controller (e.g., 0)" << std::endl;
    std::cout << "  controller_ip       - IP address for this controller (e.g., 127.0.0.1)" << std::endl;
    std::cout << "  listen_port         - Port to listen for broker connections (e.g., 8000)" << std::endl;
    std::cout << "  num_brokers         - Number of brokers expected to register (e.g., 3)" << std::endl;
    std::cout << "  topic_config_file   - JSON file with topic configurations (e.g., topics.json)" << std::endl;
    std::cout << "  metadata_log_file   - File to store metadata log (e.g., metadata.log)" << std::endl;
    std::cout << "  load_balance_leaders- 1 or 0 for eval purposes. " << std::endl;
}

int main(int argc, char* argv[]) {
    // Check arguments
    if (argc != 8) {
        std::cerr << "Error: Invalid number of arguments." << std::endl;
        printUsage(argv[0]);
        return 1;
    }

    // Parse arguments
    int controller_id = std::atoi(argv[1]);
    std::string controller_ip = argv[2];
    int listen_port = std::atoi(argv[3]);
    int num_brokers = std::atoi(argv[4]);
    std::string topic_config_file = argv[5];
    std::string metadata_log_file = argv[6];
    bool load_balance_leaders = std::atoi(argv[7]);

    int heartbeat_timeout_s = 5;
    int debug_print_period_s = 5;

    std::cout << "========================================" << std::endl;
    std::cout << "Starting Kafka Controller" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Controller ID: " << controller_id << std::endl;
    std::cout << "Controller IP: " << controller_ip << std::endl;
    std::cout << "Listen Port: " << listen_port << std::endl;
    std::cout << "Expected Brokers: " << num_brokers << std::endl;
    std::cout << "Heartbeat Timeout: " << heartbeat_timeout_s << "s (hardcoded)" << std::endl;
    std::cout << "Debug Print Period: " << debug_print_period_s << "s (hardcoded)" << std::endl;
    std::cout << "Topic Config File: " << topic_config_file << std::endl;
    std::cout << "Metadata Log File: " << metadata_log_file << std::endl;
    std::cout << "Load balance leaders: " << load_balance_leaders << std::endl;
    std::cout << "========================================\n" << std::endl;

    Controller controller(
        controller_id,
        controller_ip,
        listen_port,
        num_brokers,
        heartbeat_timeout_s,
        debug_print_period_s,
        topic_config_file,
        metadata_log_file,
        load_balance_leaders
    );

    // Start listening for broker connections (blocks indefinitely)
    controller.listenForBrokerConnections();

    return 0;
}