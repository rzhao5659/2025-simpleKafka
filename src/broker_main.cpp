#include "broker.hpp"
#include <iostream>
#include <cstdlib>
#include <sys/stat.h>
#include <thread>
#include <signal.h>  
#include <atomic>   

std::atomic<bool> shutdown_requested(false);

void signal_handler(int signal) {
    std::cout << "\nReceived signal " << signal << ", shutting down gracefully..." << std::endl;
    shutdown_requested = true;
}

void printUsage(const char* program_name) {
    std::cout << "Usage: " << program_name << " <broker_id> <broker_ip> <broker_port> "
        << "<controller_id> <controller_ip> <controller_port> <log_folder_path> <debug>"
        << std::endl;
    std::cout << "\nExample:" << std::endl;
    std::cout << "  " << program_name << " 1 127.0.0.1 9001 0 127.0.0.1 8000 ./broker1_logs/ 1" << std::endl;
    std::cout << "\nParameters:" << std::endl;
    std::cout << "  broker_id         - Unique ID for this broker (e.g., 1)" << std::endl;
    std::cout << "  broker_ip         - IP address for this broker (e.g., 127.0.0.1)" << std::endl;
    std::cout << "  broker_port       - Port to listen for client connections (e.g., 9001)" << std::endl;
    std::cout << "  controller_id     - Controller's ID (e.g., 0)" << std::endl;
    std::cout << "  controller_ip     - Controller's IP address (e.g., 127.0.0.1)" << std::endl;
    std::cout << "  controller_port   - Controller's port (e.g., 8000)" << std::endl;
    std::cout << "  log_folder_path   - Directory to store topic-partition logs (e.g., ./broker1_logs/)" << std::endl;
    std::cout << "  debug             - Enable periodic 5s print assigned topic partition state" << std::endl;
}

bool createDirectoryIfNotExists(const std::string& path) {
    struct stat info;

    if (stat(path.c_str(), &info) != 0) {
        // Directory doesn't exist, try to create it
        if (mkdir(path.c_str(), 0755) != 0) {
            return false;
        }
        std::cout << "Created log directory: " << path << std::endl;
    } else if (!(info.st_mode & S_IFDIR)) {
        // Path exists but is not a directory
        return false;
    }

    return true;
}

int main(int argc, char* argv[]) {

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);


    // Check arguments
    if (argc != 9) {
        std::cerr << "Error: Invalid number of arguments." << std::endl;
        printUsage(argv[0]);
        return 1;
    }

    // Parse arguments
    int broker_id = std::atoi(argv[1]);
    std::string broker_ip = argv[2];
    int listen_port = std::atoi(argv[3]);
    int controller_id = std::atoi(argv[4]);
    std::string controller_ip = argv[5];
    uint16_t controller_port = static_cast<uint16_t>(std::atoi(argv[6]));
    std::string log_folder_path = argv[7];
    bool debug = std::atoi(argv[8]);

    // Ensure log folder path ends with '/'
    if (!log_folder_path.empty() && log_folder_path.back() != '/') {
        log_folder_path += '/';
    }

    // Create log directory if it doesn't exist
    if (!createDirectoryIfNotExists(log_folder_path)) {
        std::cerr << "Error: Failed to create or access log directory: " << log_folder_path << std::endl;
        return 1;
    }


    std::cout << "========================================" << std::endl;
    std::cout << "Starting Kafka Broker" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Broker ID: " << broker_id << std::endl;
    std::cout << "Broker IP: " << broker_ip << std::endl;
    std::cout << "Broker Port: " << listen_port << std::endl;
    std::cout << "Controller ID: " << controller_id << std::endl;
    std::cout << "Controller Address: " << controller_ip << ":" << controller_port << std::endl;
    std::cout << "Log Folder: " << log_folder_path << std::endl;
    std::cout << "Debug print: " << debug << std::endl;
    std::cout << "========================================\n" << std::endl;

    // Create broker with new constructor signature
    Broker broker(
        broker_id,
        broker_ip,
        static_cast<uint16_t>(listen_port),
        controller_id,
        controller_ip,
        controller_port,
        log_folder_path,
        debug
    );

    std::thread listen_thread([&broker]() {
        broker.listenForClientConnections();
        });

    // Wait for shutdown signal
    while (!shutdown_requested) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "Shutting down broker..." << std::endl;
    broker.shutdown();  // Call shutdown method
    listen_thread.detach();
    return 0;
}