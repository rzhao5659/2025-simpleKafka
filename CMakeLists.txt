cmake_minimum_required(VERSION 3.10)
project(Kafka)

# Set C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# add_compile_options(-Wall -Wextra -Wpedantic)
add_compile_options(-w) # Removing warnings

# Set output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Include directories
include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/src/external)
include_directories(${CMAKE_SOURCE_DIR}/src/sockets)
include_directories(${CMAKE_SOURCE_DIR}/src/util)

# executable src files
set(SOCKET_SRC 
    src/sockets/Socket.cpp
    src/sockets/ClientSocket.cpp
    src/sockets/ClientStub.cpp
    src/sockets/ServerSocket.cpp
    src/sockets/ServerStub.cpp
)

set(ALL_SOURCES  # except mains.
    src/cluster_metadata.cpp
    src/consumer.cpp
    src/network_messages.cpp
    src/producer.cpp
    src/records.cpp
    src/broker.cpp
    src/util/debug_stream.cpp
    ${SOCKET_SRC}
)

set(PRODUCER_INT_SRC
    src/producer_interactive_main.cpp
    ${ALL_SOURCES}
)


set(CONTROLLER_SRC 
    src/controller_main.cpp     
    ${ALL_SOURCES}
)

set(BROKER_SRC 
    src/broker_main.cpp     
    ${ALL_SOURCES}
)

set(CONSUMER_SRC
    src/consumer_main.cpp
    ${ALL_SOURCES}
)



# executables
add_executable(producer ${PRODUCER_INT_SRC})
add_executable(controller ${CONTROLLER_SRC})
add_executable(broker ${BROKER_SRC})
add_executable(consumer ${CONSUMER_SRC})


