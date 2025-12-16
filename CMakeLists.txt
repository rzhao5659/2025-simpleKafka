cmake_minimum_required(VERSION 3.10)
project(Kafka)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -g")
# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
# add_compile_options(-Wall -Wextra -Wpedantic)
add_compile_options(-w) # Removing warnings

# Set output directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/src/external)
include_directories(${CMAKE_SOURCE_DIR}/src/sockets)
include_directories(${CMAKE_SOURCE_DIR}/src/util)

set(SOCKET_SRC 
    src/sockets/Socket.cpp
    src/sockets/ClientSocket.cpp
    src/sockets/ClientStub.cpp
    src/sockets/ServerSocket.cpp
    src/sockets/ServerStub.cpp
)

set(COMMON_SRC 
    src/cluster_metadata.cpp
    src/network_messages.cpp
    src/records.cpp
    src/util/debug_stream.cpp
    ${SOCKET_SRC}
)

set(PRODUCER_SRC
    src/producer.cpp
    ${COMMON_SRC}
)

set(BROKER_SRC 
    src/broker.cpp  
    src/consumer.cpp
    ${COMMON_SRC}
)

set(CONTROLLER_SRC 
    src/controller.cpp
    ${COMMON_SRC}
)

set(CONSUMER_SRC
    src/consumer.cpp
    ${COMMON_SRC}   
)


set(PRODUCER_MAIN_SRC
    src/producer_main.cpp
    ${PRODUCER_SRC}
)

set(CONTROLLER_MAIN_SRC 
    src/controller_main.cpp     
    ${CONTROLLER_SRC}
)

set(BROKER_MAIN_SRC 
    src/broker_main.cpp     
    ${BROKER_SRC}
)

set(CONSUMER_MAIN_SRC
    src/consumer_main.cpp
    ${CONSUMER_SRC}
)

set(PROD_EVAL_SRC
    src/eval/producer_eval.cpp
    ${PRODUCER_SRC}
)

set(CON_EVAL_SRC
    src/eval/consumer_eval.cpp
    ${CONSUMER_SRC}
)


# executables
add_executable(producer ${PRODUCER_MAIN_SRC})
add_executable(controller ${CONTROLLER_MAIN_SRC})
add_executable(broker ${BROKER_MAIN_SRC})
add_executable(consumer ${CONSUMER_MAIN_SRC})
add_executable(producer_eval ${PROD_EVAL_SRC})
add_executable(consumer_eval ${CON_EVAL_SRC})


