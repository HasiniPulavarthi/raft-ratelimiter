cmake_minimum_required(VERSION 3.14)
project(raft_ratelimiter CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

add_executable(raft_ratelimiter
    src/raft_node.cpp
    src/main.cpp
)

target_include_directories(raft_ratelimiter PRIVATE include)
target_link_libraries(raft_ratelimiter PRIVATE Threads::Threads)

if(NOT MSVC)
    target_compile_options(raft_ratelimiter PRIVATE -Wall -Wextra -O2)
endif()
