cmake_minimum_required(VERSION 3.16)
project(StreamingPipeline LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_library(pipeline_core STATIC src/broker.cpp)
target_include_directories(pipeline_core PUBLIC include)

add_executable(pipeline src/main.cpp)
target_link_libraries(pipeline PRIVATE pipeline_core Threads::Threads)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(pipeline PRIVATE -O3 -march=native -Wall -Wextra)
endif()

enable_testing()

add_executable(pipeline_tests tests/test_broker.cpp src/broker.cpp)
target_include_directories(pipeline_tests PRIVATE include)
target_link_libraries(pipeline_tests PRIVATE Threads::Threads)
add_test(NAME unit COMMAND pipeline_tests)
