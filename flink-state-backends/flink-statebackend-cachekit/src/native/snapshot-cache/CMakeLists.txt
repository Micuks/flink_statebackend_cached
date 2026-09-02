cmake_minimum_required(VERSION 3.16)
project(cachekit_snapshot_native LANGUAGES CXX)

option(CACHEKIT_FORCE_FNV_HASH "Force the portable FNV hash for A/B benchmarks" OFF)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(cachekit_snapshot_core STATIC
    src/byte_snapshot_table.cc
)
set_target_properties(cachekit_snapshot_core PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(cachekit_snapshot_core PUBLIC include)
target_compile_options(cachekit_snapshot_core PRIVATE -O3 -Wall -Wextra -Werror)
if(CACHEKIT_FORCE_FNV_HASH)
    target_compile_definitions(cachekit_snapshot_core PRIVATE CACHEKIT_FORCE_FNV_HASH=1)
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
    target_compile_definitions(cachekit_snapshot_core PUBLIC
        CACHEKIT_ENABLE_KUNPENG_SNAPSHOT=1)
    target_compile_options(cachekit_snapshot_core PRIVATE -march=armv8.2-a -mtune=tsv110)
else()
    target_compile_definitions(cachekit_snapshot_core PUBLIC
        CACHEKIT_ENABLE_KUNPENG_SNAPSHOT=0)
endif()

find_package(JNI REQUIRED)
add_library(cachekit_snapshot_jni SHARED src/snapshot_jni.cc)
target_include_directories(cachekit_snapshot_jni PRIVATE ${JNI_INCLUDE_DIRS})
target_link_libraries(cachekit_snapshot_jni PRIVATE cachekit_snapshot_core)
target_compile_options(cachekit_snapshot_jni PRIVATE -O3 -Wall -Wextra -Werror)

enable_testing()
add_executable(cachekit_byte_snapshot_test test/byte_snapshot_table_test.cc)
target_link_libraries(cachekit_byte_snapshot_test PRIVATE cachekit_snapshot_core)
target_compile_options(cachekit_byte_snapshot_test PRIVATE -O2 -Wall -Wextra -Werror)
add_test(NAME cachekit_byte_snapshot_test COMMAND cachekit_byte_snapshot_test)
