# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.16)
project(cachekit_native_request_plane LANGUAGES CXX)

option(CACHEKIT_NATIVE_BUILD_TESTS "Build native request-plane tests" ON)
option(CACHEKIT_NATIVE_BUILD_JNI "Build the opt-in JNI bridge" OFF)
option(CACHEKIT_NATIVE_ENABLE_SANITIZERS "Enable ASan and UBSan" OFF)

set(CACHEKIT_NATIVE_SOURCES
    src/c_api.cpp
    src/kernels_scalar.cpp
    src/request_plane.cpp
    src/runtime_features.cpp)

string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" CACHEKIT_SYSTEM_PROCESSOR)
if(CACHEKIT_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
    list(APPEND CACHEKIT_NATIVE_SOURCES
         src/kernels_aarch64_neon_crc.cpp
         src/kernels_aarch64_sve256.cpp)
    set_source_files_properties(
        src/kernels_aarch64_neon_crc.cpp
        PROPERTIES COMPILE_OPTIONS "-march=armv8-a+crc")
    set_source_files_properties(
        src/kernels_aarch64_sve256.cpp
        PROPERTIES COMPILE_OPTIONS "-march=armv8.2-a+sve+crc;-msve-vector-bits=256")
endif()

add_library(cachekit_native_request_plane STATIC ${CACHEKIT_NATIVE_SOURCES})
target_compile_features(cachekit_native_request_plane PUBLIC cxx_std_17)
target_include_directories(
    cachekit_native_request_plane
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src")
target_compile_options(
    cachekit_native_request_plane
    PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
if(CACHEKIT_NATIVE_BUILD_JNI)
    set_target_properties(
        cachekit_native_request_plane PROPERTIES POSITION_INDEPENDENT_CODE ON)
endif()

if(CACHEKIT_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
    target_compile_definitions(
        cachekit_native_request_plane PRIVATE CACHEKIT_NATIVE_AARCH64_KERNELS=1)
endif()

if(CACHEKIT_NATIVE_ENABLE_SANITIZERS)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(
            cachekit_native_request_plane PRIVATE
            -fsanitize=address,undefined -fno-omit-frame-pointer)
        target_link_options(
            cachekit_native_request_plane PRIVATE
            -fsanitize=address,undefined -fno-omit-frame-pointer)
    else()
        message(FATAL_ERROR "Sanitizers require GCC or Clang")
    endif()
endif()

if(CACHEKIT_NATIVE_BUILD_TESTS OR CACHEKIT_NATIVE_BUILD_JNI)
    add_library(
        cachekit_native_batch_codec STATIC src/jni_batch_codec.cpp)
    target_compile_features(cachekit_native_batch_codec PUBLIC cxx_std_17)
    target_include_directories(
        cachekit_native_batch_codec
        PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src")
    target_compile_options(
        cachekit_native_batch_codec
        PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
    target_link_libraries(
        cachekit_native_batch_codec PUBLIC cachekit_native_request_plane)
    if(CACHEKIT_NATIVE_BUILD_JNI)
        set_target_properties(
            cachekit_native_batch_codec PROPERTIES POSITION_INDEPENDENT_CODE ON)
    endif()
    if(CACHEKIT_NATIVE_ENABLE_SANITIZERS AND
       CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(
            cachekit_native_batch_codec PRIVATE
            -fsanitize=address,undefined -fno-omit-frame-pointer)
        target_link_options(
            cachekit_native_batch_codec PRIVATE
            -fsanitize=address,undefined -fno-omit-frame-pointer)
    endif()
endif()

if(CACHEKIT_NATIVE_BUILD_JNI)
    find_package(JNI REQUIRED)
    add_library(
        cachekit_native_request_plane_jni SHARED src/jni_bridge.cpp)
    target_compile_features(
        cachekit_native_request_plane_jni PRIVATE cxx_std_17)
    target_include_directories(
        cachekit_native_request_plane_jni PRIVATE ${JNI_INCLUDE_DIRS})
    target_compile_options(
        cachekit_native_request_plane_jni
        PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
    target_link_libraries(
        cachekit_native_request_plane_jni PRIVATE cachekit_native_batch_codec)
    set_target_properties(
        cachekit_native_request_plane_jni
        PROPERTIES OUTPUT_NAME "cachekit_native_request_plane_jni")
    if(CACHEKIT_NATIVE_ENABLE_SANITIZERS AND
       CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(
            cachekit_native_request_plane_jni PRIVATE
            -fsanitize=address,undefined -fno-omit-frame-pointer)
        target_link_options(
            cachekit_native_request_plane_jni PRIVATE
            -fsanitize=address,undefined -fno-omit-frame-pointer)
    endif()
endif()

if(CACHEKIT_NATIVE_BUILD_TESTS)
    include(CTest)
    add_executable(
        cachekit_native_request_plane_test tests/native_request_plane_test.cpp)
    target_compile_features(cachekit_native_request_plane_test PRIVATE cxx_std_17)
    target_include_directories(
        cachekit_native_request_plane_test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src")
    target_compile_options(
        cachekit_native_request_plane_test
        PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
    target_link_libraries(
        cachekit_native_request_plane_test PRIVATE cachekit_native_request_plane)
    if(CACHEKIT_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
        target_compile_definitions(
            cachekit_native_request_plane_test
            PRIVATE CACHEKIT_NATIVE_AARCH64_KERNELS=1)
    endif()
    if(CACHEKIT_NATIVE_ENABLE_SANITIZERS AND
       CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(
            cachekit_native_request_plane_test PRIVATE
            -fsanitize=address,undefined -fno-omit-frame-pointer)
        target_link_options(
            cachekit_native_request_plane_test PRIVATE
            -fsanitize=address,undefined -fno-omit-frame-pointer)
    endif()
    add_test(
        NAME cachekit_native_request_plane_test
        COMMAND cachekit_native_request_plane_test)

    add_executable(
        cachekit_native_batch_codec_test tests/jni_batch_codec_test.cpp)
    target_compile_features(
        cachekit_native_batch_codec_test PRIVATE cxx_std_17)
    target_compile_options(
        cachekit_native_batch_codec_test
        PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
    target_link_libraries(
        cachekit_native_batch_codec_test PRIVATE cachekit_native_batch_codec)
    if(CACHEKIT_NATIVE_ENABLE_SANITIZERS AND
       CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(
            cachekit_native_batch_codec_test PRIVATE
            -fsanitize=address,undefined -fno-omit-frame-pointer)
        target_link_options(
            cachekit_native_batch_codec_test PRIVATE
            -fsanitize=address,undefined -fno-omit-frame-pointer)
    endif()
    add_test(
        NAME cachekit_native_batch_codec_test
        COMMAND cachekit_native_batch_codec_test)
endif()
