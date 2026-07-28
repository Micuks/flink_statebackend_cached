/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef CACHEKIT_NATIVE_REQUEST_PLANE_C_H_
#define CACHEKIT_NATIVE_REQUEST_PLANE_C_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CACHEKIT_NATIVE_DEFAULT_MIN_BATCH_SIZE 64U

typedef struct cachekit_native_plane cachekit_native_plane;

typedef enum cachekit_native_kernel_preference {
    CACHEKIT_NATIVE_KERNEL_AUTO = 0,
    CACHEKIT_NATIVE_KERNEL_SCALAR = 1,
    CACHEKIT_NATIVE_KERNEL_NEON_CRC = 2,
    CACHEKIT_NATIVE_KERNEL_SVE256 = 3
} cachekit_native_kernel_preference;

typedef enum cachekit_native_error {
    CACHEKIT_NATIVE_OK = 0,
    CACHEKIT_NATIVE_INVALID_ARGUMENT = 1,
    CACHEKIT_NATIVE_CAPACITY_EXCEEDED = 2,
    CACHEKIT_NATIVE_OVERFLOW = 3,
    CACHEKIT_NATIVE_UNSUPPORTED_KERNEL = 4,
    CACHEKIT_NATIVE_ALLOCATION_FAILED = 5,
    CACHEKIT_NATIVE_INTERNAL = 6
} cachekit_native_error;

typedef enum cachekit_native_probe_status {
    CACHEKIT_NATIVE_MISS = 0,
    CACHEKIT_NATIVE_HIT = 1,
    CACHEKIT_NATIVE_NEGATIVE = 2
} cachekit_native_probe_status;

typedef enum cachekit_native_fill_status {
    CACHEKIT_NATIVE_INSERTED = 0,
    CACHEKIT_NATIVE_UPDATED = 1,
    CACHEKIT_NATIVE_REJECTED_STALE_GENERATION = 2,
    CACHEKIT_NATIVE_REJECTED_CAPACITY = 3,
    CACHEKIT_NATIVE_FILL_INVALID_ARGUMENT = 4,
    CACHEKIT_NATIVE_FILL_INTERNAL_ERROR = 5
} cachekit_native_fill_status;

typedef struct cachekit_native_options {
    size_t capacity_entries;
    size_t key_arena_bytes;
    size_t value_arena_bytes;
    uint32_t min_native_batch_size;
    cachekit_native_kernel_preference kernel;
    uint32_t fingerprint_mask;
} cachekit_native_options;

typedef struct cachekit_native_key_view {
    uint32_t state_id;
    uint64_t generation;
    const uint8_t* data;
    size_t size;
} cachekit_native_key_view;

typedef struct cachekit_native_fill_view {
    cachekit_native_key_view key;
    const uint8_t* value;
    size_t value_size;
    uint8_t negative;
} cachekit_native_fill_view;

typedef struct cachekit_native_probe_result {
    cachekit_native_probe_status status;
    cachekit_native_error error;
    const uint8_t* value;
    size_t value_size;
} cachekit_native_probe_result;

typedef struct cachekit_native_fill_result {
    cachekit_native_fill_status status;
    cachekit_native_error error;
} cachekit_native_fill_result;

typedef struct cachekit_native_host_features {
    uint8_t aarch64;
    uint8_t neon;
    uint8_t crc32;
    uint8_t sve;
    uint8_t sve_vector_length_256;
} cachekit_native_host_features;

void cachekit_native_options_init(cachekit_native_options* options);

cachekit_native_plane* cachekit_native_plane_create(
        const cachekit_native_options* options,
        cachekit_native_error* error,
        char* error_message,
        size_t error_message_capacity);

void cachekit_native_plane_destroy(cachekit_native_plane* plane);

cachekit_native_error cachekit_native_probe_batch(
        cachekit_native_plane* plane,
        const cachekit_native_key_view* keys,
        cachekit_native_probe_result* results,
        size_t count);

cachekit_native_error cachekit_native_fill_batch(
        cachekit_native_plane* plane,
        const cachekit_native_fill_view* fills,
        cachekit_native_fill_result* results,
        size_t count);

void cachekit_native_plane_clear(cachekit_native_plane* plane);
size_t cachekit_native_plane_size(const cachekit_native_plane* plane);
size_t cachekit_native_plane_capacity(const cachekit_native_plane* plane);
uint64_t cachekit_native_plane_evictions(const cachekit_native_plane* plane);
uint32_t cachekit_native_plane_min_batch_size(const cachekit_native_plane* plane);
uint32_t cachekit_native_plane_kernel_kind(const cachekit_native_plane* plane);
const char* cachekit_native_plane_kernel_name(const cachekit_native_plane* plane);
cachekit_native_host_features cachekit_native_detect_host_features(void);
const char* cachekit_native_error_name(cachekit_native_error error);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // CACHEKIT_NATIVE_REQUEST_PLANE_C_H_
