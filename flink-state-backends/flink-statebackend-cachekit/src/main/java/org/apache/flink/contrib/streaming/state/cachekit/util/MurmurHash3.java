/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flink.contrib.streaming.state.cachekit.util;

/**
 *  The MurmurHash3 algorithm was created by Austin Appleby and placed in the public domain.
 *  This java port was authored by Yonik Seeley and also placed into the public domain.
 *  The author hereby disclaims copyright to this source code.
 *  <p>
 *  This produces exactly the same hash values as the final C++
 *  version of MurmurHash3 and is thus suitable for producing the same hash values across
 *  platforms.
 *  <p>
 *  The 32 bit x86 version of this hash should be the fastest variant for relatively short keys like ids.
 *  murmurhash3_x64_128 is a good choice for longer strings or if you need more than 32 bits of hash.
 *  <p>
 *  Note - The x86 and x64 versions do _not_ produce the same results, as the
 *  algorithms are optimized for their respective platforms.
 *  <p>
 *  See http://github.com/yonik/java_util for future updates to this file.
 */
public final class MurmurHash3 {

  /** 128 bits of state */
  public static final class LongPair {
    public long val1;
    public long val2;
  }

  public static final int fmix32(int h) {
    h ^= h >>> 16;
    h *= 0x85ebca6b;
    h ^= h >>> 13;
    h *= 0xc2b2ae35;
    h ^= h >>> 16;
    return h;
  }

  public static final long fmix64(long k) {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;
    return k;
  }

  /** Gets a long from a byte buffer in little endian byte order. */
  public static final long getLongLittleEndian(byte[] buf, int offset) {
    return     ((long)buf[offset+7]    << 56)   // no mask needed
            | ((buf[offset+6] & 0xffL) << 48)
            | ((buf[offset+5] & 0xffL) << 40)
            | ((buf[offset+4] & 0xffL) << 32)
            | ((buf[offset+3] & 0xffL) << 24)
            | ((buf[offset+2] & 0xffL) << 16)
            | ((buf[offset+1] & 0xffL) << 8)
            | ((buf[offset  ] & 0xffL));        // no shift needed
  }


  /** Returns the MurmurHash3_x86_32 hash. */
  @SuppressWarnings("fallthrough")
  public static int murmurhash3_x86_32(byte[] data, int offset, int len, int seed) {

    final int c1 = 0xcc9e2d51;
    final int c2 = 0x1b873593;

    int h1 = seed;
    int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

    for (int i=offset; i<roundedEnd; i+=4) {
      // little endian load order
      int k1 = (data[i] & 0xff) | ((data[i+1] & 0xff) << 8) | ((data[i+2] & 0xff) << 16) | (data[i+3] << 24);
      k1 *= c1;
      k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
      k1 *= c2;

      h1 ^= k1;
      h1 = (h1 << 13) | (h1 >>> 19);  // ROTL32(h1,13);
      h1 = h1*5+0xe6546b64;
    }

    // tail
    int k1 = 0;

    switch(len & 0x03) {
      case 3:
        k1 = (data[roundedEnd + 2] & 0xff) << 16;
        // fallthrough
      case 2:
        k1 |= (data[roundedEnd + 1] & 0xff) << 8;
        // fallthrough
      case 1:
        k1 |= (data[roundedEnd] & 0xff);
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >>> 17);  // ROTL32(k1,15);
        k1 *= c2;
        h1 ^= k1;
    }

    // finalization
    h1 ^= len;

    // fmix(h1);
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;

    return h1;
  }

  public static void murmurhash3_x64_128(byte[] key, int offset, int len, int seed, LongPair out) {
    // The original source is large; keep full implementation for compatibility.
    // This is copied from the same MurmurHash3 implementation used in cached state backend.

    long h1 = seed;
    long h2 = seed;

    final long c1 = 0x87c37b91114253d5L;
    final long c2 = 0x4cf5ad432745937fL;

    int roundedEnd = offset + (len & 0xfffffff0);  // round down to 16 byte block

    for (int i=offset; i<roundedEnd; i+=16) {
      long k1 = getLongLittleEndian(key, i);
      long k2 = getLongLittleEndian(key, i+8);

      k1 *= c1; k1 = Long.rotateLeft(k1,31); k1 *= c2; h1 ^= k1;

      h1 = Long.rotateLeft(h1,27); h1 += h2; h1 = h1*5+0x52dce729;

      k2 *= c2; k2 = Long.rotateLeft(k2,33); k2 *= c1; h2 ^= k2;

      h2 = Long.rotateLeft(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
    }

    long k1 = 0;
    long k2 = 0;

    switch(len & 15) {
      case 15:
        k2 = (key[roundedEnd+14] & 0xffL) << 48;
      case 14:
        k2 |= (key[roundedEnd+13] & 0xffL) << 40;
      case 13:
        k2 |= (key[roundedEnd+12] & 0xffL) << 32;
      case 12:
        k2 |= (key[roundedEnd+11] & 0xffL) << 24;
      case 11:
        k2 |= (key[roundedEnd+10] & 0xffL) << 16;
      case 10:
        k2 |= (key[roundedEnd+9] & 0xffL) << 8;
      case 9:
        k2 |= (key[roundedEnd+8] & 0xffL);
        k2 *= c2; k2  = Long.rotateLeft(k2,33); k2 *= c1; h2 ^= k2;
      case 8:
        k1 = ((long)key[roundedEnd+7]) << 56;
      case 7:
        k1 |= (key[roundedEnd+6] & 0xffL) << 48;
      case 6:
        k1 |= (key[roundedEnd+5] & 0xffL) << 40;
      case 5:
        k1 |= (key[roundedEnd+4] & 0xffL) << 32;
      case 4:
        k1 |= (key[roundedEnd+3] & 0xffL) << 24;
      case 3:
        k1 |= (key[roundedEnd+2] & 0xffL) << 16;
      case 2:
        k1 |= (key[roundedEnd+1] & 0xffL) << 8;
      case 1:
        k1 |= (key[roundedEnd] & 0xffL);
        k1 *= c1; k1  = Long.rotateLeft(k1,31); k1 *= c2; h1 ^= k1;
    }

    h1 ^= len;
    h2 ^= len;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    h2 += h1;

    out.val1 = h1;
    out.val2 = h2;
  }
}
