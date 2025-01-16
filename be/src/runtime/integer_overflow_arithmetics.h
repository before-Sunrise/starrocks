// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <runtime/int128_arithmetics_x86_64.h>
#include <util/decimal_types.h>

namespace starrocks {
typedef __int128 int128_t;
template <typename T>
inline bool add_overflow(T a, T b, T* c) {
    return __builtin_add_overflow(a, b, c);
}
template <>
inline bool add_overflow(int a, int b, int* c) {
    return __builtin_sadd_overflow(a, b, c);
}
template <>
inline bool add_overflow(long a, long b, long* c) {
    return __builtin_saddl_overflow(a, b, c);
}

template <>
inline bool add_overflow(long long a, long long b, long long* c) {
    return __builtin_saddll_overflow(a, b, c);
}

inline bool int128_add_overflow(int128_t a, int128_t b, int128_t* c) {
    *c = a + b;
    return ((a < 0) == (b < 0)) && ((*c < 0) != (a < 0));
}

template <>
inline bool add_overflow(int128_t a, int128_t b, int128_t* c) {
#if defined(__x86_64__) && defined(__GNUC__)
    return asm_add_overflow(a, b, c);
#else
    return int128_add_overflow(a, b, c);
#endif
}

template <typename T>
inline bool sub_overflow(T a, T b, T* c) {
    return __builtin_sub_overflow(a, b, c);
}
template <>
inline bool sub_overflow(int a, int b, int* c) {
    return __builtin_ssub_overflow(a, b, c);
}
template <>
inline bool sub_overflow(long a, long b, long* c) {
    return __builtin_ssubl_overflow(a, b, c);
}

template <>
inline bool sub_overflow(long long a, long long b, long long* c) {
    return __builtin_ssubll_overflow(a, b, c);
}

inline bool int128_sub_overflow(int128_t a, int128_t b, int128_t* c) {
    *c = a - b;
    return ((a < 0) == (0 < b)) && ((*c < 0) != (a < 0));
}

template <>
inline bool sub_overflow(int128_t a, int128_t b, int128_t* c) {
#if defined(__x86_64__) && defined(__GNUC__)
    return asm_sub_overflow(a, b, c);
#else
    return int128_sub_overflow(a, b, c);
#endif
}

template <typename T>
inline bool mul_overflow(T a, T b, T* c) {
    return __builtin_mul_overflow(a, b, c);
}
template <>
inline bool mul_overflow(int a, int b, int* c) {
    return __builtin_smul_overflow(a, b, c);
}
template <>
inline bool mul_overflow(long a, long b, long* c) {
    return __builtin_smull_overflow(a, b, c);
}

template <>
inline bool mul_overflow(long long a, long long b, long long* c) {
    return __builtin_smulll_overflow(a, b, c);
}

// count leading zero for __int128
inline int clz128(unsigned __int128 v) {
    if (v == 0) return sizeof(__int128);
    unsigned __int128 shifted = v >> 64;
    if (shifted != 0) {
        return __builtin_clzll(shifted);
    } else {
        return __builtin_clzll(v) + 64;
    }
}

inline bool int128_mul_overflow(int128_t a, int128_t b, int128_t* c) {
    return __builtin_mul_overflow(a, b, c);
}

template <typename src_t>
inline double int128_to_double(src_t a) {
    typedef double dst_t;
    typedef uint64_t dst_rep_t;
    typedef __uint128_t usrc_t;
#define DST_REP_C UINT64_C

    enum {
        dstSigBits = 52,
    };

    if (a == 0) return 0.0;

    enum {
        dstMantDig = dstSigBits + 1,
        srcBits = sizeof(src_t) * CHAR_BIT,
        srcIsSigned = ((src_t)-1) < 0,
    };

    const src_t s = srcIsSigned ? a >> (srcBits - 1) : 0;

    a = (usrc_t)(a ^ s) - s;
    int sd = srcBits - clz128(a); // number of significant digits
    int e = sd - 1;               // exponent
    if (sd > dstMantDig) {
        //  start:  0000000000000000000001xxxxxxxxxxxxxxxxxxxxxxPQxxxxxxxxxxxxxxxxxx
        //  finish: 000000000000000000000000000000000000001xxxxxxxxxxxxxxxxxxxxxxPQR
        //                                                12345678901234567890123456
        //  1 = msb 1 bit
        //  P = bit dstMantDig-1 bits to the right of 1
        //  Q = bit dstMantDig bits to the right of 1
        //  R = "or" of all bits to the right of Q
        if (sd == dstMantDig + 1) {
            a <<= 1;
        } else if (sd == dstMantDig + 2) {
            // Do nothing.
        } else {
            a = ((usrc_t)a >> (sd - (dstMantDig + 2))) |
                ((a & ((usrc_t)(-1) >> ((srcBits + dstMantDig + 2) - sd))) != 0);
        }
        // finish:
        a |= (a & 4) != 0; // Or P into R
        ++a;               // round - this step may add a significant bit
        a >>= 2;           // dump Q and R
        // a is now rounded to dstMantDig or dstMantDig+1 bits
        if (a & ((usrc_t)1 << dstMantDig)) {
            a >>= 1;
            ++e;
        }
        // a is now rounded to dstMantDig bits
    } else {
        a <<= (dstMantDig - sd);
        // a is now rounded to dstMantDig bits
    }
    const int dstBits = sizeof(dst_t) * CHAR_BIT;
    const dst_rep_t dstSignMask = DST_REP_C(1) << (dstBits - 1);
    const int dstExpBits = dstBits - dstSigBits - 1;
    const int dstExpBias = (1 << (dstExpBits - 1)) - 1;
    const dst_rep_t dstSignificandMask = (DST_REP_C(1) << dstSigBits) - 1;
    // Combine sign, exponent, and mantissa.
    const dst_rep_t result = ((dst_rep_t)s & dstSignMask) | ((dst_rep_t)(e + dstExpBias) << dstSigBits) |
                             ((dst_rep_t)(a)&dstSignificandMask);

    const union {
        dst_t f;
        dst_rep_t i;
    } rep = {.i = result};
    return rep.f;
}

template <>
inline bool mul_overflow(int128_t a, int128_t b, int128_t* c) {
#if defined(__x86_64__) && defined(__GNUC__)
    return multi3(a, b, *c);
#else
    return int128_mul_overflow(a, b, c);
#endif
}

} // namespace starrocks
