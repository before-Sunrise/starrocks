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
#include "column/column_helper.h"
#include "exprs/agg/aggregate.h"
#ifdef __x86_64__
#include <immintrin.h>
#endif
#if defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_acle.h>
#include <arm_neon.h>
#endif

namespace starrocks {
struct AggStateIfState {};

class AggStateIf final : public AggregateFunctionBatchHelper<AggStateIfState, AggStateIf> {
public:
    AggStateIf(AggStateDesc agg_state_desc, const AggregateFunction* function)
            : _agg_state_desc(std::move(agg_state_desc)), _function(function) {
        DCHECK(_function != nullptr);
    }
    const AggStateDesc* get_agg_state_desc() const { return &_agg_state_desc; }

    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const override { _function->create(ctx, ptr); }

    void destroy(FunctionContext* ctx, AggDataPtr __restrict ptr) const override { _function->destroy(ctx, ptr); }

    size_t size() const override { return _function->size(); }

    size_t alignof_size() const override { return _function->alignof_size(); }

    bool is_pod_state() const override { return _function->is_pod_state(); }

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        _function->reset(ctx, args, state);
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        throw std::runtime_error("agg if doesn't implement update");
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        throw std::runtime_error("agg if doesn't implement merge");
    }

    void serialize_to_column([[maybe_unused]] FunctionContext* ctx, ConstAggDataPtr __restrict state,
                             Column* to) const override {
        _function->serialize_to_column(ctx, state, to);
    }

    void convert_to_serialize_format([[maybe_unused]] FunctionContext* ctx, const Columns& srcs, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        DCHECK_EQ(1, srcs.size());
        *dst = srcs[0];
    }

    void finalize_to_column(FunctionContext* ctx __attribute__((unused)), ConstAggDataPtr __restrict state,
                            Column* to) const override {
        _function->serialize_to_column(ctx, state, to);
    }

    // override batch interface for better performance
    void update_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                      AggDataPtr* states) const override {
        throw std::runtime_error("agg if doesn't implement update_batch");
    }

    void update_batch_selectively(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column** columns,
                                  AggDataPtr* states, const Filter& filter) const override {
        throw std::runtime_error("agg if doesn't implement update_batch_selectively");
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        const uint8_t* f_data = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(columns[0])->raw_data();
        int offset = 0;
#ifdef __AVX2__
        // !important: filter must be an uint8_t container
        constexpr int batch_nums = 256 / (8 * sizeof(uint8_t));
        __m256i all0 = _mm256_setzero_si256();
        while (offset + batch_nums < chunk_size) {
            // TODO(kks): when our memory allocate could align 32-byte, we could use _mm256_load_si256
            __m256i f = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(f_data + offset));
            int mask = _mm256_movemask_epi8(_mm256_cmpgt_epi8(f, all0));
            if (mask == 0) {
                // skip all, do nothing
            } else if (mask == 0xffffffff) {
                // all hit
                for (size_t i = offset; i < offset + batch_nums; ++i) {
                    _function->update(ctx, columns + 1, state, i);
                }
            } else {
                for (size_t i = offset; i < offset + batch_nums; i++) {
                    if (f_data[i] == 1) {
                        _function->update(ctx, columns + 1, state, i);
                    }
                }
            }
            offset += batch_nums;
        }
#elif defined(__ARM_NEON) && defined(__aarch64__)
        constexpr int batch_nums = 128 / (8 * sizeof(uint8_t));
        while (offset + batch_nums < chunk_size) {
            const uint8x16_t v_null_data = vld1q_u8(f_data + offset);
            uint64_t nibble_mask = SIMD::get_nibble_mask(vmvnq_u8(vceqzq_u8(v_null_data)));
            if (nibble_mask == 0) {
                // skip all, do nothing
            } else if (nibble_mask == 0xffff'ffff'ffff'ffffull) { // all hit
                for (size_t i = offset; i < offset + batch_nums; ++i) {
                    _function->update(ctx, columns + 1, state, i);
                }
            } else { // Some hit.
                for (size_t i = offset; i < offset + batch_nums; i++) {
                    if (f_data[i] == 1) {
                        _function->update(ctx, columns + 1, state, i);
                    }
                }
            }
            offset += batch_nums;
        }
#endif
        for (size_t i = 0; i < chunk_size; ++i) {
            if (f_data[i] == 1) {
                _function->update(ctx, columns + 1, state, i);
            }
        }
    }

    void merge_batch(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column* column,
                     AggDataPtr* states) const override {
        for (size_t i = 0; i < chunk_size; ++i) {
            _function->merge(ctx, column, states[i] + state_offset, i);
        }
    }

    void merge_batch_selectively(FunctionContext* ctx, size_t chunk_size, size_t state_offset, const Column* column,
                                 AggDataPtr* states, const Filter& filter) const override {
        throw std::runtime_error("agg if doesn't implement merge_batch_selectively");
    }

    void merge_batch_single_state(FunctionContext* ctx, AggDataPtr __restrict state, const Column* input, size_t start,
                                  size_t size) const override {
        throw std::runtime_error("merge_batch_single_state");
    }

    std::string get_name() const override { return "agg_state_if"; }

private:
    const AggStateDesc _agg_state_desc;
    const AggregateFunction* _function;
};
} // namespace starrocks