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

#include <map>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/base_aggregate_test.h"
#include "testutil/function_utils.h"

namespace starrocks {

static ColumnPtr make_const_bigint(int64_t v, size_t size) {
    return ColumnHelper::create_const_column<TYPE_BIGINT>(v, size);
}

static MutableColumnPtr make_nullable_double(const std::vector<std::pair<bool, double>>& data) {
    auto values = DoubleColumn::create();
    auto nulls = NullColumn::create();
    values->reserve(data.size());
    nulls->reserve(data.size());
    for (const auto& e : data) {
        nulls->append(e.first ? 1 : 0);
        values->append(e.second);
    }
    return NullableColumn::create(std::move(values), std::move(nulls));
}

static std::map<double, double> read_map_row(const NullableColumn* nullable_map_col, size_t row) {
    DCHECK(nullable_map_col->is_nullable());
    DCHECK(!nullable_map_col->is_null(row));
    auto* map_col = down_cast<MapColumn*>(nullable_map_col->data_column().get());

    const auto offsets = map_col->offsets().immutable_data();
    const uint32_t start = offsets[row];
    const uint32_t end = offsets[row + 1];

    auto* key_data = down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(map_col->keys_column_raw_ptr()));
    auto* val_data = down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(map_col->values_column_raw_ptr()));

    std::map<double, double> out;
    for (uint32_t i = start; i < end; i++) {
        out.emplace(key_data->get_data()[i], val_data->get_data()[i]);
    }
    return out;
}

static std::map<double, double> read_map_row(const MapColumn* map_col, size_t row) {
    const auto offsets = map_col->offsets().immutable_data();
    const uint32_t start = offsets[row];
    const uint32_t end = offsets[row + 1];

    auto* key_data = down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(map_col->keys_column_raw_ptr()));
    auto* val_data = down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(map_col->values_column_raw_ptr()));

    std::map<double, double> out;
    for (uint32_t i = start; i < end; i++) {
        out.emplace(key_data->get_data()[i], val_data->get_data()[i]);
    }
    return out;
}

static MutableColumnPtr make_nullable_map_double_double() {
    auto keys = NullableColumn::create(DoubleColumn::create(), NullColumn::create());
    auto vals = NullableColumn::create(DoubleColumn::create(), NullColumn::create());
    auto offsets = UInt32Column::create();
    auto map = MapColumn::create(std::move(keys), std::move(vals), std::move(offsets));
    return NullableColumn::create(std::move(map), NullColumn::create());
}

static MapColumn::MutablePtr make_map_double_double() {
    auto keys = NullableColumn::create(DoubleColumn::create(), NullColumn::create());
    auto vals = NullableColumn::create(DoubleColumn::create(), NullColumn::create());
    auto offsets = UInt32Column::create();
    return MapColumn::create(std::move(keys), std::move(vals), std::move(offsets));
}

TEST(NumericHistogramAggTest, BasicTwoArgs) {
    const AggregateFunction* func = get_aggregate_function("numeric_histogram", TYPE_BIGINT, TYPE_MAP, true);
    ASSERT_NE(func, nullptr);

    FunctionContext::TypeDesc ret;
    std::vector<FunctionContext::TypeDesc> args{TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_DOUBLE)};
    FunctionUtils fn_utils(nullptr, ret, args);
    FunctionContext* ctx = fn_utils.get_fn_ctx();

    // buckets is constant BIGINT.
    ctx->set_constant_columns({make_const_bigint(2, 1)});

    auto state = ManagedAggrState::create(ctx, func);

    auto buckets_col = make_const_bigint(2, 4);
    auto values = DoubleColumn::create();
    values->append(1.0);
    values->append(2.0);
    values->append(3.0);
    values->append(4.0);

    const Column* cols[] = {buckets_col.get(), values.get()};
    func->update_batch_single_state(ctx, values->size(), cols, state->state());

    auto out = make_nullable_map_double_double();
    func->finalize_to_column(ctx, state->state(), out.get());

    auto* out_nullable = down_cast<NullableColumn*>(out.get());
    ASSERT_EQ(out_nullable->size(), 1);
    ASSERT_FALSE(out_nullable->is_null(0));

    const auto result = read_map_row(out_nullable, 0);
    ASSERT_EQ(result.size(), 2);
    ASSERT_DOUBLE_EQ(result.at(1.5), 2.0);
    ASSERT_DOUBLE_EQ(result.at(3.5), 2.0);
}

TEST(NumericHistogramAggTest, BasicThreeArgs) {
    const AggregateFunction* func = get_aggregate_function("numeric_histogram", TYPE_BIGINT, TYPE_MAP, true);
    ASSERT_NE(func, nullptr);

    FunctionContext::TypeDesc ret;
    std::vector<FunctionContext::TypeDesc> args{TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_DOUBLE),
                                                TypeDescriptor(TYPE_DOUBLE)};
    FunctionUtils fn_utils(nullptr, ret, args);
    FunctionContext* ctx = fn_utils.get_fn_ctx();
    ctx->set_constant_columns({make_const_bigint(2, 1)});

    auto state = ManagedAggrState::create(ctx, func);

    auto buckets_col = make_const_bigint(2, 4);
    auto values = DoubleColumn::create();
    auto weights = DoubleColumn::create();
    values->append(1.0);
    values->append(2.0);
    values->append(3.0);
    values->append(4.0);
    weights->append(1.0);
    weights->append(1.0);
    weights->append(2.0);
    weights->append(2.0);

    const Column* cols[] = {buckets_col.get(), values.get(), weights.get()};
    func->update_batch_single_state(ctx, values->size(), cols, state->state());

    auto out = make_nullable_map_double_double();
    func->finalize_to_column(ctx, state->state(), out.get());

    auto* out_nullable = down_cast<NullableColumn*>(out.get());
    ASSERT_EQ(out_nullable->size(), 1);
    ASSERT_FALSE(out_nullable->is_null(0));

    const auto result = read_map_row(out_nullable, 0);
    ASSERT_EQ(result.size(), 2);
    ASSERT_DOUBLE_EQ(result.at(1.5), 2.0);
    ASSERT_DOUBLE_EQ(result.at(3.5), 4.0);
}

TEST(NumericHistogramAggTest, NullRowIgnored) {
    const AggregateFunction* func = get_aggregate_function("numeric_histogram", TYPE_BIGINT, TYPE_MAP, true);
    ASSERT_NE(func, nullptr);

    FunctionContext::TypeDesc ret;
    std::vector<FunctionContext::TypeDesc> args{TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_DOUBLE),
                                                TypeDescriptor(TYPE_DOUBLE)};
    FunctionUtils fn_utils(nullptr, ret, args);
    FunctionContext* ctx = fn_utils.get_fn_ctx();
    ctx->set_constant_columns({make_const_bigint(3, 1)});

    auto state = ManagedAggrState::create(ctx, func);

    auto buckets_col = make_const_bigint(3, 3);
    // value: [1, NULL, 3], weight: [1, 1, 1] -> total weight should be 2 (NULL row ignored).
    auto values = make_nullable_double({{false, 1.0}, {true, 0.0}, {false, 3.0}});
    auto weights = make_nullable_double({{false, 1.0}, {false, 1.0}, {false, 1.0}});

    const Column* cols[] = {buckets_col.get(), values.get(), weights.get()};
    func->update_batch_single_state(ctx, values->size(), cols, state->state());

    auto out = make_nullable_map_double_double();
    func->finalize_to_column(ctx, state->state(), out.get());

    auto* out_nullable = down_cast<NullableColumn*>(out.get());
    ASSERT_EQ(out_nullable->size(), 1);
    ASSERT_FALSE(out_nullable->is_null(0));

    const auto result = read_map_row(out_nullable, 0);
    double total = 0.0;
    for (const auto& [k, v] : result) {
        total += v;
    }
    ASSERT_DOUBLE_EQ(total, 2.0);
}

TEST(NumericHistogramAggTest, SerializeAndMerge) {
    const AggregateFunction* func = get_aggregate_function("numeric_histogram", TYPE_BIGINT, TYPE_MAP, false);
    ASSERT_NE(func, nullptr);

    FunctionContext::TypeDesc ret;
    std::vector<FunctionContext::TypeDesc> args{TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_DOUBLE)};
    FunctionUtils fn_utils(nullptr, ret, args);
    FunctionContext* ctx = fn_utils.get_fn_ctx();
    ctx->set_constant_columns({make_const_bigint(10, 1)});

    auto state1 = ManagedAggrState::create(ctx, func);
    auto buckets_col = make_const_bigint(10, 100);
    auto values = DoubleColumn::create();
    for (int i = 0; i < 100; i++) {
        values->append(static_cast<double>(i));
    }
    const Column* cols[] = {buckets_col.get(), values.get()};
    func->update_batch_single_state(ctx, values->size(), cols, state1->state());

    auto intermediate = BinaryColumn::create();
    func->serialize_to_column(ctx, state1->state(), intermediate.get());
    ASSERT_EQ(intermediate->size(), 1);

    auto state2 = ManagedAggrState::create(ctx, func);
    func->merge(ctx, intermediate.get(), state2->state(), 0);

    auto out1 = make_map_double_double();
    auto out2 = make_map_double_double();
    func->finalize_to_column(ctx, state1->state(), out1.get());
    func->finalize_to_column(ctx, state2->state(), out2.get());

    ASSERT_EQ(read_map_row(out1.get(), 0), read_map_row(out2.get(), 0));
}

} // namespace starrocks
