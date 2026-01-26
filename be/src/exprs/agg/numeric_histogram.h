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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <numeric>
#include <queue>
#include <utility>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"

namespace starrocks {

// Histogram used by aggregate function numeric_histogram(buckets, value[, weight]).
// Notes:
// - buckets must be a non-null constant BIGINT (validated in BE).
// - NULL handling is done by NullableAggregateFunctionVariadic; this implementation assumes
//   all inputs are non-null for the rows that reach update().
class NumericHistogram {
public:
    static constexpr uint8_t kFormatTag = 0;
    static constexpr int32_t kEntryBufferSize = 100;

    NumericHistogram() = default;

    void init(int32_t max_buckets) {
        maxBuckets = max_buckets;
        nextIndex = 0;
        values.assign(static_cast<size_t>(maxBuckets + kEntryBufferSize), 0.0);
        weights.assign(static_cast<size_t>(maxBuckets + kEntryBufferSize), 0.0);
    }

    bool inited() const { return maxBuckets > 0; }

    void clear() { nextIndex = 0; }

    void add(double value, double weight) {
        if (nextIndex == static_cast<int32_t>(values.size())) {
            compact();
        }
        values[nextIndex] = value;
        weights[nextIndex] = weight;
        nextIndex++;
    }

    void compact() {
        nextIndex = merge_same_buckets(values, weights, nextIndex);
        if (nextIndex <= maxBuckets) {
            return;
        }
        store(merge_buckets(values, weights, nextIndex, maxBuckets));
    }

    void merge_with(NumericHistogram&& other) {
        if (other.nextIndex == 0) {
            return;
        }
        if (nextIndex == 0) {
            // Fast-path: adopt other's content.
            *this = std::move(other);
            return;
        }

        int32_t count = nextIndex + other.nextIndex;
        std::vector<double> new_values(static_cast<size_t>(count));
        std::vector<double> new_weights(static_cast<size_t>(count));

        concat(new_values, values, nextIndex, other.values, other.nextIndex);
        concat(new_weights, weights, nextIndex, other.weights, other.nextIndex);

        count = merge_same_buckets(new_values, new_weights, count);
        if (count <= maxBuckets) {
            // copy back into values/weights
            std::copy_n(new_values.data(), count, values.data());
            std::copy_n(new_weights.data(), count, weights.data());
            nextIndex = count;
            return;
        }

        store(merge_buckets(new_values, new_weights, count, maxBuckets));
    }

    void serialize_to(BinaryColumn* dst) {
        compact();
        Bytes& bytes = dst->get_bytes();
        const size_t old_size = bytes.size();
        const int32_t entry_count = nextIndex;
        const size_t total_size = sizeof(uint8_t) + sizeof(int32_t) + sizeof(int32_t) +
                                  sizeof(double) * static_cast<size_t>(entry_count) +
                                  sizeof(double) * static_cast<size_t>(entry_count);
        bytes.resize(old_size + total_size);

        uint8_t* p = bytes.data() + old_size;
        std::memcpy(p, &kFormatTag, sizeof(uint8_t));
        p += sizeof(uint8_t);
        std::memcpy(p, &maxBuckets, sizeof(int32_t));
        p += sizeof(int32_t);
        std::memcpy(p, &entry_count, sizeof(int32_t));
        p += sizeof(int32_t);
        std::memcpy(p, values.data(), sizeof(double) * static_cast<size_t>(entry_count));
        p += sizeof(double) * static_cast<size_t>(entry_count);
        std::memcpy(p, weights.data(), sizeof(double) * static_cast<size_t>(entry_count));

        dst->get_offset().emplace_back(bytes.size());
    }

    static StatusOr<NumericHistogram> deserialize_from(const Slice& src) {
        NumericHistogram hist;
        const uint8_t* p = reinterpret_cast<const uint8_t*>(src.data);
        const uint8_t* end = p + src.size;

        if (p + sizeof(uint8_t) + sizeof(int32_t) + sizeof(int32_t) > end) {
            return Status::InternalError("numeric_histogram: invalid serialized data");
        }

        uint8_t tag;
        std::memcpy(&tag, p, sizeof(uint8_t));
        p += sizeof(uint8_t);
        if (tag != kFormatTag) {
            return Status::InternalError("numeric_histogram: unsupported format tag");
        }

        int32_t max_buckets;
        int32_t entry_count;
        std::memcpy(&max_buckets, p, sizeof(int32_t));
        p += sizeof(int32_t);
        std::memcpy(&entry_count, p, sizeof(int32_t));
        p += sizeof(int32_t);

        if (max_buckets < 2) {
            return Status::InternalError("numeric_histogram: invalid maxBuckets");
        }
        if (entry_count < 0 || entry_count > max_buckets) {
            return Status::InternalError("numeric_histogram: invalid entry count");
        }

        const size_t required = sizeof(double) * static_cast<size_t>(entry_count) * 2;
        if (p + required > end) {
            return Status::InternalError("numeric_histogram: invalid serialized data length");
        }

        hist.init(max_buckets);
        hist.nextIndex = entry_count;
        std::memcpy(hist.values.data(), p, sizeof(double) * static_cast<size_t>(entry_count));
        p += sizeof(double) * static_cast<size_t>(entry_count);
        std::memcpy(hist.weights.data(), p, sizeof(double) * static_cast<size_t>(entry_count));
        return hist;
    }

    int32_t bucket_count() const { return nextIndex; }
    int32_t max_buckets() const { return maxBuckets; }
    const double* bucket_values() const { return values.data(); }
    const double* bucket_weights() const { return weights.data(); }

private:
    static int64_t _double_to_long_bits(double v) {
        int64_t bits;
        std::memcpy(&bits, &v, sizeof(bits));
        return bits;
    }

    static bool _double_less(double a, double b) {
        const bool a_nan = std::isnan(a);
        const bool b_nan = std::isnan(b);
        if (a_nan || b_nan) {
            return !a_nan && b_nan;
        }
        if (a < b) return true;
        if (a > b) return false;
        return _double_to_long_bits(a) < _double_to_long_bits(b);
    }

    struct Node {
        Node(int32_t id, double value, double weight, Node* left, Node* right)
                : id(id), value(value), weight(weight), left(left), right(right) {
            if (right != nullptr) {
                right->left = this;
                penalty = compute_penalty(value, weight, right->value, right->weight);
            } else {
                penalty = std::numeric_limits<double>::infinity();
            }
            if (left != nullptr) {
                left->right = this;
            }
        }

        Node(int32_t id, double value, double weight, Node* right) : Node(id, value, weight, nullptr, right) {}

        int32_t id;
        double value;
        double weight;
        double penalty;
        bool valid = true;
        Node* left = nullptr;
        Node* right = nullptr;
    };

    struct NodeLess {
        bool operator()(const Node* a, const Node* b) const {
            if (a->penalty != b->penalty) {
                return a->penalty > b->penalty; // min-heap
            }
            return a->id > b->id;
        }
    };

    static void concat(std::vector<double>& target, const std::vector<double>& first, int32_t first_len,
                       const std::vector<double>& second, int32_t second_len) {
        std::memcpy(target.data(), first.data(), sizeof(double) * static_cast<size_t>(first_len));
        std::memcpy(target.data() + first_len, second.data(), sizeof(double) * static_cast<size_t>(second_len));
    }

    static int32_t merge_same_buckets(std::vector<double>& values, std::vector<double>& weights, int32_t count) {
        if (count <= 1) return count;

        // Sort indices by corresponding values, then merge buckets by iterating in sorted order.
        // This avoids rebuilding a temporary (value, weight) array while still keeping (value, weight) paired.
        std::vector<int32_t> idx(static_cast<size_t>(count));
        std::iota(idx.begin(), idx.end(), 0);
        std::sort(idx.begin(), idx.end(), [&](int32_t a, int32_t b) { return _double_less(values[a], values[b]); });

        int32_t out = 0;
        double cur_v = values[idx[0]];
        double cur_w = weights[idx[0]];
        for (int32_t i = 1; i < count; i++) {
            const double v = values[idx[i]];
            const double w = weights[idx[i]];
            if (cur_v == v) {
                cur_w += w;
            } else {
                values[out] = cur_v;
                weights[out] = cur_w;
                out++;
                cur_v = v;
                cur_w = w;
            }
        }
        values[out] = cur_v;
        weights[out] = cur_w;
        return out + 1;
    }

    static double compute_penalty(double value1, double weight1, double value2, double weight2) {
        const double wsum = weight1 + weight2;
        if (UNLIKELY(wsum == 0.0)) {
            return 0.0;
        }
        const double diff = value1 - value2;
        return (weight1 * weight2 / wsum) * (diff * diff);
    }

    static std::vector<std::pair<double, double>> merge_buckets(const std::vector<double>& values,
                                                                const std::vector<double>& weights, int32_t count,
                                                                int32_t target_count) {
        // Precondition: values/weights[0..count) are sorted by value in ascending order.
        DCHECK_GT(count, 0);
        DCHECK_GT(target_count, 0);
        DCHECK_GE(count, target_count);

        std::priority_queue<Node*, std::vector<Node*>, NodeLess> queue;
        std::vector<std::unique_ptr<Node>> nodes;
        nodes.reserve(static_cast<size_t>(count) * 2);

        // Initialize doubly linked nodes from right to left.
        nodes.emplace_back(std::make_unique<Node>(count - 1, values[count - 1], weights[count - 1], nullptr, nullptr));
        Node* right = nodes.back().get();
        queue.push(right);
        for (int32_t i = count - 2; i >= 0; i--) {
            nodes.emplace_back(std::make_unique<Node>(i, values[i], weights[i], nullptr, right));
            right = nodes.back().get();
            queue.push(right);
        }

        while (count > target_count) {
            Node* current = queue.top();
            queue.pop();
            if (!current->valid) {
                continue;
            }

            count--;
            Node* r = current->right;
            if (r == nullptr || !r->valid) {
                continue;
            }

            const double new_weight = current->weight + r->weight;
            const double new_value = (current->value * current->weight + r->value * r->weight) / new_weight;

            // Invalidate right so we can skip it if it shows up again.
            r->valid = false;

            // Create merged node and link to right of right.
            Node* rr = r->right;
            nodes.emplace_back(std::make_unique<Node>(current->id, new_value, new_weight, nullptr, rr));
            Node* merged = nodes.back().get();
            queue.push(merged);

            // Replace left node to refresh its penalty (relative to merged).
            Node* l = current->left;
            if (l != nullptr) {
                if (l->valid) {
                    l->valid = false;
                    nodes.emplace_back(std::make_unique<Node>(l->id, l->value, l->weight, l->left, merged));
                    queue.push(nodes.back().get());
                }
            }
        }

        // Collect valid nodes from the queue.
        std::vector<std::pair<double, double>> out;
        out.reserve(static_cast<size_t>(target_count));
        while (!queue.empty()) {
            Node* n = queue.top();
            queue.pop();
            if (n->valid) {
                out.emplace_back(n->value, n->weight);
            }
        }

        std::sort(out.begin(), out.end(), [](const auto& a, const auto& b) { return _double_less(a.first, b.first); });
        return out;
    }

    void store(std::vector<std::pair<double, double>> pairs) {
        nextIndex = 0;
        std::sort(pairs.begin(), pairs.end(), [](const auto& a, const auto& b) { return _double_less(a.first, b.first); });
        for (const auto& p : pairs) {
            values[nextIndex] = p.first;
            weights[nextIndex] = p.second;
            nextIndex++;
        }
    }

    int32_t maxBuckets = 0;
    int32_t nextIndex = 0;
    std::vector<double> values;
    std::vector<double> weights;
};

struct NumericHistogramState {
    bool inited = false;
    NumericHistogram histogram;
};

class NumericHistogramAggregateFunction final
        : public AggregateFunctionBatchHelper<NumericHistogramState, NumericHistogramAggregateFunction> {
public:
    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr __restrict state) const override {
        auto& st = this->data(state);
        st.inited = false;
        st.histogram = NumericHistogram();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        auto& st = this->data(state);
        if (!st.inited) {
            const int32_t buckets = parse_and_validate_buckets(ctx);
            st.histogram.init(buckets);
            st.inited = true;
        }

        const size_t v_idx = columns[1]->is_constant() ? 0 : row_num;
        const auto* v_col = down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(columns[1]));
        const double value = v_col->get_data()[v_idx];
        double weight = 1.0;
        if (ctx->get_num_args() == 3) {
            const size_t w_idx = columns[2]->is_constant() ? 0 : row_num;
            const auto* w_col = down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(columns[2]));
            weight = w_col->get_data()[w_idx];
        }
        st.histogram.add(value, weight);
    }

    void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
                                   AggDataPtr __restrict state) const override {
        auto& st = this->data(state);
        if (!st.inited) {
            const int32_t buckets = parse_and_validate_buckets(ctx);
            st.histogram.init(buckets);
            st.inited = true;
        }

        const auto* v_col = down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(columns[1]));
        const auto& v_data = v_col->get_data();
        if (ctx->get_num_args() == 2) {
            if (columns[1]->is_constant()) {
                const double v = v_data[0];
                for (size_t i = 0; i < chunk_size; i++) {
                    st.histogram.add(v, 1.0);
                }
            } else {
                for (size_t i = 0; i < chunk_size; i++) {
                    st.histogram.add(v_data[i], 1.0);
                }
            }
            return;
        }

        const auto* w_col = down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(columns[2]));
        const auto& w_data = w_col->get_data();
        if (columns[1]->is_constant() && columns[2]->is_constant()) {
            const double v = v_data[0];
            const double w = w_data[0];
            for (size_t i = 0; i < chunk_size; i++) {
                st.histogram.add(v, w);
            }
        } else if (columns[1]->is_constant()) {
            const double v = v_data[0];
            for (size_t i = 0; i < chunk_size; i++) {
                st.histogram.add(v, w_data[i]);
            }
        } else if (columns[2]->is_constant()) {
            const double w = w_data[0];
            for (size_t i = 0; i < chunk_size; i++) {
                st.histogram.add(v_data[i], w);
            }
        } else {
            for (size_t i = 0; i < chunk_size; i++) {
                st.histogram.add(v_data[i], w_data[i]);
            }
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        auto& st = this->data(state);
        const size_t idx = column->is_constant() ? 0 : row_num;
        const auto* bin = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(column));
        const Slice s = bin->get_slice(idx);

        auto res = NumericHistogram::deserialize_from(s);
        if (!res.ok()) {
            ctx->set_error(res.status().message().c_str());
            return;
        }
        NumericHistogram other = std::move(res).value();

        if (!st.inited) {
            st.inited = true;
            st.histogram = std::move(other);
            return;
        }
        if (st.histogram.max_buckets() != other.max_buckets()) {
            ctx->set_error("numeric_histogram: buckets mismatch during merge");
            return;
        }
        st.histogram.merge_with(std::move(other));
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        auto* bin = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(to));
        auto& st = this->data(state);
        DCHECK(st.inited);
        // serialize_to() compacts and appends slice + offset.
        const_cast<NumericHistogram&>(st.histogram).serialize_to(bin);
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        DCHECK(dst->is_binary());
        auto* bin = down_cast<BinaryColumn*>(dst.get());

        const int32_t buckets = parse_and_validate_buckets(ctx);
        NumericHistogram temp;
        temp.init(buckets);

        const Column* v_col_raw = src[1].get();
        const Column* w_col_raw = (src.size() >= 3) ? src[2].get() : nullptr;

        auto* v_col = down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(v_col_raw));
        const auto& v_data = v_col->get_data();

        const bool v_const = v_col_raw->is_constant();
        const bool has_weight = (ctx->get_num_args() == 3);
        const bool w_const = has_weight && w_col_raw->is_constant();

        const DoubleColumn* w_col = nullptr;
        const DoubleColumn::Container* w_data = nullptr;
        if (has_weight) {
            w_col = down_cast<const DoubleColumn*>(ColumnHelper::get_data_column(w_col_raw));
            w_data = &w_col->get_data();
        }

        for (size_t i = 0; i < chunk_size; i++) {
            temp.clear();
            const double v = v_data[v_const ? 0 : i];
            const double w = has_weight ? (*w_data)[w_const ? 0 : i] : 1.0;
            temp.add(v, w);
            temp.serialize_to(bin);
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& st = this->data(state);
        st.histogram.compact();

        auto* map_column = down_cast<MapColumn*>(ColumnHelper::get_data_column(to));
        const int32_t n = st.histogram.bucket_count();

        // Append keys/values.
        auto* key_data_col = down_cast<DoubleColumn*>(ColumnHelper::get_data_column(map_column->keys_column_raw_ptr()));
        auto* val_data_col =
                down_cast<DoubleColumn*>(ColumnHelper::get_data_column(map_column->values_column_raw_ptr()));

        const double* keys = st.histogram.bucket_values();
        const double* vals = st.histogram.bucket_weights();
        for (int32_t i = 0; i < n; i++) {
            key_data_col->append(keys[i]);
            val_data_col->append(vals[i]);
        }

        // Ensure keys/values null columns grow (MapColumn requires them to be NullableColumn).
        if (map_column->keys_column()->is_nullable()) {
            auto* nullable_keys = down_cast<NullableColumn*>(map_column->keys_column_raw_ptr());
            nullable_keys->null_column_data().resize(nullable_keys->null_column_data().size() + n);
        }
        if (map_column->values_column()->is_nullable()) {
            auto* nullable_vals = down_cast<NullableColumn*>(map_column->values_column_raw_ptr());
            nullable_vals->null_column_data().resize(nullable_vals->null_column_data().size() + n);
        }

        auto* offsets = map_column->offsets_column_raw_ptr();
        offsets->append(offsets->immutable_data().back() + static_cast<uint32_t>(n));
    }

    std::string get_name() const override { return "numeric_histogram"; }

private:
    static int32_t parse_and_validate_buckets(FunctionContext* ctx) {
        if (!ctx->is_notnull_constant_column(0)) {
            ctx->set_error("numeric_histogram: buckets must be a non-null constant BIGINT");
            return 2; // unreachable; but keeps callers simple.
        }
        const int64_t buckets64 = ColumnHelper::get_const_value<TYPE_BIGINT>(ctx->get_constant_column(0));
        if (buckets64 < 2) {
            ctx->set_error("numeric_histogram bucket count must be greater than one");
            return 2;
        }
        if (buckets64 > std::numeric_limits<int32_t>::max()) {
            ctx->set_error("numeric_histogram: buckets is too large");
            return 2;
        }
        return static_cast<int32_t>(buckets64);
    }
};

} // namespace starrocks
