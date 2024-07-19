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

package com.starrocks.catalog.combinator;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.AggStateType;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.FunctionSet;

import java.util.Objects;

public class AggStateUnionCombinator extends AggregateFunction {
    private final AggregateFunction aggFunc;

    /**
     * Union combinator for aggregate function to union the agg state to return the immediate result of aggregate function.
     * </p>
     * DEFINE immediate_type {agg_func}_union(immediate_type)
     * DESC:
     *  input: immediate_type with agg_state_type
     *  immediate: immediate_type with agg_state_type
     *  output: immediate_type with agg_state_type
     */
    public AggStateUnionCombinator(AggregateFunction aggFunc, AggStateType aggStateType) {
        super(new FunctionName(aggFunc.functionName() + FunctionSet.AGG_STATE_UNION_SUFFIX),
                ImmutableList.of(aggStateType), aggStateType, aggStateType, false);
        this.aggFunc = Objects.requireNonNull(aggFunc, "nested can not be null");
    }

    public static AggStateUnionCombinator of(AggregateFunction aggFunc) {
        return new AggStateUnionCombinator(aggFunc, new AggStateType(aggFunc));
    }
}
