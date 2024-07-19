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

package com.starrocks.catalog;

import com.google.api.client.util.Lists;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.FeConstants;
import com.starrocks.thrift.TAggStateTypeDesc;
import com.starrocks.thrift.TTypeDesc;

import java.util.Arrays;
import java.util.List;

/**
 * It's a wrapper for the result type with extra information for the aggregate function.
 */
public class AggStateType extends Type {

    // argument types
    @SerializedName(value = "argTypes")
    private List<Type> argTypes;
    // return type
    @SerializedName(value = "returnType")
    private Type returnType;
    // result nullable
    @SerializedName(value = "resultNullable")
    private Boolean resultNullable;
    // agg function's name
    @SerializedName(value = "functionName")
    private String functionName;

    public AggStateType(AggregateFunction aggFunc) {
        this(aggFunc.functionName(), aggFunc.getIntermediateType(), Arrays.asList(aggFunc.getArgs()),
                aggFunc.isNullable());
    }

    public AggStateType(String functionName,
                        Type returnType,
                        List<Type> argTypes,
                        Boolean resultNullable) {
        Preconditions.checkNotNull(functionName, "functionName should not be null");
        Preconditions.checkNotNull(returnType, "returnType should not be null");
        Preconditions.checkNotNull(argTypes, "argTypes should not be null");
        Preconditions.checkNotNull(resultNullable, "resultNullable should not be null");
        this.functionName = functionName;
        this.returnType = returnType;
        this.argTypes = argTypes;
        this.resultNullable = resultNullable;
    }

    public List<Type> getArgTypes() {
        return argTypes;
    }

    public Boolean getResultNullable() {
        return resultNullable;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(argTypes, resultNullable, functionName);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AggStateType)) {
            return false;
        }
        AggStateType other = (AggStateType) o;
        int subTypeNumber = argTypes.size();
        for (int i = 0; i < subTypeNumber; i++) {
            if (!argTypes.get(i).equals(other.argTypes.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toSql(int depth) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("AGG_STATE<").append(functionName).append("(");
        for (int i = 0; i < argTypes.size(); i++) {
            if (i > 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append(argTypes.get(i).toSql());
        }
        stringBuilder.append(", result_nullable=");
        stringBuilder.append(resultNullable);
        stringBuilder.append(">");
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    protected String prettyPrint(int lpad) {
        return Strings.repeat(" ", lpad) + toSql();
    }

    @Override
    public void toThrift(TTypeDesc result) {
        // normal
        returnType.toThrift(result);
        // wrapper extra data type
        TAggStateTypeDesc tAggStateType = new TAggStateTypeDesc();
        tAggStateType.setAgg_func_name(functionName);
        for (int i = 0; i < argTypes.size(); i++) {
            TTypeDesc tTypeDesc = new TTypeDesc();
            argTypes.get(i).toThrift(tTypeDesc);
            tAggStateType.addToArg_types(tTypeDesc);
        }
        tAggStateType.setResult_nullable(resultNullable);
        tAggStateType.setFunc_version(FeConstants.AGG_FUNC_VERSION);
    }

    @Override
    public boolean isFullyCompatible(Type other) {
        return false;
    }

    @Override
    public AggStateType clone() {
        AggStateType clone = (AggStateType) super.clone();
        clone.functionName = this.functionName;
        clone.resultNullable = this.resultNullable;
        clone.argTypes = Lists.newArrayList(this.argTypes);
        return clone;
    }

    public String toMysqlDataTypeString() {
        return "binary";
    }

    // This implementation is the same as BE schema_columns_scanner.cpp type_to_string
    public String toMysqlColumnTypeString() {
        return toSql();
    }

    @Override
    protected String toTypeString(int depth) {
        if (depth >= MAX_NESTING_DEPTH) {
            return "agg_state<...>";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("AGG_STATE<").append(functionName).append("(");
        for (int i = 0; i < argTypes.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(argTypes.get(i).toSql());
        }
        if (resultNullable) {
            sb.append(" NULL");
        }
        sb.append(">");
        return sb.toString();
    }
}

