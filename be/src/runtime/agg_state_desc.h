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

#include <gen_cpp/descriptors.pb.h>

#include <memory>
#include <string>
#include <vector>

#include "gen_cpp/Types_types.h"

namespace starrocks {

struct TypeDescriptor;

class AggStateDesc;
using AggStateDescPtr = std::shared_ptr<AggStateDesc>;

class AggregateFunction;

class AggStateDesc {
public:
    AggStateDesc(std::string func_name, std::shared_ptr<TypeDescriptor> return_type,
                 std::vector<TypeDescriptor> arg_types, bool is_result_nullable, int func_version)
            : _func_name(std::move(func_name)),
              _return_type(std::move(return_type)),
              _arg_types(std::move(arg_types)),
              _is_result_nullable(is_result_nullable),
              _func_version(func_version) {}

    std::string get_func_name() const { return _func_name; }
    std::shared_ptr<TypeDescriptor> get_return_type() const { return _return_type; }
    std::vector<TypeDescriptor> get_arg_types() const { return _arg_types; }
    bool is_result_nullable() const { return _is_result_nullable; }
    int get_func_version() const { return _func_version; }

    // Transform this AggStateDesc to a thrift TTypeDesc.
    void to_thrift(TTypeDesc* t);
    // Transform this AggStateDesc to a protobuf AggStateTypePB.
    void to_protobuf(AggStateTypePB* desc);

    // Create a new AggStateDesc from a thrift TTypeDesc.
    static AggStateDescPtr from_thrift(const TAggStateDesc& desc);
    // Create a new AggStateDesc from a protobuf AggStateTypePB.
    static AggStateDescPtr from_protobuf(const AggStateTypePB& desc);
    // Convert thrift TAggStateDesc to protobuf AggStateTypePB.
    static void thrift_to_protobuf(const TAggStateDesc& desc, AggStateTypePB* pb);
    // Get the aggregate function state descriptor.
    static const AggregateFunction* get_agg_state_func(AggStateDesc* agg_state_desc, bool is_result_nullable);

private:
    // nested aggregate function name
    std::string _func_name;
    // nested aggregate function return type
    std::shared_ptr<TypeDescriptor> _return_type;
    // nested aggregate function argument types
    std::vector<TypeDescriptor> _arg_types;
    // nested aggregate function input is nullable
    bool _is_result_nullable;
    // nested aggregate function version
    int _func_version;
};

} // namespace starrocks