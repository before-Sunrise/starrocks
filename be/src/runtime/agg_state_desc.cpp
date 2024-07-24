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

#include "runtime/agg_state_desc.h"

#include <memory>

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "runtime/types.h"

namespace starrocks {

// Create a new AggStateDesc from a thrift TTypeDesc.
AggStateDescPtr AggStateDesc::from_thrift(const TAggStateDesc& desc) {
    VLOG(2) << "TAggStateDesc:" << apache::thrift::ThriftDebugString(desc);
    std::string agg_func_name = desc.agg_func_name;
    // return type
    TypeDescriptor return_type = TypeDescriptor::from_thrift(desc.ret_types);
    // arg types
    std::vector<TypeDescriptor> arg_types;
    for (auto& arg_type : desc.arg_types) {
        arg_types.emplace_back(TypeDescriptor::from_thrift(arg_type));
    }
    bool result_nullable = desc.result_nullable;
    int func_version = desc.func_version;
    return std::make_shared<AggStateDesc>(agg_func_name, std::make_shared<TypeDescriptor>(return_type), arg_types,
                                          result_nullable, func_version);
}

// Transform this AggStateDesc to a thrift TTypeDesc.
void AggStateDesc::to_thrift(TTypeDesc* t) {
    t->__isset.agg_state_desc = true;
    t->agg_state_desc.agg_func_name = _func_name;
    t->agg_state_desc.result_nullable = _is_result_nullable;
    t->agg_state_desc.func_version = _func_version;
    // return type
    TTypeDesc t_return_type = _return_type->to_thrift();
    for (auto& node : t_return_type.types) {
        t->agg_state_desc.ret_types.push_back(node);
    }
    // arg types
    for (auto& arg_type : _arg_types) {
        t->agg_state_desc.arg_types.push_back(arg_type.to_thrift());
    }
}

AggStateDescPtr AggStateDesc::from_protobuf(const AggStateTypePB& desc) {
    auto& agg_func_name = desc.agg_func_name();
    bool is_result_nullable = desc.is_result_nullable();
    bool func_version = desc.func_version();
    std::vector<TypeDescriptor> arg_types;
    // arg types
    for (auto& arg_type : desc.arg_types()) {
        arg_types.emplace_back(TypeDescriptor::from_protobuf(arg_type));
    }
    // ret type
    auto ret_type = TypeDescriptor::from_protobuf(desc.ret_type());
    return std::make_shared<AggStateDesc>(agg_func_name, std::make_shared<TypeDescriptor>(ret_type), arg_types,
                                          is_result_nullable, func_version);
}

void AggStateDesc::to_protobuf(AggStateTypePB* desc) {
    desc->set_agg_func_name(this->get_func_name());
    desc->set_is_result_nullable(this->is_result_nullable());
    desc->set_func_version(this->get_func_version());
    // arg types
    for (auto& arg_type : this->get_arg_types()) {
        auto* arg_type_pb = desc->add_arg_types();
        *arg_type_pb = arg_type.to_protobuf();
    }
    // ret type
    auto ret_type_desc = this->get_return_type();
    auto* ret_type_pb = desc->mutable_ret_type();
    *ret_type_pb = ret_type_desc->to_protobuf();
}

void AggStateDesc::thrift_to_protobuf(const TAggStateDesc& desc, AggStateTypePB* pb) {
    pb->set_agg_func_name(desc.agg_func_name);
    pb->set_is_result_nullable(desc.result_nullable);
    pb->set_func_version(desc.func_version);
    // arg types
    for (auto& arg_type : desc.arg_types) {
        auto arg_type_desc = TypeDescriptor::from_thrift(arg_type);
        auto* arg_type_pb = pb->add_arg_types();
        *arg_type_pb = arg_type_desc.to_protobuf();
    }
    // ret type
    auto ret_type_desc = TypeDescriptor::from_thrift(desc.ret_types);
    auto* ret_type_pb = pb->mutable_ret_type();
    *ret_type_pb = ret_type_desc.to_protobuf();
}

const AggregateFunction* AggStateDesc::get_agg_state_func(AggStateDesc* agg_state_desc, bool is_result_nullable) {
    DCHECK(agg_state_desc);
    auto* agg_function = get_aggregate_function(agg_state_desc->get_func_name(), *(agg_state_desc->get_return_type()),
                                                agg_state_desc->get_arg_types(), is_result_nullable,
                                                TFunctionBinaryType::BUILTIN, agg_state_desc->get_func_version());
    if (agg_function == nullptr) {
        LOG(WARNING) << "Failed to get aggregate function for " << agg_state_desc->get_func_name();
    }
    return agg_function;
}

} // namespace starrocks