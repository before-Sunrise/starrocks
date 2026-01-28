// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.SingleRangePartitionDesc;

import java.util.Map;

/**
 * Test-only shim for legacy call sites.
 *
 * Some tests historically referenced {@code PartitionDescAnalyzer} without an explicit import. In that case Java
 * resolves the symbol in the current package first (i.e. {@code com.starrocks.catalog.PartitionDescAnalyzer}).
 *
 * This helper keeps those tests compiling while delegating to {@link SingleRangePartitionDesc#analyze(int, Map)}.
 */
public final class PartitionDescAnalyzer {
    private PartitionDescAnalyzer() {
    }

    public static void analyzeSingleRangePartitionDesc(SingleRangePartitionDesc desc, int partColNum,
                                                       Map<String, String> properties) throws AnalysisException {
        desc.analyze(partColNum, properties);
    }
}

