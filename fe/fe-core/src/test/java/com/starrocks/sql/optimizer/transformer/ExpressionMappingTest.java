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

package com.starrocks.sql.optimizer.transformer;

import com.starrocks.catalog.TableName;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ExpressionMappingTest {

    @Test
    public void testAddExpressionToColumnsDoesNotOverrideCurrentScope() {
        // Empty scope/fields is enough: we're only validating expressionToColumns merge semantics.
        Scope scope = new Scope(RelationId.anonymous(), new RelationFields());
        ExpressionMapping mapping = new ExpressionMapping(scope, java.util.List.of());

        SlotRef slot1 = new SlotRef(new TableName(null, null, "A"), "c1");
        ColumnRefOperator colRef1 = new ColumnRefOperator(1, IntegerType.INT, "c1", true);
        mapping.getExpressionToColumns().put(slot1, colRef1);

        // Another SlotRef that is equal() to slot1 (same table alias + column name),
        // but maps to a different ColumnRefOperator. This can happen across nested scopes
        // when aliases are reused (e.g. FROM (...) A) A) A).
        SlotRef slot2 = new SlotRef(new TableName(null, null, "A"), "c1");
        ColumnRefOperator colRef2 = new ColumnRefOperator(2, IntegerType.INT, "c1", true);
        Map<com.starrocks.sql.ast.expression.Expr, ColumnRefOperator> child = new HashMap<>();
        child.put(slot2, colRef2);

        mapping.addExpressionToColumns(child);

        // Current-scope mapping must win; otherwise later expression translation can use the wrong ColumnRefOperator.
        Assert.assertSame(colRef1, mapping.getExpressionToColumns().get(slot1));
    }
}
