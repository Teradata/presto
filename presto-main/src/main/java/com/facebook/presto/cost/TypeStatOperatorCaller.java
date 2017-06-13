/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.cost;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singletonList;

/**
 * This will contain set of all function used in process of calculation stats.
 * It will be created based on Type and TypeRegistry.
 */
public class TypeStatOperatorCaller
{
    private final Type type;
    private final FunctionRegistry functionRegistry;
    private final ConnectorSession session;

    public TypeStatOperatorCaller(Type type, FunctionRegistry functionRegistry, ConnectorSession session)
    {
        this.type = type;
        this.functionRegistry = functionRegistry;
        this.session = session;
    }

    public boolean callComparisonOperator(ComparisonExpressionType comparisonType, Object left, Object right)
    {
        OperatorType operatorType = OperatorType.valueOf(comparisonType.name());
        Signature signature = functionRegistry.resolveOperator(operatorType, ImmutableList.of(type, type));
        ScalarFunctionImplementation implementation = functionRegistry.getScalarFunctionImplementation(signature);
        return (boolean) ExpressionInterpreter.invoke(session, implementation, ImmutableList.of(left, right));
    }

    public boolean callBetweenOperator(Object which, Object low, Object high)
    {
        Signature signature = functionRegistry.resolveOperator(BETWEEN, ImmutableList.of(type, type, type));
        ScalarFunctionImplementation implementation = functionRegistry.getScalarFunctionImplementation(signature);
        return (boolean) ExpressionInterpreter.invoke(session, implementation, ImmutableList.of(which, low, high));
    }

    public Slice castToVarchar(Object object)
    {
        Signature castSignature = functionRegistry.getCoercion(type, VarcharType.createUnboundedVarcharType());
        ScalarFunctionImplementation castImplementation = functionRegistry.getScalarFunctionImplementation(castSignature);
        return (Slice) ExpressionInterpreter.invoke(session, castImplementation, singletonList(object));
    }

    public double translateToDouble(Object object)
    {
        if (object instanceof Long) {
            return (double) (long) object;
        }
        return (double) object; //fixme
    }

    public Ordering<Object> getOrdering()
    {
        return Ordering.from((o1, o2) -> {
            checkNotNull(o1 != null, "nulls not supported");
            checkNotNull(o2 != null, "nulls not supported");
            if (callComparisonOperator(ComparisonExpressionType.LESS_THAN, o1, o2)) {
                return -1;
            }
            else if (callComparisonOperator(ComparisonExpressionType.EQUAL, o1, o2)) {
                return 0;
            }
            else {
                return 1;
            }
        });
    }
}
