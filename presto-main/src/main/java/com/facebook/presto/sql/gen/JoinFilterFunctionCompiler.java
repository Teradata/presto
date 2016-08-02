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
package com.facebook.presto.sql.gen;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import java.util.Map;

import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;
import static java.lang.String.format;

public class JoinFilterFunctionCompiler
{
    private final Metadata metadata;

    public JoinFilterFunctionCompiler(Metadata metadata)
    {
        this.metadata = metadata;
    }

    public void generateMethods(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, RowExpression filter, int leftBlocksSize)
    {
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);
        generateFilterMethod(classDefinition, callSiteBinder, cachedInstanceBinder, filter, leftBlocksSize);
        generateConstructor(classDefinition, cachedInstanceBinder);
    }

    private void generateFilterMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, CachedInstanceBinder cachedInstanceBinder, RowExpression filter, int leftBlocksSize)
    {
        // todo handle TRY expression
        Map<CallExpression, MethodDefinition> tryMethodMap = ImmutableMap.of();

        //int leftPosition, Block[] leftBlocks, int rightPosition, Block[] rightBlocks
        Parameter leftPosition = arg("leftPosition", int.class);
        Parameter leftBlocks = arg("leftBlocks", Block[].class);
        Parameter rightPosition = arg("rightPosition", int.class);
        Parameter rightBlocks = arg("rightBlocks", Block[].class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(boolean.class),
                ImmutableList.<Parameter>builder()
                        .add(leftPosition)
                        .add(leftBlocks)
                        .add(rightPosition)
                        .add(rightBlocks)
                        .build());

        method.comment("filter: %s", filter.toString());
        BytecodeBlock body = method.getBody();

        Scope scope = method.getScope();
        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());

        BytecodeExpressionVisitor visitor = new BytecodeExpressionVisitor(
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder, leftPosition, leftBlocks, rightPosition, rightBlocks, leftBlocksSize, wasNullVariable),
                metadata.getFunctionRegistry(),
                tryMethodMap);

        BytecodeNode visitorBody = filter.accept(visitor, scope);

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(visitorBody)
                .putVariable(result)
                .append(new IfStatement()
                        .condition(wasNullVariable)
                        .ifTrue(constantFalse().ret())
                        .ifFalse(result.ret()));
    }

    private static RowExpressionVisitor<Scope, BytecodeNode> fieldReferenceCompiler(
            final CallSiteBinder callSiteBinder,
            final Variable leftPosition,
            final Variable leftBlocks,
            final Variable rightPosition,
            final Variable rightBlocks,
            final int leftBlocksSize,
            final Variable wasNullVariable)
    {
        return new RowExpressionVisitor<Scope, BytecodeNode>()
        {
            @Override
            public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
            {
                int field = node.getField();
                Type type = node.getType();

                Class<?> javaType = type.getJavaType();
                if (!javaType.isPrimitive() && javaType != Slice.class) {
                    javaType = Object.class;
                }

                Variable blocksVariable;
                Variable positionVariable;
                if (field < leftBlocksSize) {
                    blocksVariable = leftBlocks;
                    positionVariable = leftPosition;
                }
                else {
                    blocksVariable = rightBlocks;
                    positionVariable = rightPosition;
                    field -= leftBlocksSize;
                }

                IfStatement ifStatement = new IfStatement();
                ifStatement.condition()
                        .setDescription(format("block_%d.get%s()", field, type))
                        .append(blocksVariable.getElement(field))
                        .append(positionVariable)
                        .invokeInterface(Block.class, "isNull", boolean.class, int.class);

                ifStatement.ifTrue()
                        .putVariable(wasNullVariable, true)
                        .pushJavaDefault(javaType);

                String methodName = "get" + Primitives.wrap(javaType).getSimpleName();

                ifStatement.ifFalse()
                        .append(loadConstant(callSiteBinder.bind(type, Type.class)))
                        .append(blocksVariable.getElement(field))
                        .append(positionVariable)
                        .invokeInterface(Type.class, methodName, javaType, Block.class, int.class);

                return ifStatement;
            }

            @Override
            public BytecodeNode visitCall(CallExpression call, Scope scope)
            {
                throw new UnsupportedOperationException("only input references are supported");
            }

            @Override
            public BytecodeNode visitConstant(ConstantExpression literal, Scope scope)
            {
                throw new UnsupportedOperationException("only input references are supported");
            }
        };
    }

    private void generateConstructor(ClassDefinition classDefinition, CachedInstanceBinder cachedInstanceBinder)
    {
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));
        Variable thisVariable = constructorDefinition.getThis();
        BytecodeBlock constructor = constructorDefinition
                .getBody()
                .comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class)
                .ret();
    }
}
