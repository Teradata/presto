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
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.OpCode;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.ForLoop;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import com.facebook.presto.bytecode.instruction.LabelNode;
import com.facebook.presto.operator.InMemoryJoinHash;
import com.facebook.presto.operator.JoinFilterFunction;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.PagesHashStrategy;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.CompilerUtils.makeClassName;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantBoolean;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantLong;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.getStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.not;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.notEqual;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static java.util.Objects.requireNonNull;

public class JoinCompiler
{
    private final LoadingCache<CacheKey, LookupSourceFactory> lookupSourceFactories = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<CacheKey, LookupSourceFactory>()
            {
                @Override
                public LookupSourceFactory load(CacheKey key)
                        throws Exception
                {
                    return internalCompileLookupSourceFactory(key.getTypes(), key.getJoinChannels(), key.getJoinFilterFunctionClass());
                }
            });

    private final LoadingCache<CacheKey, Class<? extends PagesHashStrategy>> hashStrategies = CacheBuilder.newBuilder().maximumSize(1000).build(
            new CacheLoader<CacheKey, Class<? extends PagesHashStrategy>>() {
                @Override
                public Class<? extends PagesHashStrategy> load(CacheKey key)
                        throws Exception
                {
                    return internalCompileHashStrategy(key.getTypes(), key.getJoinChannels(), key.getJoinFilterFunctionClass());
                }
            });

    public LookupSourceFactory compileLookupSourceFactory(List<? extends Type> types, List<Integer> joinChannels, Optional<Class<? extends JoinFilterFunction>> joinFilterFunctionClass)
    {
        try {
            return lookupSourceFactories.get(new CacheKey(types, joinChannels, joinFilterFunctionClass));
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    public PagesHashStrategyFactory compilePagesHashStrategyFactory(List<Type> types, List<Integer> joinChannels, Optional<Class<? extends JoinFilterFunction>> joinFilterFunction)
    {
        requireNonNull(types, "types is null");
        requireNonNull(joinChannels, "joinChannels is null");
        requireNonNull(joinFilterFunction, "joinFilterFunction is null");

        try {
            return new PagesHashStrategyFactory(hashStrategies.get(new CacheKey(types, joinChannels, joinFilterFunction)));
        }
        catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private LookupSourceFactory internalCompileLookupSourceFactory(List<Type> types, List<Integer> joinChannels, Optional<Class<? extends JoinFilterFunction>> joinFilterFunctionClass)
    {
        Class<? extends PagesHashStrategy> pagesHashStrategyClass = internalCompileHashStrategy(types, joinChannels, joinFilterFunctionClass);

        Class<? extends LookupSource> lookupSourceClass = IsolatedClass.isolateClass(
                new DynamicClassLoader(getClass().getClassLoader()),
                LookupSource.class,
                InMemoryJoinHash.class);

        return new LookupSourceFactory(lookupSourceClass, new PagesHashStrategyFactory(pagesHashStrategyClass));
    }

    private Class<? extends PagesHashStrategy> internalCompileHashStrategy(List<Type> types, List<Integer> joinChannels, Optional<Class<? extends JoinFilterFunction>> joinFilterFunctionClassOptional)
    {
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("PagesHashStrategy"),
                type(Object.class),
                type(PagesHashStrategy.class));

        FieldDefinition emptyBlockArrayField = classDefinition.declareField(a(PRIVATE, FINAL, STATIC), "EMPTY_BLOCK_ARRAY", Block[].class);
        classDefinition.getClassInitializer().getBody()
                .comment("EMPTY_BLOCK_ARRAY = new Block[0]")
                .append(BytecodeExpressions.newArray(type(Block[].class), 0))
                .putStaticField(emptyBlockArrayField);

        FieldDefinition sizeField = classDefinition.declareField(a(PRIVATE, FINAL), "size", type(long.class));
        List<FieldDefinition> channelFields = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "channel_" + i, type(List.class, Block.class));
            channelFields.add(channelField);
        }
        FieldDefinition channelArraysField = classDefinition.declareField(a(PRIVATE, FINAL), "channelArrays", type(List.class, Block[].class));
        List<Type> joinChannelTypes = new ArrayList<>();
        List<FieldDefinition> joinChannelFields = new ArrayList<>();
        for (int i = 0; i < joinChannels.size(); i++) {
            joinChannelTypes.add(types.get(joinChannels.get(i)));
            FieldDefinition channelField = classDefinition.declareField(a(PRIVATE, FINAL), "joinChannel_" + i, type(List.class, Block.class));
            joinChannelFields.add(channelField);
        }
        FieldDefinition hashChannelField = classDefinition.declareField(a(PRIVATE, FINAL), "hashChannel", type(List.class, Block.class));
        Optional<FieldDefinition> joinFilterFunctionField = joinFilterFunctionClassOptional.map(
                joinFilterFunctionClass -> classDefinition.declareField(a(PRIVATE, FINAL), "joinFilterFunction", JoinFilterFunction.class)
        );

        generateConstructor(classDefinition, joinChannels, sizeField, channelFields, channelArraysField, joinChannelFields, hashChannelField, joinFilterFunctionField);
        generateGetChannelCountMethod(classDefinition, channelFields);
        generateGetSizeInBytesMethod(classDefinition, sizeField);
        generateAppendToMethod(classDefinition, callSiteBinder, types, channelFields);
        generateHashPositionMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields, hashChannelField);
        generateHashRowMethod(classDefinition, callSiteBinder, joinChannelTypes);
        generateRowEqualsRowMethod(classDefinition, callSiteBinder, joinChannelTypes);
        generatePositionEqualsRowMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields, true);
        generatePositionEqualsRowMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields, false);
        generatePositionEqualsRowWithPageMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields);
        generatePositionEqualsPositionMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields, true);
        generatePositionEqualsPositionMethod(classDefinition, callSiteBinder, joinChannelTypes, joinChannelFields, false);
        generateHasFilterFunctionMethod(classDefinition, joinFilterFunctionField);
        generateApplyFilterFunctionMethod(classDefinition, joinFilterFunctionField);
        generateGetLeftBlocksMethod(classDefinition, channelArraysField, emptyBlockArrayField);
        generateIsPositionNull(classDefinition, joinChannelFields);

        return defineClass(classDefinition, PagesHashStrategy.class, callSiteBinder.getBindings(), getClass().getClassLoader());
    }

    private static void generateConstructor(ClassDefinition classDefinition,
            List<Integer> joinChannels,
            FieldDefinition sizeField,
            List<FieldDefinition> channelFields,
            FieldDefinition channelArraysField,
            List<FieldDefinition> joinChannelFields,
            FieldDefinition hashChannelField,
            Optional<FieldDefinition> joinFilterFunctionFieldOptional)
    {
        Parameter channels = arg("channels", type(List.class, type(List.class, Block.class)));
        Parameter hashChannel = arg("hashChannel", type(Optional.class, Integer.class));
        Parameter joinFilterFunction = arg("joinFilterFunction", type(Optional.class, JoinFilterFunction.class));
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), channels, hashChannel, joinFilterFunction);

        Variable thisVariable = constructorDefinition.getThis();
        Variable blockIndex = constructorDefinition.getScope().declareVariable(int.class, "blockIndex");

        BytecodeBlock constructor = constructorDefinition
                .getBody()
                .comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        constructor.comment("this.size = 0")
                    .append(thisVariable.setField(sizeField, constantLong(0L)));

        constructor.comment("Set channel fields");

        for (int index = 0; index < channelFields.size(); index++) {
            BytecodeExpression channel = channels.invoke("get", Object.class, constantInt(index))
                    .cast(type(List.class, Block.class));

            constructor.append(thisVariable.setField(channelFields.get(index), channel));

            BytecodeBlock loopBody = new BytecodeBlock();

            constructor.comment("for(blockIndex = 0; blockIndex < channel.size(); blockIndex++) { size += channel.get(i).getRetainedSizeInBytes() }")
                    .append(new ForLoop()
                            .initialize(blockIndex.set(constantInt(0)))
                            .condition(new BytecodeBlock()
                                    .append(blockIndex)
                                    .append(channel.invoke("size", int.class))
                                    .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                            .update(new BytecodeBlock().incrementVariable(blockIndex, (byte) 1))
                            .body(loopBody));

            loopBody.append(thisVariable)
                    .append(thisVariable)
                    .getField(sizeField)
                    .append(
                            channel.invoke("get", Object.class, blockIndex)
                            .cast(type(Block.class))
                            .invoke("getRetainedSizeInBytes", int.class)
                            .cast(long.class))
                    .longAdd()
                    .putField(sizeField);
        }

        constructor.comment("Set channelArrays field");

        constructor.append(thisVariable.setField(
                channelArraysField,
                newInstance(ArrayList.class)));

        if (!channelFields.isEmpty()) {
            Variable blocksVariable = constructorDefinition.getScope().declareVariable(Block[].class, "blocks");
            BytecodeExpression firstChannel = thisVariable.getField(channelFields.get(0));
            BytecodeBlock loopBody = new BytecodeBlock();
            constructor.append(
                    new ForLoop()
                            .initialize(blockIndex.set(constantInt(0)))
                            .condition(new BytecodeBlock()
                                    .append(blockIndex)
                                    .append(firstChannel.invoke("size", int.class))
                                    .invokeStatic(CompilerOperations.class, "lessThan", boolean.class, int.class, int.class))
                            .update(new BytecodeBlock().incrementVariable(blockIndex, (byte) 1))
                            .body(loopBody)
            );
            loopBody.append(blocksVariable.set(newArray(type(Block[].class), channelFields.size())));
            for (int channelIndex = 0; channelIndex < channelFields.size(); ++channelIndex) {
                loopBody.append(
                        blocksVariable.setElement(
                                constantInt(channelIndex),
                                thisVariable
                                        .getField(channelFields.get(channelIndex))
                                        .invoke("get", Object.class, blockIndex)
                                        .cast(Block.class)
                        )
                );
            }
            loopBody.append(thisVariable.getField(channelArraysField).invoke("add", boolean.class, ImmutableList.of(Object.class), blocksVariable)).pop();
        }

        constructor.comment("Set join channel fields");
        for (int index = 0; index < joinChannelFields.size(); index++) {
            BytecodeExpression joinChannel = channels.invoke("get", Object.class, constantInt(joinChannels.get(index)))
                    .cast(type(List.class, Block.class));

            constructor.append(thisVariable.setField(joinChannelFields.get(index), joinChannel));
        }

        constructor.comment("Set hashChannel");
        constructor.append(new IfStatement()
                .condition(hashChannel.invoke("isPresent", boolean.class))
                .ifTrue(thisVariable.setField(
                        hashChannelField,
                        channels.invoke("get", Object.class, hashChannel.invoke("get", Object.class).cast(Integer.class).cast(int.class))))
                .ifFalse(thisVariable.setField(
                        hashChannelField,
                        constantNull(hashChannelField.getType()))));

        constructor.comment("check if join filter function is passed only if field is generated");
        if (joinFilterFunctionFieldOptional.isPresent()) {
            constructor.append(invokeStatic(Preconditions.class, "checkArgument", void.class, ImmutableList.of(boolean.class, Object.class),
                    joinFilterFunction.invoke("isPresent", boolean.class), constantString("expected joinFilterFunction")));
        }
        else {
            constructor.append(invokeStatic(Preconditions.class, "checkArgument", void.class, ImmutableList.of(boolean.class, Object.class),
                    not(joinFilterFunction.invoke("isPresent", boolean.class)), constantString("unexpected joinFilterFunction")));
        }
        joinFilterFunctionFieldOptional.ifPresent(joinFilterFunctionField -> {
            constructor.comment("Set joinFilterFunction");
            constructor.append(thisVariable.setField(
                    joinFilterFunctionField,
                    joinFilterFunction.invoke("get", Object.class).cast(joinFilterFunctionField.getType())));
        });
        constructor.ret();
    }

    private static void generateGetChannelCountMethod(ClassDefinition classDefinition, List<FieldDefinition> channelFields)
    {
        classDefinition.declareMethod(
                a(PUBLIC),
                "getChannelCount",
                type(int.class))
                .getBody()
                .push(channelFields.size())
                .retInt();
    }

    private static void generateGetSizeInBytesMethod(ClassDefinition classDefinition, FieldDefinition sizeField)
    {
        MethodDefinition getSizeInBytesMethod = classDefinition.declareMethod(a(PUBLIC), "getSizeInBytes", type(long.class));

        Variable thisVariable = getSizeInBytesMethod.getThis();
        getSizeInBytesMethod.getBody()
                .append(thisVariable.getField(sizeField))
                .retLong();
    }

    private static void generateAppendToMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> types, List<FieldDefinition> channelFields)
    {
        Parameter blockIndex = arg("blockIndex", int.class);
        Parameter blockPosition = arg("blockPosition", int.class);
        Parameter pageBuilder = arg("pageBuilder", PageBuilder.class);
        Parameter outputChannelOffset = arg("outputChannelOffset", int.class);
        MethodDefinition appendToMethod = classDefinition.declareMethod(a(PUBLIC), "appendTo", type(void.class), blockIndex, blockPosition, pageBuilder, outputChannelOffset);

        Variable thisVariable = appendToMethod.getThis();
        BytecodeBlock appendToBody = appendToMethod.getBody();

        for (int index = 0; index < channelFields.size(); index++) {
            Type type = types.get(index);
            BytecodeExpression typeExpression = constantType(callSiteBinder, type);

            BytecodeExpression block = thisVariable
                    .getField(channelFields.get(index))
                    .invoke("get", Object.class, blockIndex)
                    .cast(Block.class);

            appendToBody
                    .comment("%s.appendTo(channel_%s.get(blockIndex), blockPosition, pageBuilder.getBlockBuilder(outputChannelOffset + %s));", type.getClass(), index, index)
                    .append(typeExpression)
                    .append(block)
                    .append(blockPosition)
                    .append(pageBuilder)
                    .append(outputChannelOffset)
                    .push(index)
                    .append(OpCode.IADD)
                    .invokeVirtual(PageBuilder.class, "getBlockBuilder", BlockBuilder.class, int.class)
                    .invokeInterface(Type.class, "appendTo", void.class, Block.class, int.class, BlockBuilder.class);
        }
        appendToBody.ret();
    }

    private void generateIsPositionNull(ClassDefinition classDefinition, List<FieldDefinition> joinChannelFields)
    {
        Parameter blockIndex = arg("blockIndex", int.class);
        Parameter blockPosition = arg("blockPosition", int.class);
        MethodDefinition isPositionNullMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "isPositionNull",
                type(boolean.class),
                blockIndex,
                blockPosition);

        for (FieldDefinition joinChannelField : joinChannelFields) {
            BytecodeExpression block = isPositionNullMethod
                    .getThis()
                    .getField(joinChannelField)
                    .invoke("get", Object.class, blockIndex)
                    .cast(Block.class);

            IfStatement ifStatement = new IfStatement();
            ifStatement.condition(block.invoke(
                    "isNull",
                    boolean.class,
                    blockPosition
            ));
            ifStatement.ifTrue(constantTrue().ret());
            isPositionNullMethod.getBody().append(ifStatement);
        }

        isPositionNullMethod
                .getBody()
                .append(constantFalse().ret());
    }

    private static void generateHashPositionMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> joinChannelTypes, List<FieldDefinition> joinChannelFields, FieldDefinition hashChannelField)
    {
        Parameter blockIndex = arg("blockIndex", int.class);
        Parameter blockPosition = arg("blockPosition", int.class);
        MethodDefinition hashPositionMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "hashPosition",
                type(long.class),
                blockIndex,
                blockPosition);

        Variable thisVariable = hashPositionMethod.getThis();
        BytecodeExpression hashChannel = thisVariable.getField(hashChannelField);
        BytecodeExpression bigintType = constantType(callSiteBinder, BigintType.BIGINT);

        IfStatement ifStatement = new IfStatement();
        ifStatement.condition(notEqual(hashChannel, constantNull(hashChannelField.getType())));
        ifStatement.ifTrue(
                bigintType.invoke(
                        "getLong",
                        long.class,
                        hashChannel.invoke("get", Object.class, blockIndex).cast(Block.class),
                        blockPosition)
                        .ret()
        );

        hashPositionMethod
                .getBody()
                .append(ifStatement);

        Variable resultVariable = hashPositionMethod.getScope().declareVariable(long.class, "result");
        hashPositionMethod.getBody().push(0L).putVariable(resultVariable);

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            BytecodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            BytecodeExpression block = hashPositionMethod
                    .getThis()
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, blockIndex)
                    .cast(Block.class);

            hashPositionMethod
                    .getBody()
                    .getVariable(resultVariable)
                    .push(31L)
                    .append(OpCode.LMUL)
                    .append(typeHashCode(type, block, blockPosition))
                    .append(OpCode.LADD)
                    .putVariable(resultVariable);
        }

        hashPositionMethod
                .getBody()
                .getVariable(resultVariable)
                .retLong();
    }

    private static void generateHashRowMethod(ClassDefinition classDefinition, CallSiteBinder callSiteBinder, List<Type> joinChannelTypes)
    {
        Parameter position = arg("position", int.class);
        Parameter page = arg("blocks", Page.class);
        MethodDefinition hashRowMethod = classDefinition.declareMethod(a(PUBLIC), "hashRow", type(long.class), position, page);

        Variable resultVariable = hashRowMethod.getScope().declareVariable(long.class, "result");
        hashRowMethod.getBody().push(0L).putVariable(resultVariable);

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            BytecodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            BytecodeExpression block = page.invoke("getBlock", Block.class, constantInt(index));

            hashRowMethod
                    .getBody()
                    .getVariable(resultVariable)
                    .push(31L)
                    .append(OpCode.LMUL)
                    .append(typeHashCode(type, block, position))
                    .append(OpCode.LADD)
                    .putVariable(resultVariable);
        }

        hashRowMethod
                .getBody()
                .getVariable(resultVariable)
                .retLong();
    }

    private static BytecodeNode typeHashCode(BytecodeExpression type, BytecodeExpression blockRef, BytecodeExpression blockPosition)
    {
        return new IfStatement()
            .condition(blockRef.invoke("isNull", boolean.class, blockPosition))
            .ifTrue(constantLong(0L))
            .ifFalse(type.invoke("hash", long.class, blockRef, blockPosition));
    }

    private static void generateRowEqualsRowMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> joinChannelTypes)
    {
        Parameter leftPosition = arg("leftPosition", int.class);
        Parameter leftPage = arg("leftPage", Page.class);
        Parameter rightPosition = arg("rightPosition", int.class);
        Parameter rightPage = arg("rightPage", Page.class);
        MethodDefinition rowEqualsRowMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "rowEqualsRow",
                type(boolean.class),
                leftPosition,
                leftPage,
                rightPosition,
                rightPage);

        Scope compilerContext = rowEqualsRowMethod.getScope();
        for (int index = 0; index < joinChannelTypes.size(); index++) {
            BytecodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            BytecodeExpression leftBlock = leftPage.invoke("getBlock", Block.class, constantInt(index));

            BytecodeExpression rightBlock = rightPage.invoke("getBlock", Block.class, constantInt(index));

            LabelNode checkNextField = new LabelNode("checkNextField");
            rowEqualsRowMethod
                    .getBody()
                    .append(typeEquals(
                            type,
                            leftBlock,
                            leftPosition,
                            rightBlock,
                            rightPosition))
                    .ifTrueGoto(checkNextField)
                    .push(false)
                    .retBoolean()
                    .visitLabel(checkNextField);
        }

        rowEqualsRowMethod
                .getBody()
                .push(true)
                .retInt();
    }

    private static void generatePositionEqualsRowMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> joinChannelTypes,
            List<FieldDefinition> joinChannelFields,
            boolean ignoreNulls)
    {
        Parameter leftBlockIndex = arg("leftBlockIndex", int.class);
        Parameter leftBlockPosition = arg("leftBlockPosition", int.class);
        Parameter rightPosition = arg("rightPosition", int.class);
        Parameter rightPage = arg("rightPage", Page.class);
        MethodDefinition positionEqualsRowMethod = classDefinition.declareMethod(
                a(PUBLIC),
                ignoreNulls ? "positionEqualsRowIgnoreNulls" : "positionEqualsRow",
                type(boolean.class),
                leftBlockIndex,
                leftBlockPosition,
                rightPosition,
                rightPage);

        Variable thisVariable = positionEqualsRowMethod.getThis();

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            BytecodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            BytecodeExpression leftBlock = thisVariable
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, leftBlockIndex)
                    .cast(Block.class);

            BytecodeExpression rightBlock = rightPage.invoke("getBlock", Block.class, constantInt(index));
            BytecodeNode equalityCondition;
            if (ignoreNulls) {
                equalityCondition = typeEqualsIgnoreNulls(type, leftBlock, leftBlockPosition, rightBlock, rightPosition);
            }
            else {
                equalityCondition = typeEquals(type, leftBlock, leftBlockPosition, rightBlock, rightPosition);
            }

            LabelNode checkNextField = new LabelNode("checkNextField");
            positionEqualsRowMethod
                    .getBody()
                    .append(equalityCondition)
                    .ifTrueGoto(checkNextField)
                    .push(false)
                    .retBoolean()
                    .visitLabel(checkNextField);
        }

        positionEqualsRowMethod
                .getBody()
                .push(true)
                .retInt();
    }

    private static void generatePositionEqualsRowWithPageMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> joinChannelTypes,
            List<FieldDefinition> joinChannelFields)
    {
        Parameter leftBlockIndex = arg("leftBlockIndex", int.class);
        Parameter leftBlockPosition = arg("leftBlockPosition", int.class);
        Parameter rightPosition = arg("rightPosition", int.class);
        Parameter page = arg("page", Page.class);
        Parameter rightChannels = arg("rightChannels", int[].class);

        MethodDefinition positionEqualsRowMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "positionEqualsRow",
                type(boolean.class),
                leftBlockIndex,
                leftBlockPosition,
                rightPosition,
                page,
                rightChannels);

        Variable thisVariable = positionEqualsRowMethod.getThis();
        BytecodeBlock body = positionEqualsRowMethod.getBody();

        for (int index = 0; index < joinChannelTypes.size(); index++) {
            BytecodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            BytecodeExpression leftBlock = thisVariable
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, leftBlockIndex)
                    .cast(Block.class);

            BytecodeExpression rightBlock = page.invoke("getBlock", Block.class, rightChannels.getElement(index));

            body.append(new IfStatement()
                    .condition(typeEquals(type, leftBlock, leftBlockPosition, rightBlock, rightPosition))
                    .ifFalse(constantFalse().ret()));
        }

        body.append(constantTrue().ret());
    }

    private static void generatePositionEqualsPositionMethod(
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            List<Type> joinChannelTypes,
            List<FieldDefinition> joinChannelFields,
            boolean ignoreNulls)
    {
        Parameter leftBlockIndex = arg("leftBlockIndex", int.class);
        Parameter leftBlockPosition = arg("leftBlockPosition", int.class);
        Parameter rightBlockIndex = arg("rightBlockIndex", int.class);
        Parameter rightBlockPosition = arg("rightBlockPosition", int.class);
        MethodDefinition positionEqualsPositionMethod = classDefinition.declareMethod(
                a(PUBLIC),
                ignoreNulls ? "positionEqualsPositionIgnoreNulls" : "positionEqualsPosition",
                type(boolean.class),
                leftBlockIndex,
                leftBlockPosition,
                rightBlockIndex,
                rightBlockPosition);

        Variable thisVariable = positionEqualsPositionMethod.getThis();
        for (int index = 0; index < joinChannelTypes.size(); index++) {
            BytecodeExpression type = constantType(callSiteBinder, joinChannelTypes.get(index));

            BytecodeExpression leftBlock = thisVariable
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, leftBlockIndex)
                    .cast(Block.class);

            BytecodeExpression rightBlock = thisVariable
                    .getField(joinChannelFields.get(index))
                    .invoke("get", Object.class, rightBlockIndex)
                    .cast(Block.class);

            BytecodeNode equalityCondition;
            if (ignoreNulls) {
                equalityCondition = typeEqualsIgnoreNulls(type, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
            }
            else {
                equalityCondition = typeEquals(type, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
            }

            LabelNode checkNextField = new LabelNode("checkNextField");
            positionEqualsPositionMethod
                    .getBody()
                    .append(equalityCondition)
                    .ifTrueGoto(checkNextField)
                    .push(false)
                    .retBoolean()
                    .visitLabel(checkNextField);
        }

        positionEqualsPositionMethod
                .getBody()
                .push(true)
                .retInt();
    }

    private void generateHasFilterFunctionMethod(ClassDefinition classDefinition, Optional<FieldDefinition> joinFilterFunctionField)
    {
        MethodDefinition getFilterFunctionMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "hasFilterFunction",
                type(boolean.class));

        getFilterFunctionMethod.getBody()
                .append(constantBoolean(joinFilterFunctionField.isPresent()))
                .ret(boolean.class);
    }

    private void generateApplyFilterFunctionMethod(ClassDefinition classDefinition, Optional<FieldDefinition> joinFilterFunctionFieldOptional)
    {
        Parameter leftBlockIndex = arg("leftBlockIndex", int.class);
        Parameter leftPosition = arg("leftPosition", int.class);
        Parameter rightPosition = arg("rightPosition", int.class);
        Parameter allRightBlocks = arg("allRightBlocks", Block[].class);

        MethodDefinition applyFilterFunctionMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "applyFilterFunction",
                type(boolean.class),
                leftBlockIndex,
                leftPosition,
                rightPosition,
                allRightBlocks);

        if (joinFilterFunctionFieldOptional.isPresent()) {
            Variable thisVariable = applyFilterFunctionMethod.getThis();
            applyFilterFunctionMethod.getBody()
                    .append(thisVariable
                            .getField(joinFilterFunctionFieldOptional.get())
                            .invoke("filter", boolean.class, leftPosition, thisVariable.invoke("getLeftBlocks", Block[].class, leftBlockIndex), rightPosition, allRightBlocks));
        }
        else {
            applyFilterFunctionMethod.getBody().append(constantBoolean(true));
        }
        applyFilterFunctionMethod.getBody().retBoolean();
    }

    private void generateGetLeftBlocksMethod(ClassDefinition classDefinition, FieldDefinition channelArraysField, FieldDefinition emptyBlockArrayField)
    {
        Parameter leftBlockIndex = arg("leftBlockIndex", int.class);
        MethodDefinition getLeftBlocksMethod = classDefinition.declareMethod(
                a(PUBLIC),
                "getLeftBlocks",
                type(Block[].class),
                leftBlockIndex);

        Variable thisVariable = getLeftBlocksMethod.getThis();
        getLeftBlocksMethod.getBody()
                .append(new IfStatement()
                        .condition(thisVariable.getField(channelArraysField).invoke("isEmpty", boolean.class))
                        .ifTrue(getStatic(emptyBlockArrayField))
                        .ifFalse(thisVariable
                                .getField(channelArraysField)
                                .invoke("get", Object.class, leftBlockIndex)
                                .cast(Block[].class)))
                .ret(Block[].class);
    }

    private static BytecodeNode typeEquals(
            BytecodeExpression type,
            BytecodeExpression leftBlock,
            BytecodeExpression leftBlockPosition,
            BytecodeExpression rightBlock,
            BytecodeExpression rightBlockPosition)
    {
        IfStatement ifStatement = new IfStatement();
        ifStatement.condition()
                .append(leftBlock.invoke("isNull", boolean.class, leftBlockPosition))
                .append(rightBlock.invoke("isNull", boolean.class, rightBlockPosition))
                .append(OpCode.IOR);

        ifStatement.ifTrue()
                .append(leftBlock.invoke("isNull", boolean.class, leftBlockPosition))
                .append(rightBlock.invoke("isNull", boolean.class, rightBlockPosition))
                .append(OpCode.IAND);

        ifStatement.ifFalse().append(typeEqualsIgnoreNulls(type, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition));

        return ifStatement;
    }

    private static BytecodeNode typeEqualsIgnoreNulls(
            BytecodeExpression type,
            BytecodeExpression leftBlock,
            BytecodeExpression leftBlockPosition,
            BytecodeExpression rightBlock,
            BytecodeExpression rightBlockPosition)
    {
        return type.invoke("equalTo", boolean.class, leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
    }

    public static class LookupSourceFactory
    {
        private final Constructor<? extends LookupSource> constructor;
        private final PagesHashStrategyFactory pagesHashStrategyFactory;

        public LookupSourceFactory(Class<? extends LookupSource> lookupSourceClass, PagesHashStrategyFactory pagesHashStrategyFactory)
        {
            this.pagesHashStrategyFactory = pagesHashStrategyFactory;
            try {
                constructor = lookupSourceClass.getConstructor(LongArrayList.class, PagesHashStrategy.class);
            }
            catch (NoSuchMethodException e) {
                throw Throwables.propagate(e);
            }
        }

        public LookupSource createLookupSource(LongArrayList addresses, List<List<Block>> channels, Optional<Integer> hashChannel, Optional<JoinFilterFunction> joinFilterFunction)
        {
            PagesHashStrategy pagesHashStrategy = pagesHashStrategyFactory.createPagesHashStrategy(channels, hashChannel, joinFilterFunction);
            try {
                return constructor.newInstance(addresses, pagesHashStrategy);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static class PagesHashStrategyFactory
    {
        private final Constructor<? extends PagesHashStrategy> constructor;

        public PagesHashStrategyFactory(Class<? extends PagesHashStrategy> pagesHashStrategyClass)
        {
            try {
                constructor = pagesHashStrategyClass.getConstructor(List.class, Optional.class, Optional.class);
            }
            catch (NoSuchMethodException e) {
                throw Throwables.propagate(e);
            }
        }

        public PagesHashStrategy createPagesHashStrategy(List<? extends List<Block>> channels, Optional<Integer> hashChannel, Optional<JoinFilterFunction> joinFilterFunction)
        {
            try {
                return constructor.newInstance(channels, hashChannel, joinFilterFunction);
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static final class CacheKey
    {
        private final List<Type> types;
        private final List<Integer> joinChannels;
        private final Optional<Class<? extends JoinFilterFunction>> joinFilterFunctionClass;

        private CacheKey(List<? extends Type> types, List<Integer> joinChannels, Optional<Class<? extends JoinFilterFunction>> joinFilterFunctionClass)
        {
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.joinChannels = ImmutableList.copyOf(requireNonNull(joinChannels, "joinChannels is null"));
            this.joinFilterFunctionClass = requireNonNull(joinFilterFunctionClass, "joinFilterFunctionClass can not be null");
        }

        private CacheKey(List<? extends Type> types, List<Integer> joinChannels)
        {
            this(types, joinChannels, Optional.empty());
        }

        private List<Type> getTypes()
        {
            return types;
        }

        private List<Integer> getJoinChannels()
        {
            return joinChannels;
        }

        private Optional<Class<? extends JoinFilterFunction>> getJoinFilterFunctionClass()
        {
            return joinFilterFunctionClass;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(types, joinChannels, joinFilterFunctionClass);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof CacheKey)) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) obj;
            return Objects.equals(types, cacheKey.types) &&
                    Objects.equals(joinChannels, cacheKey.joinChannels) &&
                    Objects.equals(joinFilterFunctionClass, cacheKey.joinFilterFunctionClass);
        }
    }
}
