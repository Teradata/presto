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
package com.facebook.presto.operator;

import com.facebook.presto.operator.aggregation.BlockIndex;
import com.facebook.presto.operator.aggregation.BlockPosition;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.operator.aggregation.state.NullableDoubleStateSerializer;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.AggregationStateSerializerFactory;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.LiteralParameter;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;

public class AnnotationBasedOperators
{
    @AggregationFunction("simple_exact_aggregate")
    @Description("Simple exact aggregate description")
    public static class ExactAggregationFunction
    {
        @InputFunction
        public static void input(@AggregationState NullableDoubleState state, @SqlType(DOUBLE) double value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(@AggregationState NullableDoubleState combine1, @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction(DOUBLE)
        public static void output(@AggregationState NullableDoubleState state, BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @AggregationFunction("simple_exact_aggregate_aggregation_state_moved")
    @Description("Simple exact function which has @AggregationState on different than first positions")
    public static class StateOnDifferentThanFirstPositionAggregationFunction
    {
        @InputFunction
        public static void input(@SqlType(DOUBLE) double value, @AggregationState NullableDoubleState state)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(@AggregationState NullableDoubleState combine1, @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction(DOUBLE)
        public static void output(BlockBuilder out, @AggregationState NullableDoubleState state)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @AggregationFunction("no_aggregation_state_aggregate")
    @Description("Aggregate with no @AggregationState annotations")
    public static class NotAnnotatedAggregateStateAggregationFunction
    {
        @InputFunction
        public static void input(NullableDoubleState state, @SqlType(DOUBLE) double value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(NullableDoubleState combine1, NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction(DOUBLE)
        public static void output(NullableDoubleState state, BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @AggregationFunction("custom_serializer_aggregate")
    @Description("Aggregate with no @AggregationState annotations")
    public static class CustomStateSerializerAggregationFunction
    {
        public static class CustomSerializer extends NullableDoubleStateSerializer
        {
        }

        @InputFunction
        public static void input(@AggregationState NullableDoubleState state, @SqlType(DOUBLE) double value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(@AggregationState NullableDoubleState combine1, @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction(DOUBLE)
        public static void output(@AggregationState NullableDoubleState state, BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }

        @AggregationStateSerializerFactory(NullableDoubleState.class)
        public static CustomSerializer createSerializer()
        {
            return new CustomSerializer();
        }
    }

    @AggregationFunction(value = "custom_decomposable_aggregate", decomposable = false)
    @Description("Aggregate with Decomposable=true")
    public static class NotDecomposableAggregationFunction
    {
        @InputFunction
        public static void input(@AggregationState NullableDoubleState state, @SqlType(DOUBLE) double value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(@AggregationState NullableDoubleState combine1, @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction(DOUBLE)
        public static void output(@AggregationState NullableDoubleState state, BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }

        @AggregationStateSerializerFactory(NullableDoubleState.class)
        public static AccumulatorStateSerializer<?> createSerializer()
        {
            return new CustomStateSerializerAggregationFunction.CustomSerializer();
        }
    }

    @AggregationFunction("simple_generic_implementations")
    @Description("Simple aggregate with two generic implementations")
    public static class GenericAggregationFunction
    {
        @InputFunction
        @TypeParameter("T")
        public static void input(
                @AggregationState NullableDoubleState state,
                @SqlType("T") double value)
        {
            // noop this is only for annotation testing puproses
        }

        @InputFunction
        @TypeParameter("T")
        public static void input(
                @AggregationState NullableLongState state,
                @SqlType("T") long value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @AggregationState NullableLongState state,
                @AggregationState NullableLongState otherState)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @AggregationState NullableDoubleState state,
                @AggregationState NullableDoubleState otherState)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("T")
        public static void output(
                @AggregationState NullableLongState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("T")
        public static void output(
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @AggregationFunction("block_input_aggregate")
    @Description("Simple aggregate with @BlockPosition usage")
    public static class BlockInputAggregationFunction
    {
        @InputFunction
        public static void input(@AggregationState NullableDoubleState state, @BlockPosition @SqlType(DOUBLE) Block value, @BlockIndex int id)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(@AggregationState NullableDoubleState combine1, @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction(DOUBLE)
        public static void output(@AggregationState NullableDoubleState state, BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @AggregationFunction("implicit_specialized_aggregate")
    @Description("Simple implicit specialized aggregate")
    public static class ImplicitSpecializedAggregationFunction
    {
        @InputFunction
        @TypeParameter("T")
        public static void input(@AggregationState NullableDoubleState state,
                @SqlType("array(T)") Block arrayBlock, @SqlType("T") double additionalValue)
        {
            // noop this is only for annotation testing puproses
        }

        @InputFunction
        @TypeParameter("T")
        public static void input(@AggregationState NullableLongState state,
                @SqlType("array(T)") Block arrayBlock, @SqlType("T") long additionalValue)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @AggregationState NullableLongState state,
                @AggregationState NullableLongState otherState)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @AggregationState NullableDoubleState state,
                @AggregationState NullableDoubleState otherState)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("T")
        public static void output(
                @AggregationState NullableLongState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("T")
        public static void output(
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @AggregationFunction("explicit_specialized_aggregate")
    @Description("Simple explicit specialized aggregate")
    public static class ExplicitSpecializedAggregationFunction
    {
        @InputFunction
        @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
        @TypeParameter("T")
        public static void input(@AggregationState NullableDoubleState state,
                @SqlType("array(T)") Block arrayBlock)
        {
            // noop this is only for annotation testing puproses
        }

        @InputFunction
        @TypeParameter("T")
        public static void input(@AggregationState NullableLongState state,
                @SqlType("array(T)") Block arrayBlock)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @AggregationState NullableLongState state,
                @AggregationState NullableLongState otherState)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(
                @AggregationState NullableDoubleState state,
                @AggregationState NullableDoubleState otherState)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("T")
        public static void output(
                @AggregationState NullableLongState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("T")
        public static void output(
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @AggregationFunction("multi_output_aggregate")
    @Description("Simple multi output function aggregate generic description")
    public static class MultiOutputAggregationFunction
    {
        @InputFunction
        public static void input(@AggregationState NullableDoubleState state, @SqlType(DOUBLE) double value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(@AggregationState NullableDoubleState combine1, @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @AggregationFunction("multi_output_aggregate_1")
        @Description("Simple multi output function aggregate specialized description")
        @OutputFunction(DOUBLE)
        public static void output1(@AggregationState NullableDoubleState state, BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }

        @AggregationFunction("multi_output_aggregate_2")
        @OutputFunction(DOUBLE)
        public static void output2(@AggregationState NullableDoubleState state, BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }
    }

    @AggregationFunction("inject_operator_aggregate")
    @Description("Simple aggregate with operator injected")
    public static class InjectOperatorAggregateFunction
    {
        @InputFunction
        public static void input(@OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {DOUBLE, DOUBLE}) MethodHandle methodHandle,
                @AggregationState NullableDoubleState state,
                @SqlType(DOUBLE) double value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(@OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {DOUBLE, DOUBLE}) MethodHandle methodHandle,
                @AggregationState NullableDoubleState combine1,
                @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction(DOUBLE)
        public static void output(@OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {DOUBLE, DOUBLE}) MethodHandle methodHandle,
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }

        @AggregationStateSerializerFactory(NullableDoubleState.class)
        public static CustomStateSerializerAggregationFunction.CustomSerializer createSerializer(
                @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {DOUBLE, DOUBLE}) MethodHandle methodHandle)
        {
            return new CustomStateSerializerAggregationFunction.CustomSerializer();
        }
    }

    @AggregationFunction("inject_type_aggregate")
    @Description("Simple aggregate with type injected")
    public static class InjectTypeAggregateFunction
    {
        @InputFunction
        @TypeParameter("T")
        public static void input(@TypeParameter("T") Type type,
                @AggregationState NullableDoubleState state,
                @SqlType("T") double value)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(@TypeParameter("T") Type type,
                @AggregationState NullableDoubleState combine1,
                @AggregationState NullableDoubleState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("T")
        public static void output(@TypeParameter("T") Type type,
                @AggregationState NullableDoubleState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }

        @AggregationStateSerializerFactory(NullableDoubleState.class)
        public static CustomStateSerializerAggregationFunction.CustomSerializer createSerializer(
                @TypeParameter("T") Type type)
        {
            return new CustomStateSerializerAggregationFunction.CustomSerializer();
        }
    }

    @AggregationFunction("inject_literal_aggregate")
    @Description("Simple aggregate with type literal")
    public static class InjectLiteralAggregateFunction
    {
        @InputFunction
        @LiteralParameters("x")
        public static void input(@LiteralParameter("x") Long varcharSize,
                @AggregationState SliceState state,
                @SqlType("varchar(x)") Slice slice)
        {
            // noop this is only for annotation testing puproses
        }

        @CombineFunction
        public static void combine(@LiteralParameter("x") Long varcharSize,
                @AggregationState SliceState combine1,
                @AggregationState SliceState combine2)
        {
            // noop this is only for annotation testing puproses
        }

        @OutputFunction("varchar(x)")
        public static void output(@LiteralParameter("x") Long varcharSize,
                @AggregationState SliceState state,
                BlockBuilder out)
        {
            // noop this is only for annotation testing puproses
        }

        @AggregationStateSerializerFactory(SliceState.class)
        public static CustomStateSerializerAggregationFunction.CustomSerializer createSerializer(
                @LiteralParameter("x") Long varcharSize)
        {
            return new CustomStateSerializerAggregationFunction.CustomSerializer();
        }
    }

    @ScalarFunction("single_implementation_parametric_scalar")
    @Description("Simple scalar with single implemenatation based on class")
    public static class SingleImplementationScalarFunction
    {
        @SqlType(DOUBLE)
        public static double fun(@SqlType(DOUBLE) double v)
        {
            return v;
        }
    }

    @ScalarFunction(value = "hidden_scalar_function", hidden = true)
    @Description("Simple scalar with hidden property set")
    public static class HiddenScalarFunction
    {
        @SqlType(DOUBLE)
        public static double fun(@SqlType(DOUBLE) double v)
        {
            return v;
        }
    }

    @ScalarFunction(value = "non_deterministic_scalar_function", deterministic = false)
    @Description("Simple scalar with deterministic property reset")
    public static class NonDeterministicScalarFunction
    {
        @SqlType(DOUBLE)
        public static double fun(@SqlType(DOUBLE) double v)
        {
            return v;
        }
    }

    @ScalarFunction("scalar_with_nullable")
    @Description("Simple scalar with nullable primitive")
    public static class WithNullablePrimitiveArgScalarFunction
    {
        @SqlType(DOUBLE)
        public static double fun(@SqlType(DOUBLE) double v, @SqlNullable @SqlType(DOUBLE) double v2, @IsNull boolean v2isNull)
        {
            return v;
        }
    }

    @ScalarFunction("scalar_with_nullable_complex")
    @Description("Simple scalar with nullable complex type")
    public static class WithNullableComplexArgScalarFunction
    {
        @SqlType(DOUBLE)
        public static double fun(@SqlType(DOUBLE) double v, @SqlNullable @SqlType(DOUBLE) Double v2)
        {
            return v;
        }
    }

    public static class StaticMethodScalarFunction
    {
        @ScalarFunction("static_method_scalar")
        @Description("Simple scalar with single implemenatation based on method")
        @SqlType(DOUBLE)
        public static double fun(@SqlType(DOUBLE) double v)
        {
            return v;
        }
    }

    public static class MultiScalarFunction
    {
        @ScalarFunction("static_method_scalar_1")
        @Description("Simple scalar with single implemenatation based on method 1")
        @SqlType(DOUBLE)
        public static double fun1(@SqlType(DOUBLE) double v)
        {
            return v;
        }

        @ScalarFunction(value = "static_method_scalar_2", hidden = true, deterministic = false)
        @Description("Simple scalar with single implemenatation based on method 2")
        @SqlType(BIGINT)
        public static long fun2(@SqlType(BIGINT) long v)
        {
            return v;
        }
    }

    @ScalarFunction("parametric_scalar")
    @Description("Parametric scalar description")
    public static class ParametricScalarFunction
    {
        @SqlType("T")
        @TypeParameter("T")
        public static double fun(@SqlType("T") double v)
        {
            return v;
        }

        @SqlType("T")
        @TypeParameter("T")
        public static long fun(@SqlType("T") long v)
        {
            return v;
        }
    }

    @ScalarFunction("with_exact_scalar")
    @Description("Parametric scalar with exact and generic implementations")
    public static class ComplexParametricScalarFunction
    {
        @SqlType(BOOLEAN)
        @LiteralParameters("x")
        public static boolean fun1(@SqlType("array(varchar(x))") Block array)
        {
            return true;
        }

        @SqlType(BOOLEAN)
        public static boolean fun2(@SqlType("array(varchar(17))") Block array)
        {
            return true;
        }
    }

    @ScalarFunction("parametric_scalar_inject")
    @Description("Parametric scalar with literal injected")
    public static class SimpleInjectionScalarFunction
    {
        @SqlType(BIGINT)
        @LiteralParameters("x")
        public static long fun(@LiteralParameter("x") Long literalParam, @SqlType("varchar(x)") Slice val)
        {
            return literalParam;
        }
    }

    @ScalarFunction("parametric_scalar_inject_constructor")
    @Description("Parametric scalar with type injected though constructor")
    public static class ConstructorInjectionScalarFunction
    {
        private final Type type;

        @TypeParameter("T")
        public ConstructorInjectionScalarFunction(@TypeParameter("T") Type type)
        {
            this.type = type;
        }

        @SqlType(BIGINT)
        @TypeParameter("T")
        public long fun(@SqlType("array(T)") Slice val)
        {
            return 17L;
        }
    }
}
