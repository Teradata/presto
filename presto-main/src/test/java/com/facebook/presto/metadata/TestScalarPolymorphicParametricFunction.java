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
package com.facebook.presto.metadata;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertEquals;

public class TestScalarPolymorphicParametricFunction
{
    private static final TypeRegistry TYPE_REGISTRY = new TypeRegistry();
    private static final FunctionRegistry REGISTRY = new FunctionRegistry(TYPE_REGISTRY, new BlockEncodingManager(TYPE_REGISTRY), true);
    private static final Signature SIGNATURE = Signature.builder()
            .name("foo")
            .returnType("bigint")
            .argumentTypes("varchar(x)")
            .build();
    private static final int INPUT_VARCHAR_LENGTH = 10;
    private static final TypeSignature INPUT_VARCHAR_TYPE = parseTypeSignature("varchar(" + INPUT_VARCHAR_LENGTH + ")");
    private static final List<TypeSignature> PARAMETER_TYPES = ImmutableList.of(INPUT_VARCHAR_TYPE);
    private static final Slice INPUT_SLICE = Slices.allocate(INPUT_VARCHAR_LENGTH);

    @Test
    public void testSelectsMethodBasedOnArgumentTypes()
            throws Throwable
    {
        ParametricFunction function = ParametricFunction.builder(TestScalarPolymorphicParametricFunction.class)
                .signature(SIGNATURE)
                .methods("bigintToBigintWithExtraParameter")
                .methods("varcharToBigintWithExtraParameter")
                .extraParameters((types, literals) -> ImmutableList.of(literals.get("x")))
                .build();

        FunctionInfo functionInfo = function.specialize(emptyMap(), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);

        assertEquals(functionInfo.getSignature(), SIGNATURE);
        assertEquals(functionInfo.resolveCalculatedTypes(PARAMETER_TYPES).getSignature(), SIGNATURE.resolveCalculatedTypes(PARAMETER_TYPES));
        assertEquals(functionInfo.getMethodHandle().invoke(INPUT_SLICE), (long) INPUT_VARCHAR_LENGTH);
    }

    @Test
    public void testSelectsMethodBasedOnReturnType()
            throws Throwable
    {
        ParametricFunction function = ParametricFunction.builder(TestScalarPolymorphicParametricFunction.class)
                .signature(SIGNATURE)
                .methods("varcharToVarcharWithExtraParameter")
                .methods("varcharToBigintWithExtraParameter")
                .extraParameters((types, literals) -> ImmutableList.of(42))
                .build();

        FunctionInfo functionInfo = function.specialize(emptyMap(), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);

        assertEquals(functionInfo.resolveCalculatedTypes(PARAMETER_TYPES).getSignature(), SIGNATURE.resolveCalculatedTypes(PARAMETER_TYPES));
        assertEquals(functionInfo.getMethodHandle().invoke(INPUT_SLICE), (long) 42);
    }

    @Test
    public void testPassesAliasedOutputLiteralsToExtraParametersFunction()
            throws Throwable
    {
        Signature signature = Signature.builder()
                .name("foo")
                .returnType("varchar(x + 10 as result_length)")
                .argumentTypes("varchar(x)")
                .build();

        ParametricFunction function = ParametricFunction.builder(TestScalarPolymorphicParametricFunction.class)
                .signature(signature)
                .methods("varcharToVarcharWithExtraParameter")
                .extraParameters((types, literals) -> ImmutableList.of(literals.get("result_length").intValue()))
                .build();

        FunctionInfo functionInfo = function.specialize(emptyMap(), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);
        Slice slice = (Slice) functionInfo.getMethodHandle().invoke(INPUT_SLICE);
        assertEquals(slice.length(), (long) INPUT_VARCHAR_LENGTH + 10);
    }

    @Test
    public void testSameLiteralInArgumentsAndReturnValue()
            throws Throwable
    {
        Signature signature = Signature.builder()
                .name("foo")
                .returnType("varchar(x)")
                .argumentTypes("varchar(x)")
                .build();

        ParametricFunction function = ParametricFunction.builder(TestScalarPolymorphicParametricFunction.class)
                .signature(signature)
                .methods("varcharToVarchar")
                .build();

        FunctionInfo functionInfo = function.specialize(emptyMap(), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);
        Slice slice = (Slice) functionInfo.getMethodHandle().invoke(INPUT_SLICE);
        assertEquals(slice.length(), (long) INPUT_VARCHAR_LENGTH);
        assertEquals(functionInfo.resolveCalculatedTypes(PARAMETER_TYPES).getReturnType(), INPUT_VARCHAR_TYPE);
    }

    @Test
    public void testTypeParameters()
            throws Throwable
    {
        Signature signature = Signature.builder()
                .name("foo")
                .typeParameters(comparableWithVariadicBound("V", VARCHAR))
                .returnType("V")
                .argumentTypes("V")
                .build();

        ParametricFunction function = ParametricFunction.builder(TestScalarPolymorphicParametricFunction.class)
                .signature(signature)
                .methods("varcharToVarchar")
                .build();

        FunctionInfo functionInfo = function.specialize(ImmutableMap.of("V", TYPE_REGISTRY.getType(INPUT_VARCHAR_TYPE)), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);
        Slice slice = (Slice) functionInfo.getMethodHandle().invoke(INPUT_SLICE);
        assertEquals(slice.length(), (long) INPUT_VARCHAR_LENGTH);
    }

    @Test(expectedExceptions = {IllegalStateException.class},
            expectedExceptionsMessageRegExp = "method foo was not found in class class com.facebook.presto.metadata.TestScalarPolymorphicParametricFunction")
    public void testFailIfNotAllMethodsPresent()
    {
        ParametricFunction.builder(TestScalarPolymorphicParametricFunction.class)
                .signature(SIGNATURE)
                .methods("bigintToBigintWithExtraParameter")
                .methods("foo")
                .build();
    }

    @Test(expectedExceptions = {IllegalStateException.class},
            expectedExceptionsMessageRegExp = "no methods are selected \\(call methods\\(\\) first\\)")
    public void testFailNoMethodsAreSelectedWhenExtraParametersFunctionIsSet()
    {
        ParametricFunction.builder(TestScalarPolymorphicParametricFunction.class)
                .signature(SIGNATURE)
                .extraParameters((types, literals) -> ImmutableList.of(literals.get("x")))
                .build();
    }

    @Test(expectedExceptions = {IllegalStateException.class},
            expectedExceptionsMessageRegExp = "two matching methods \\(varcharToBigintWithTwoExtraParameters and varcharToBigintWithExtraParameter\\) for parameter types \\[varchar\\(10\\)\\]")
    public void testFailIfTwoMethodsWithSameArguments()
    {
        ParametricFunction function = ParametricFunction.builder(TestScalarPolymorphicParametricFunction.class)
                .signature(SIGNATURE)
                .methods("varcharToBigintWithTwoExtraParameters")
                .methods("varcharToBigintWithExtraParameter")
                .build();

        function.specialize(emptyMap(), PARAMETER_TYPES, TYPE_REGISTRY, REGISTRY);
    }

    public static Slice varcharToVarchar(Slice varchar)
    {
        return varchar;
    }

    public static long varcharToBigintWithExtraParameter(Slice varchar, long extraParameter)
    {
        return extraParameter;
    }

    public static long bigintToBigintWithExtraParameter(long bigint, int extraParameter)
    {
        return bigint;
    }

    public static long varcharToBigintWithTwoExtraParameters(Slice varchar, long extraParameter1, int extraParameter2)
    {
        return extraParameter1;
    }

    public static Slice varcharToVarcharWithExtraParameter(Slice string, int extraParameter)
    {
        return Slices.allocate(extraParameter);
    }
}
