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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.byteCode.Block;
import com.facebook.presto.byteCode.ClassDefinition;
import com.facebook.presto.byteCode.DynamicClassLoader;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.Parameter;
import com.facebook.presto.byteCode.Scope;
import com.facebook.presto.byteCode.Variable;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.CompilerOperations;
import com.facebook.presto.sql.gen.CompilerUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.byteCode.Access.FINAL;
import static com.facebook.presto.byteCode.Access.PRIVATE;
import static com.facebook.presto.byteCode.Access.PUBLIC;
import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.Access.a;
import static com.facebook.presto.byteCode.Parameter.arg;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.metadata.Signature.internalFunction;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.gen.CompilerUtils.defineClass;
import static com.facebook.presto.util.Reflection.methodHandle;

public final class ConcatFunction
        extends ParametricScalar
{
    public static final ConcatFunction CONCAT = new ConcatFunction();
    private static final Signature SIGNATURE = new Signature("concat", ImmutableList.of(), StandardTypes.VARCHAR, ImmutableList.of(StandardTypes.VARCHAR), true, false);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "concatenates given string";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Class<?> clazz = generateConcat(arity);
        MethodHandle methodHandle = methodHandle(clazz, "concat", Collections.nCopies(arity, Slice.class).toArray(new Class<?>[arity]));
        List<Boolean> nullableParameters = ImmutableList.copyOf(Collections.nCopies(arity, false));

        Signature specializedSignature = internalFunction(SIGNATURE.getName(), VARCHAR.getTypeSignature(), Collections.nCopies(arity, VARCHAR.getTypeSignature()));
        return new FunctionInfo(specializedSignature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, nullableParameters);
    }

    private static Class<?> generateConcat(int arity)
    {
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName("Concat" + arity + "ScalarFunction"),
                type(Object.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        // Generate concat()
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        for (int i = 0; i < arity; i++) {
            parameters.add(arg("arg" + i, Slice.class));
        }

        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "concat", type(Slice.class), parameters.build());
        Scope scope = method.getScope();
        Block body = method.getBody();

        body.comment("put 0 as total size on stack")
                .push(0);

        for (int i = 0; i < arity; ++i) {
            body.comment("get length of i-th varchar")
                    .append(scope.getVariable("arg" + i))
                    .invokeVirtual(Slice.class, "length", int.class)
                    .comment("add size of i-th varchar to total size")
                    .intAdd();
        }

        Variable concatVariable = scope.declareVariable(Slice.class, "concat");
        body.comment("Slice concat = Slices.allocate(total length of parameter slices)")
                .invokeStatic(Slices.class, "allocate", Slice.class, int.class)
                .putVariable(concatVariable);

        body.comment("put 0 as current position on stack")
                .push(0);

        for (int i = 0; i < arity; ++i) {
            body.comment("concat.setBytes(current position, i-th varchar")
                    .dup()
                    .append(scope.getVariable("arg" + i))
                    .getVariable(concatVariable)
                    .invokeStatic(CompilerOperations.class, "setBytes", void.class, int.class, Slice.class, Slice.class);

            body.comment("get length of i-th varchar")
                    .append(scope.getVariable("arg" + i))
                    .invokeVirtual(Slice.class, "length", int.class)
                    .comment("add size of i-th varchar to current position")
                    .intAdd();
        }

        body.comment("pop current position variable")
                .pop()
                .getVariable(concatVariable)
                .retObject();

        return defineClass(definition, Object.class, ImmutableMap.of(), new DynamicClassLoader(ArrayConstructor.class.getClassLoader()));
    }
}
