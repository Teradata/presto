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

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.sql.gen.CallSiteBinder;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.CompilerUtils.makeClassName;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Objects.requireNonNull;

public final class ScalarFunctionImplementation
{
    private static final String GENERATED_METHOD_NAME = "scalar";

    private final boolean nullable;
    private final List<Boolean> nullableArguments;
    private final MethodHandle methodHandle;
    private final Optional<MethodHandle> instanceFactory;
    private final boolean deterministic;
    private final boolean returnValueAsParameter;
    private final Optional<Integer> returnValueSliceLength;
    private ScalarFunctionImplementation returnValuesAsReturnImplementation;

    public ScalarFunctionImplementation(boolean nullable, List<Boolean> nullableArguments, MethodHandle methodHandle, boolean deterministic)
    {
        this(nullable, nullableArguments, methodHandle, Optional.empty(), deterministic);
    }

    public ScalarFunctionImplementation(boolean nullable, List<Boolean> nullableArguments, MethodHandle methodHandle, Optional<MethodHandle> instanceFactory, boolean deterministic)
    {
        this(nullable, nullableArguments, methodHandle, instanceFactory, deterministic, false, Optional.empty());
    }

    public ScalarFunctionImplementation(
            boolean nullable,
            List<Boolean> nullableArguments,
            MethodHandle methodHandle,
            Optional<MethodHandle> instanceFactory,
            boolean deterministic,
            boolean returnValueAsParameter,
            Optional<Integer> returnValueSliceLength)
    {
        this.nullable = nullable;
        this.nullableArguments = ImmutableList.copyOf(requireNonNull(nullableArguments, "nullableArguments is null"));
        this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
        this.instanceFactory = requireNonNull(instanceFactory, "instanceFactory is null");
        this.deterministic = deterministic;
        this.returnValueAsParameter = returnValueAsParameter;
        this.returnValueSliceLength = returnValueSliceLength;

        int expectedParametersCount = methodHandle.type().parameterCount();
        if (returnValueAsParameter) {
            expectedParametersCount--;
        }
        if (instanceFactory.isPresent()) {
            expectedParametersCount--;
        }
        if (methodHandle.type().parameterCount() >= 1 && methodHandle.type().parameterType(0) == ConnectorSession.class) {
            expectedParametersCount--;
        }
        checkArgument(nullableArguments.size() == expectedParametersCount, "method handle parameters count mismatch");

        if (returnValueAsParameter) {
            checkArgument(!nullable, "nullable not supported for functions returning value as parameter");
            checkArgument(getMethodHandle().type().returnType() == void.class, "methodHandle returns non-void for function returning value as parameter");
            int returnParameterPos = instanceFactory.isPresent() ? 1 : 0;
            checkArgument(getMethodHandle().type().parameterType(returnParameterPos) == Slice.class, "only Slice parameters supported as return parameter");
            checkArgument(returnValueSliceLength.isPresent(), "returnValuesSliceLength not set");
        }
        else {
            checkArgument(!returnValueSliceLength.isPresent(), "returnValueSliceLength set for non Slice return parameter");
        }

        if (instanceFactory.isPresent()) {
            Class<?> instanceType = instanceFactory.get().type().returnType();
            checkArgument(instanceType.equals(methodHandle.type().parameterType(0)), "methodHandle is not an instance method");
        }
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public List<Boolean> getNullableArguments()
    {
        return nullableArguments;
    }

    public MethodHandle getMethodHandle()
    {
        return methodHandle;
    }

    public Optional<MethodHandle> getInstanceFactory()
    {
        return instanceFactory;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    public boolean isReturnValueAsParameter()
    {
        return returnValueAsParameter;
    }

    public Optional<Integer> getReturnValueSliceLength()
    {
        return returnValueSliceLength;
    }

    public synchronized ScalarFunctionImplementation ensureReturnValueAsReturn()
    {
        if (!returnValueAsParameter) {
            return this;
        }
        if (returnValuesAsReturnImplementation == null) {
            MethodHandle newMethodHandle = generateReturnAsReturnMethodHandler(this.methodHandle, returnValueSliceLength.get(), instanceFactory.isPresent());
            returnValuesAsReturnImplementation = new ScalarFunctionImplementation(
                    nullable,
                    nullableArguments,
                    newMethodHandle,
                    instanceFactory,
                    deterministic,
                    false,
                    Optional.empty());
        }
        return returnValuesAsReturnImplementation;
    }

    private MethodHandle generateReturnAsReturnMethodHandler(MethodHandle originalMethodHandle, int returnValueSliceLength, boolean usesInstance)
    {
        MethodType originalMethodType = originalMethodHandle.type();
        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName("ScalarImplementation"),
                type(Object.class));

        List<Parameter> newParameters = newArrayList();
        for (int i = 0; i < originalMethodType.parameterCount(); ++i) {
            if (i != (usesInstance ? 1 : 0)) {
                newParameters.add(arg("p" + i, originalMethodType.parameterType(i)));
            }
        }
        MethodDefinition methodDefinition = classDefinition.declareMethod(a(PUBLIC, STATIC), GENERATED_METHOD_NAME, type(Slice.class), newParameters);

        Variable returnVariable = methodDefinition.getScope().declareVariable(Slice.class, "returnValue");
        BytecodeBlock methodBody = methodDefinition.getBody();

        methodBody.append(constantInt(returnValueSliceLength))
                .invokeStatic(Slices.class, "allocate", Slice.class, int.class)
                .putVariable(returnVariable);

        if (usesInstance) {
            methodBody.append(newParameters.get(0));
        }
        methodBody.getVariable(returnVariable);
        for (int i = usesInstance ? 1 : 0; i < newParameters.size(); ++i) {
            methodBody.append(newParameters.get(i));
        }
        methodBody.append(invoke(callSiteBinder.bind(originalMethodHandle), "originalMethod"));
        methodBody.getVariable(returnVariable);
        methodBody.ret(Slice.class);

        Class<?> generatedClass = defineClass(classDefinition, Object.class, callSiteBinder.getBindings(), getClass().getClassLoader());
        return lookupNewMethodHandle(generatedClass);
    }

    private MethodHandle lookupNewMethodHandle(Class<?> generatedClass)
    {
        MethodHandle newMethodHandle = null;
        for (Method method : generatedClass.getDeclaredMethods()) {
            if (method.getName().equals(GENERATED_METHOD_NAME)) {
                try {
                    newMethodHandle = MethodHandles.lookup().unreflect(method);
                }
                catch (IllegalAccessException e) {
                    Throwables.propagate(e);
                }
                break;
            }
        }
        requireNonNull(newMethodHandle, "Did not find scalar method in declared class");
        return newMethodHandle;
    }
}
