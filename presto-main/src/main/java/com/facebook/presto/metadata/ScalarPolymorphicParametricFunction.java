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

import com.facebook.presto.metadata.ParametricFunctionBuilder.ExtraParametersFunction;
import com.facebook.presto.metadata.ParametricFunctionBuilder.MethodsWithExtraParametersFunction;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.type.TypeUtils.extractLiteralParameters;
import static com.facebook.presto.type.TypeUtils.resolveCalculatedType;
import static com.facebook.presto.type.TypeUtils.resolveType;
import static com.facebook.presto.type.TypeUtils.resolveTypes;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static java.util.Locale.US;
import static java.util.stream.Collectors.toMap;

class ScalarPolymorphicParametricFunction
        implements ParametricFunction
{
    private final Signature signature;
    private final String description;
    private final boolean hidden;
    private final boolean deterministic;
    private final boolean nullableResult;
    private final List<Boolean> nullableArguments;
    private final List<MethodsWithExtraParametersFunction> methodsWithExtraParametersFunctions;

    ScalarPolymorphicParametricFunction(Signature signature, String description, boolean hidden, boolean deterministic,
            boolean nullableResult, List<Boolean> nullableArguments, List<MethodsWithExtraParametersFunction> methodsWithExtraParametersFunctions)
    {
        this.signature = checkNotNull(signature, "signature is null");
        this.description = description;
        this.hidden = hidden;
        this.deterministic = deterministic;
        this.nullableResult = nullableResult;
        this.nullableArguments = checkNotNull(nullableArguments, "nullableArguments is null");
        this.methodsWithExtraParametersFunctions = checkNotNull(methodsWithExtraParametersFunctions, "methodsWithExtraParametersFunctions is null");
    }

    @Override
    public Signature getSignature()
    {
        return signature;
    }

    @Override
    public boolean isScalar()
    {
        return true;
    }

    @Override
    public boolean isAggregate()
    {
        return false;
    }

    @Override
    public boolean isHidden()
    {
        return hidden;
    }

    @Override
    public boolean isApproximate()
    {
        return false;
    }

    @Override
    public boolean isWindow()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return deterministic;
    }

    @Override
    public boolean isUnbound()
    {
        return !signature.getTypeParameters().isEmpty() || signature.isVariableArity() || signature.isReturnTypeOrAnyArgumentTypeCalculated();
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, List<TypeSignature> parameterTypes, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Map<String, OptionalLong> inputLiteralParameters = signature.bindLiteralParameters(parameterTypes);
        TypeSignature calculatedReturnType = resolveCalculatedType(signature.getReturnType(), inputLiteralParameters);
        Map<String, OptionalLong> outputLiteralParameters = extractLiteralParameters(signature.getReturnType(), calculatedReturnType);
        outputLiteralParameters.entrySet().removeAll(inputLiteralParameters.entrySet());
        Map<String, OptionalLong> literalParameters = ImmutableMap.<String, OptionalLong>builder()
                .putAll(inputLiteralParameters)
                .putAll(outputLiteralParameters)
                .build();

        List<Type> resolvedParameterTypes = resolveTypes(parameterTypes, typeManager);
        Type resolvedReturnType;
        if (types.containsKey(calculatedReturnType.getBase())) {
            resolvedReturnType = types.get(calculatedReturnType.getBase());
        }
        else {
            resolvedReturnType = resolveType(calculatedReturnType, typeManager);
        }

        Optional<Method> matchingMethod = Optional.empty();
        Optional<ExtraParametersFunction> matchingExtraParametersFunction = Optional.empty();
        for (MethodsWithExtraParametersFunction methodsWithExtraParametersFunction : methodsWithExtraParametersFunctions) {
            for (Method method : methodsWithExtraParametersFunction.getMethods()) {
                if (matchesParameterAndReturnTypes(method, resolvedParameterTypes, resolvedReturnType)) {
                    if (matchingMethod.isPresent()) {
                        throw new IllegalStateException("two matching methods (" + matchingMethod.get().getName() + " and " + method.getName() + ") for parameter types " + parameterTypes);
                    }
                    matchingMethod = Optional.of(method);
                    matchingExtraParametersFunction = methodsWithExtraParametersFunction.getExtraParametersFunction();
                }
            }
        }
        checkState(matchingMethod.isPresent(), "no matching method for parameter types %s", parameterTypes);

        List<Object> extraParameters = computeExtraParameters(types, literalParameters, matchingExtraParametersFunction);
        MethodHandle matchingMethodHandle = applyExtraParameters(matchingMethod, extraParameters);

        return new FunctionInfo(signature, description, hidden, matchingMethodHandle, deterministic, nullableResult, nullableArguments);
    }

    private boolean matchesParameterAndReturnTypes(Method method, List<Type> resolvedTypes, Type returnType)
    {
        checkState(method.getParameterCount() >= resolvedTypes.size(),
                "method %s has not enough arguments: %s (should have at least %s)", method.getName(), method.getParameterCount(), resolvedTypes.size());

        Class<?>[] methodParameterJavaTypes = method.getParameterTypes();
        for (int i = 0; i < resolvedTypes.size(); ++i) {
            if (!methodParameterJavaTypes[i].equals(resolvedTypes.get(i).getJavaType())) {
                return false;
            }
        }

        return method.getReturnType().equals(returnType.getJavaType());
    }

    private List<Object> computeExtraParameters(Map<String, Type> types, Map<String, OptionalLong> boundLiterals, Optional<ExtraParametersFunction> matchingExtraParametersFunction)
    {
        Map<String, Long> filteredBoundLiterals = boundLiterals.entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(toMap(entry -> entry.getKey().toLowerCase(US), entry -> entry.getValue().getAsLong()));
        return matchingExtraParametersFunction.map(function -> function.computeExtraParameters(types, filteredBoundLiterals)).orElse(emptyList());
    }

    private MethodHandle applyExtraParameters(Optional<Method> matchingMethod, List<Object> extraParameters)
    {
        int expectedNumberOfArguments = signature.getArgumentTypes().size() + extraParameters.size();
        int matchingMethodParameterCount = matchingMethod.get().getParameterCount();
        checkState(matchingMethodParameterCount == expectedNumberOfArguments,
                "method %s has invalid number of arguments: %s (should have %s)", matchingMethod.get().getName(), matchingMethodParameterCount, expectedNumberOfArguments);

        MethodHandle matchingMethodHandle = Reflection.methodHandle(matchingMethod.get());
        matchingMethodHandle = MethodHandles.insertArguments(matchingMethodHandle, signature.getArgumentTypes().size(), extraParameters.toArray());
        return matchingMethodHandle;
    }
}
