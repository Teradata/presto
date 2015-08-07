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

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public final class ParametricFunctionBuilder
{
    private final Class<?> clazz;
    private Signature signature;
    private String description;
    private Optional<Boolean> hidden = Optional.empty();
    private boolean deterministic;
    private boolean nullableResult;
    private List<Boolean> nullableArguments = emptyList();
    private List<MethodsWithExtraParametersFunction> methodsWithExtraParametersFunctions = newArrayList();

    public ParametricFunctionBuilder(Class<?> clazz)
    {
        this.clazz = clazz;
    }

    public ParametricFunctionBuilder signature(Signature signature)
    {
        this.signature = checkNotNull(signature, "signature is null");
        this.hidden = Optional.of(hidden.orElse(isOperator(signature)));
        return this;
    }

    public ParametricFunctionBuilder description(String description)
    {
        this.description = description;
        return this;
    }

    public ParametricFunctionBuilder hidden(boolean hidden)
    {
        this.hidden = Optional.of(hidden);
        return this;
    }

    public ParametricFunctionBuilder deterministic(boolean deterministic)
    {
        this.deterministic = deterministic;
        return this;
    }

    public ParametricFunctionBuilder nullableResult(boolean nullableResult)
    {
        this.nullableResult = nullableResult;
        return this;
    }

    public ParametricFunctionBuilder nullableArguments(boolean... nullableArguments)
    {
        checkNotNull(nullableArguments, "nullableArguments is null");

        ImmutableList.Builder<Boolean> nullableArgumentsBuilder = ImmutableList.builder();
        for (boolean nullableArgument : nullableArguments) {
            nullableArgumentsBuilder.add(nullableArgument);
        }
        this.nullableArguments = nullableArgumentsBuilder.build();
        return this;
    }

    public ParametricFunctionBuilder nullableArguments(List<Boolean> nullableArguments)
    {
        this.nullableArguments = copyOf(checkNotNull(nullableArguments, "nullableArguments is null"));
        return this;
    }

    public ParametricFunctionBuilder methods(String... methodNames)
    {
        return methods(asList(checkNotNull(methodNames, "methodNames is null")));
    }

    public ParametricFunctionBuilder methods(List<String> methodNames)
    {
        checkNotNull(methodNames, "methodNames is null");

        List<Method> matchingMethods = asList(clazz.getMethods()).stream()
                .filter(method -> methodNames.contains(method.getName()))
                .collect(toList());
        List<String> matchingMethodNames = matchingMethods.stream()
                .map(Method::getName)
                .collect(toList());

        for (String methodName : methodNames) {
            checkState(matchingMethodNames.contains(methodName), "method %s was not found in class %s", methodName, clazz);
        }

        methodsWithExtraParametersFunctions.add(new MethodsWithExtraParametersFunction(matchingMethods));
        return this;
    }

    public ParametricFunctionBuilder extraParameters(ExtraParametersFunction extraParametersFunction)
    {
        checkNotNull(extraParametersFunction, "extraParametersFunction is null");
        checkState(!methodsWithExtraParametersFunctions.isEmpty(), "no methods are selected (call methods() first)");
        checkState(!getLast(methodsWithExtraParametersFunctions).getExtraParametersFunction().isPresent(), "extraParameters already defined for selected methods");

        getLast(methodsWithExtraParametersFunctions).setExtraParametersFunction(extraParametersFunction);
        return this;
    }

    public ParametricFunction build()
    {
        checkState(signature != null, "signature is null");

        if (nullableArguments.isEmpty()) {
            nullableArguments = Collections.nCopies(signature.getArgumentTypes().size(), false);
        }

        return new ScalarPolymorphicParametricFunction(signature, description, hidden.orElse(false), deterministic, nullableResult, nullableArguments, methodsWithExtraParametersFunctions);
    }

    private static boolean isOperator(Signature signature)
    {
        for (OperatorType operator : OperatorType.values()) {
            if (signature.getName().equals(FunctionRegistry.mangleOperatorName(operator))) {
                return true;
            }
        }

        return false;
    }

    @FunctionalInterface
    public interface ExtraParametersFunction
    {
        List<Object> computeExtraParameters(Map<String, Type> typeMappings, Map<String, Long> literalMappings);
    }

    static class MethodsWithExtraParametersFunction
    {
        private final List<Method> methods;
        private Optional<ExtraParametersFunction> extraParametersFunction = Optional.empty();

        private MethodsWithExtraParametersFunction(List<Method> methods)
        {
            this.methods = methods;
        }

        List<Method> getMethods()
        {
            return methods;
        }

        Optional<ExtraParametersFunction> getExtraParametersFunction()
        {
            return extraParametersFunction;
        }

        private void setExtraParametersFunction(ExtraParametersFunction extraParametersFunction)
        {
            this.extraParametersFunction = Optional.of(extraParametersFunction);
        }
    }
}
