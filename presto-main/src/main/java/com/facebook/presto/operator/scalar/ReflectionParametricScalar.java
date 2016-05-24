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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SignatureBinder;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.annotations.ScalarHeader;
import com.facebook.presto.operator.scalar.annotations.ScalarHeaderAndMethods;
import com.facebook.presto.operator.scalar.annotations.ScalarImplementation;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_IMPLEMENTATION;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ReflectionParametricScalar
        extends SqlScalarFunction
{
    // These three collections implement something similar to partial template specialization from C++, and allow more optimized implementations to be provided for specific types

    // These are implementations for concrete types (they have no unbound type parameters), and have the highest priority when picking an implementation
    private final Map<Signature, ScalarImplementation> exactImplementations;
    // These are implementations for when a type parameter binds to a specific native container type
    private final List<ScalarImplementation> specializedImplementations;
    // These are generic implementations
    private final List<ScalarImplementation> implementations;



    ScalarHeader details;

    private ReflectionParametricScalar(
            Signature signature,
            ScalarHeader details,
            Map<Signature, ScalarImplementation> exactImplementations,
            List<ScalarImplementation> specializedImplementations,
            List<ScalarImplementation> implementations)
    {
        super(signature);
        this.details = details;
        this.exactImplementations = ImmutableMap.copyOf(requireNonNull(exactImplementations, "exactImplementations is null"));
        this.specializedImplementations = ImmutableList.copyOf(requireNonNull(specializedImplementations, "specializedImplementations is null"));
        this.implementations = ImmutableList.copyOf(requireNonNull(implementations, "implementations is null"));
    }

    @Override
    public boolean isHidden()
    {
        return details.isHidden();
    }

    @Override
    public boolean isDeterministic()
    {
        return details.isDeterministic();
    }

    @Override
    public String getDescription()
    {
        return details.getDescription().isPresent() ? details.getDescription().get() : "";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Signature boundSignature = SignatureBinder.bindVariables(getSignature(), boundVariables, arity);
        if (exactImplementations.containsKey(boundSignature)) {
            ScalarImplementation implementation = exactImplementations.get(boundSignature);
            ScalarImplementation.MethodHandleAndConstructor methodHandleAndConstructor = implementation.specialize(boundSignature, boundVariables, typeManager, functionRegistry);
            if (methodHandleAndConstructor != null) {
                return new ScalarFunctionImplementation(implementation.isNullable(), implementation.getNullableArguments(), methodHandleAndConstructor.getMethodHandle(), methodHandleAndConstructor.getConstructor(), isDeterministic());
            }
            else {
                return new ScalarFunctionImplementation(implementation.isNullable(), implementation.getNullableArguments(), implementation.getMethodHandle(), isDeterministic());
            }
        }

        ScalarFunctionImplementation selectedImplementation = null;
        for (ScalarImplementation implementation : specializedImplementations) {
            ScalarImplementation.MethodHandleAndConstructor methodHandle = implementation.specialize(boundSignature, boundVariables, typeManager, functionRegistry);
            if (methodHandle != null) {
                checkCondition(selectedImplementation == null, AMBIGUOUS_FUNCTION_IMPLEMENTATION, "Ambiguous implementation for %s with bindings %s", getSignature(), boundVariables.getTypeVariables());
                selectedImplementation = new ScalarFunctionImplementation(implementation.isNullable(), implementation.getNullableArguments(), methodHandle.getMethodHandle(), methodHandle.getConstructor(), isDeterministic());
            }
        }
        if (selectedImplementation != null) {
            return selectedImplementation;
        }

        for (ScalarImplementation implementation : implementations) {
            ScalarImplementation.MethodHandleAndConstructor methodHandle = implementation.specialize(boundSignature, boundVariables, typeManager, functionRegistry);
            if (methodHandle != null) {
                checkCondition(selectedImplementation == null, AMBIGUOUS_FUNCTION_IMPLEMENTATION, "Ambiguous implementation for %s with bindings %s", getSignature(), boundVariables.getTypeVariables());
                selectedImplementation = new ScalarFunctionImplementation(implementation.isNullable(), implementation.getNullableArguments(), methodHandle.getMethodHandle(), methodHandle.getConstructor(), isDeterministic());
            }
        }
        if (selectedImplementation != null) {
            return selectedImplementation;
        }

        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("Unsupported type parameters (%s) for %s", boundVariables, getSignature()));
    }

    public static List<SqlScalarFunction> parseFunctionDefinitionClass(Class<?> clazz)
    {
        ImmutableList.Builder<SqlScalarFunction> builder = ImmutableList.builder();
        for (ScalarHeaderAndMethods scalar : ScalarHeaderAndMethods.fromFunctionDefinitionClassAnnotations(clazz)) {
            builder.add(parseScalarImplementations(scalar, findConstructors(clazz), clazz.getSimpleName()));
        }
        return builder.build();
    }

    public static List<SqlScalarFunction> parseFunctionSetClass(Class<?> clazz)
    {
        ImmutableList.Builder<SqlScalarFunction> builder = ImmutableList.builder();
        for (ScalarHeaderAndMethods methods : ScalarHeaderAndMethods.fromFunctionSetClassAnnotations(clazz)) {
            builder.add(parseScalarImplementations(methods, findConstructors(clazz), clazz.getSimpleName()));
        }
        return builder.build();
    }

    private static SqlScalarFunction parseScalarImplementations(ScalarHeaderAndMethods scalar, Map<Set<TypeParameter>, Constructor<?>> constructors, String objectName)
    {
        ImmutableMap.Builder<Signature, ScalarImplementation> exactImplementations = ImmutableMap.builder();
        ImmutableList.Builder<ScalarImplementation> specializedImplementations = ImmutableList.builder();
        ImmutableList.Builder<ScalarImplementation> implementations = ImmutableList.builder();
        Signature signature = null;
        ScalarHeader header = scalar.getHeader();
        checkArgument(!header.getName().isEmpty());

        for (Method method : scalar.getMethods()) {
            ScalarImplementation implementation = ScalarImplementation.Parser.parseImplementation(header.getName(), method, constructors);
            if (implementation.getSignature().getTypeVariableConstraints().isEmpty()
                    && implementation.getSignature().getArgumentTypes().stream().noneMatch(TypeSignature::isCalculated)) {
                exactImplementations.put(implementation.getSignature(), implementation);
                continue;
            }
            if (signature == null) {
                signature = implementation.getSignature();
            }
            else {
                checkArgument(implementation.getSignature().equals(signature), "Implementations with type parameters must all have matching signatures. %s does not match %s", implementation.getSignature(), signature);
            }
            if (implementation.hasSpecializedTypeParameters()) {
                specializedImplementations.add(implementation);
            }
            else {
                implementations.add(implementation);
            }
        }

        Map<Signature, ScalarImplementation> exactImplementationsMap = exactImplementations.build();
        if (signature == null) {
            checkArgument(!exactImplementationsMap.isEmpty(), "Implementation of ScalarFunction %s must be parametric or exact implementation.", objectName);
            checkArgument(exactImplementationsMap.size() == 1, "It is not allowed to use clases without generic signature.");
            Map.Entry<Signature, ScalarImplementation> onlyImplementation = exactImplementationsMap.entrySet().iterator().next();
            signature = onlyImplementation.getKey();
            //return SqlScalarFunction.create(onlyImplementation.getKey(), description, hidden, implementation.getMethodHandle(), Optional.empty(), deterministic, implementation.isNullable(), implementation.getNullableArguments());
        }

        return new ReflectionParametricScalar(
                signature,
                header,
                exactImplementationsMap,
                specializedImplementations.build(),
                implementations.build());
    }

    private static Map<Set<TypeParameter>, Constructor<?>> findConstructors(Class<?> clazz)
    {
        ImmutableMap.Builder<Set<TypeParameter>, Constructor<?>> builder = ImmutableMap.builder();
        for (Constructor<?> constructor : clazz.getConstructors()) {
            Set<TypeParameter> typeParameters = new HashSet<>();
            Stream.of(constructor.getAnnotationsByType(TypeParameter.class))
                    .forEach(typeParameters::add);
            builder.put(typeParameters, constructor);
        }
        return builder.build();
    }


}
