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
package com.facebook.presto.operator.scalar.annotations;

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.ParametricScalar;
import com.facebook.presto.operator.scalar.TypeParameter;
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

import static com.google.common.base.Preconditions.checkArgument;

public class ScalarFromAnnotationsParser {
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
        ImmutableList.Builder<ScalarImplementation> genericImplementations = ImmutableList.builder();
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
                genericImplementations.add(implementation);
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

        ScalarImplementations implementations = new ScalarImplementations(exactImplementations.build(), specializedImplementations.build(), genericImplementations.build());
        return new ParametricScalar(
                signature,
                header,
                implementations);
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
