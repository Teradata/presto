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
package com.facebook.presto.type;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.metadata.TypeVariableConstraint;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.metadata.SignatureBinder.bindVariables;
import static com.facebook.presto.type.TypeUtils.resolveTypes;
import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.util.Collections.nCopies;

public final class NullFunction
        extends SqlScalarFunction
{
    /**
     * Creates function that statically returns null
     */
    public static SqlScalarFunction create(String functionName, List<String> argumentTypes, String returnType)
    {
        List<TypeVariableConstraint> typeVariables = argumentTypes.contains("T") ? ImmutableList.of(typeVariable("T")) : ImmutableList.of();
        return new NullFunction(functionName, argumentTypes, typeVariables, returnType);
    }

    private NullFunction(String functionName, List<String> argumentTypes, List<TypeVariableConstraint> typeVariables, String returnType)
    {
        super(functionName, typeVariables, ImmutableList.of(), returnType, argumentTypes, false);
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        List<TypeSignature> resolvedParameterTypeSignatures = bindVariables(getSignature().getArgumentTypes(), boundVariables);
        List<Type> resolvedParameterTypes = resolveTypes(resolvedParameterTypeSignatures, typeManager);
        TypeSignature resolvedReturnTypeSignature = bindVariables(getSignature().getReturnType(), boundVariables);
        Type resolvedReturnType = typeManager.getType(resolvedReturnTypeSignature);

        List<Class<?>> parametersJavaTypes = resolvedParameterTypes.stream()
                .map((prestoType) -> Primitives.wrap(prestoType.getJavaType()))
                .collect(Collectors.toList());
        Class<?> returnJavaType = Primitives.wrap(resolvedReturnType.getJavaType());

        MethodHandle implementation = dropArguments(constant(returnJavaType, null), 0, parametersJavaTypes);
        return new ScalarFunctionImplementation(true, nCopies(resolvedParameterTypes.size(), Boolean.TRUE), implementation, true);
    }

    @Override
    public boolean isHidden()
    {
        return true;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Unknown operator";
    }
}
