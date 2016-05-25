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
import com.facebook.presto.operator.scalar.annotations.ScalarImplementation;
import com.facebook.presto.operator.scalar.annotations.ScalarImplementations;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.TypeManager;

import static com.facebook.presto.spi.StandardErrorCode.AMBIGUOUS_FUNCTION_IMPLEMENTATION;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ParametricScalar
        extends SqlScalarFunction
{
    ScalarHeader details;
    ScalarImplementations implementations;

    public ParametricScalar(
            Signature signature,
            ScalarHeader details,
            ScalarImplementations implementations)
    {
        super(signature);
        this.details = requireNonNull(details);
        this.implementations = requireNonNull(implementations);
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
        if (implementations.getExactImplementations().containsKey(boundSignature)) {
            ScalarImplementation implementation = implementations.getExactImplementations().get(boundSignature);
            ScalarImplementation.MethodHandleAndConstructor methodHandleAndConstructor = implementation.specialize(boundSignature, boundVariables, typeManager, functionRegistry);
            if (methodHandleAndConstructor != null) {
                return new ScalarFunctionImplementation(implementation.isNullable(), implementation.getNullableArguments(), methodHandleAndConstructor.getMethodHandle(), methodHandleAndConstructor.getConstructor(), isDeterministic());
            }
            else {
                return new ScalarFunctionImplementation(implementation.isNullable(), implementation.getNullableArguments(), implementation.getMethodHandle(), isDeterministic());
            }
        }

        ScalarFunctionImplementation selectedImplementation = null;
        for (ScalarImplementation implementation : implementations.getSpecializedImplementations()) {
            ScalarImplementation.MethodHandleAndConstructor methodHandle = implementation.specialize(boundSignature, boundVariables, typeManager, functionRegistry);
            if (methodHandle != null) {
                checkCondition(selectedImplementation == null, AMBIGUOUS_FUNCTION_IMPLEMENTATION, "Ambiguous implementation for %s with bindings %s", getSignature(), boundVariables.getTypeVariables());
                selectedImplementation = new ScalarFunctionImplementation(implementation.isNullable(), implementation.getNullableArguments(), methodHandle.getMethodHandle(), methodHandle.getConstructor(), isDeterministic());
            }
        }
        if (selectedImplementation != null) {
            return selectedImplementation;
        }

        for (ScalarImplementation implementation : implementations.getGenericImplementations()) {
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
}
