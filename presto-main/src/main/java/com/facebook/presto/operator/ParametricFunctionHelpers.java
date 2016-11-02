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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.operator.annotations.ImplementationDependency;
import com.facebook.presto.spi.type.TypeManager;

import java.lang.invoke.MethodHandle;
import java.util.List;

public class ParametricFunctionHelpers
{
    private ParametricFunctionHelpers() {}

    public static MethodHandle bindDependencies(MethodHandle inputHandle, List<ImplementationDependency> dependencies, BoundVariables variables, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        for (ImplementationDependency dependency : dependencies) {
            inputHandle = inputHandle.bindTo(dependency.resolve(variables, typeManager, functionRegistry));
        }
        return inputHandle;
    }
}
