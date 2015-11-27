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

import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeParameter;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public final class MapParametricType
        implements ParametricType
{
    public static final MapParametricType MAP = new MapParametricType();

    private MapParametricType()
    {
    }

    @Override
    public String getName()
    {
        return StandardTypes.MAP;
    }

    @Override
    public Type createType(List<TypeParameter> parameters)
    {
        checkArgument(parameters.size() == 2, "Expected two parameters, got %s", parameters);
        Optional<Type> key = parameters.get(0).getType();
        Optional<Type> value = parameters.get(1).getType();
        checkArgument(key.isPresent() && value.isPresent(), "Expected key and type to be types, got %s", parameters);
        return new MapType(key.get(), value.get());
    }
}
