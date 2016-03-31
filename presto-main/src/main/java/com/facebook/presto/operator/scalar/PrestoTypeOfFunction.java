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

import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;

@Description("Return type of scalar parameter as VARCHAR")
@ScalarFunction("presto_typeof")
public final class PrestoTypeOfFunction
{
    private PrestoTypeOfFunction() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice typeof(
            @TypeParameter("T") Type type,
            @SqlType("T") Object value)
    {
        return Slices.wrappedBuffer(ByteBuffer.wrap(type.toString().getBytes()));
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice typeof(
            @TypeParameter("T") Type type,
            @SqlType("T") long value)
    {
        return typeof(type, (Object) value);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice typeof(
            @TypeParameter("T") Type type,
            @SqlType("T") double value)
    {
        return typeof(type, (Object) value);
    }

    @TypeParameter("T")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice typeof(
            @TypeParameter("T") Type type,
            @SqlType("T") boolean value)
    {
        return typeof(type, (Object) value);
    }
}
