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
package com.facebook.presto.tests.datatype;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Strings.padEnd;
import static java.util.Optional.empty;
import static java.util.Optional.of;

public class DataType<T>
{
    private Function<T, String> toLiteral;
    private Function<T, ?> toResult;
    private String insertType;
    private Type prestoResultType;

    public static String quote(String value)
    {
        return "'" + value + "'";
    }

    public static DataType<String> varcharDataType(int size)
    {
        return varcharDataType(size, "");
    }

    public static DataType<String> varcharDataType(int size, String properties)
    {
        return varcharDataType(of(size), properties);
    }

    public static DataType<String> varcharDataType()
    {
        return varcharDataType(empty(), "");
    }

    private static DataType<String> varcharDataType(Optional<Integer> length, String properties)
    {
        String prefix = length.map(size -> "varchar(" + size + ")").orElse("varchar");
        String suffix = properties.isEmpty() ? "" : " " + properties;
        VarcharType varcharType = length.map(VarcharType::createVarcharType).orElse(createUnboundedVarcharType());
        return stringDataType(prefix + suffix, varcharType);
    }

    public static DataType<String> stringDataType(String insertType, Type prestoResultType)
    {
        return dataType(insertType, prestoResultType, DataType::quote, Function.identity());
    }

    public static DataType<String> charDataType(int length)
    {
        return charDataType(length, "");
    }

    public static DataType<String> charDataType(int length, String properties)
    {
        String suffix = properties.isEmpty() ? "" : " " + properties;
        return charDataType("char(" + length + ")" + suffix, length);
    }

    public static DataType<String> charDataType(String insertType, int length)
    {
        return dataType(insertType, createCharType(length), DataType::quote, input -> padEnd(input, length, ' '));
    }

    public static <T> DataType<T> dataType(String insertType, Type prestoResultType, Function<T, String> toLiteral, Function<T, ?> toResult)
    {
        return new DataType<>(insertType, prestoResultType, toLiteral, toResult);
    }

    private DataType(String insertType, Type prestoResultType, Function<T, String> toLiteral, Function<T, ?> toResult)
    {
        this.insertType = insertType;
        this.prestoResultType = prestoResultType;
        this.toLiteral = toLiteral;
        this.toResult = toResult;
    }

    public String toLiteral(T inputValue)
    {
        return toLiteral.apply(inputValue);
    }

    public Object toResult(T inputValue)
    {
        return toResult.apply(inputValue);
    }

    public String getInsertType()
    {
        return insertType;
    }

    public Type getPrestoResultType()
    {
        return prestoResultType;
    }
}
