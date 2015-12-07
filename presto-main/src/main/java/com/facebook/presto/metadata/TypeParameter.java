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

import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class TypeParameter
{
    private final String name;
    private final boolean comparableRequired;
    private final boolean orderableRequired;
    private final String variadicBound;

    @JsonCreator
    public TypeParameter(
            @JsonProperty("name") String name,
            @JsonProperty("comparableRequired") boolean comparableRequired,
            @JsonProperty("orderableRequired") boolean orderableRequired,
            @JsonProperty("variadicBound") @Nullable String variadicBound)
    {
        this.name = requireNonNull(name, "name is null");
        this.comparableRequired = comparableRequired;
        this.orderableRequired = orderableRequired;
        this.variadicBound = variadicBound;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public boolean isComparableRequired()
    {
        return comparableRequired;
    }

    @JsonProperty
    public boolean isOrderableRequired()
    {
        return orderableRequired;
    }

    @JsonProperty
    public String getVariadicBound()
    {
        return variadicBound;
    }

    public Optional<Type> canBind(Type type, boolean allowCoercion)
    {
        return allowCoercion ? canBindAllowingCoercion(type) : canBindNoCoercion(type);
    }

    public Optional<Type> canBindNoCoercion(Type type)
    {
        if (comparableRequired && !type.isComparable()) {
            return Optional.empty();
        }
        if (orderableRequired && !type.isOrderable()) {
            return Optional.empty();
        }
        if (variadicBound != null && !type.getTypeSignature().getBase().equals(variadicBound)) {
            return Optional.empty();
        }
        return Optional.of(type);
    }

    public Optional<Type> canBindAllowingCoercion(Type type)
    {
        Optional<Type> boundType = canBindNoCoercion(type);
        if (!boundType.isPresent()) {
            if (variadicBound == null) {
                return Optional.empty();
            }
            // hack zone - should reuse coercion rules from FunctionRegistry
            // but as we operate on String based variadicBound here and not on
            // explicit types we cannot use it directly.
            // Also some mechanism of determining default values for
            // literal parameters is required if variadic bound is e.g. (DECIMAL(*))
            if (type.getTypeSignature().getBase().equals("unknown")) {
                if (variadicBound.equals("decimal")) {
                    boundType = Optional.of(DecimalType.createDecimalType(1));
                }
                else if (variadicBound.equals("varchar")) {
                    boundType = Optional.of(VarcharType.VARCHAR);
                }
            }
        }
        return boundType;
    }

    @Override
    public String toString()
    {
        String value = name;
        if (comparableRequired) {
            value += ":comparable";
        }
        if (orderableRequired) {
            value += ":orderable";
        }
        if (variadicBound != null) {
            value += ":" + variadicBound + "<*>";
        }
        return value;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TypeParameter other = (TypeParameter) o;

        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.comparableRequired, other.comparableRequired) &&
                Objects.equals(this.orderableRequired, other.orderableRequired) &&
                Objects.equals(this.variadicBound, other.variadicBound);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, comparableRequired, orderableRequired, variadicBound);
    }
}
