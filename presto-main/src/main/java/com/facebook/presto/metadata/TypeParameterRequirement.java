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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.facebook.presto.metadata.TypeParameterRequirement.ConstraintKind.LONG;
import static com.facebook.presto.metadata.TypeParameterRequirement.ConstraintKind.TYPE;

public final class TypeParameterRequirement
{
    public enum ConstraintKind
    {
        TYPE,
        LONG
    }

    private final String name;
    private final BaseConstraint value;

    public TypeParameterRequirement(
            @JsonProperty("name") String name,
            @JsonProperty("constraint") BaseConstraint value)
    {
        this.name = name;
        this.value = value;
    }

    public static TypeParameterRequirement typeConstraint(String name, boolean comparableRequired, boolean orderableRequired, @Nullable String variadicBound)
    {
        return new TypeParameterRequirement(name, new TypeConstraint(comparableRequired, orderableRequired, variadicBound));
    }

    public static TypeParameterRequirement longConstraint(String name, @Nullable String computation)
    {
        return new TypeParameterRequirement(name, new LongConstraint(computation));
    }

    public TypeConstraint getTypeConstraint()
    {
        return (TypeConstraint) value;
    }

    public LongConstraint getLongConstraint()
    {
        return (LongConstraint) value;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public ConstraintKind getConstraintKind()
    {
        return value.getKind();
    }

    @JsonProperty
    public Object getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return name + value.toString();
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
        TypeParameterRequirement that = (TypeParameterRequirement) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, value);
    }

    @JsonSubTypes({
            @JsonSubTypes.Type(value = TypeConstraint.class, name = "type"),
            @JsonSubTypes.Type(value = LongConstraint.class, name = "long")
    })

    interface BaseConstraint
    {
        ConstraintKind getKind();
    }

    public static class TypeConstraint
            implements BaseConstraint
    {
        private final boolean comparableRequired;
        private final boolean orderableRequired;
        private final String variadicBound;

        @JsonCreator
        public TypeConstraint(
                @JsonProperty("comparableRequired") boolean comparableRequired,
                @JsonProperty("orderableRequired") boolean orderableRequired,
                @JsonProperty("variadicBound") @Nullable String variadicBound)
        {
            this.comparableRequired = comparableRequired;
            this.orderableRequired = orderableRequired;
            this.variadicBound = variadicBound;
        }

        @Override
        public ConstraintKind getKind()
        {
            return TYPE;
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

        public boolean canBind(Type type)
        {
            if (comparableRequired && !type.isComparable()) {
                return false;
            }
            if (orderableRequired && !type.isOrderable()) {
                return false;
            }
            if (variadicBound != null && !type.getTypeSignature().getBase().equals(variadicBound)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString()
        {
            String value = "";
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

            TypeConstraint other = (TypeConstraint) o;

            return Objects.equals(this.comparableRequired, other.comparableRequired) &&
                    Objects.equals(this.orderableRequired, other.orderableRequired) &&
                    Objects.equals(this.variadicBound, other.variadicBound);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(comparableRequired, orderableRequired, variadicBound);
        }
    }

    public static class LongConstraint
            implements BaseConstraint
    {
        private final String calculation;

        @JsonCreator
        public LongConstraint(@JsonProperty("calculation") @Nullable String calculation)
        {
            this.calculation = calculation;
        }

        @Override
        public ConstraintKind getKind()
        {
            return LONG;
        }

        public String getCalculation()
        {
            return calculation;
        }

        @Override
        public String toString()
        {
            return calculation;
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
            LongConstraint that = (LongConstraint) o;
            return Objects.equals(calculation, that.calculation);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(calculation);
        }
    }
}
