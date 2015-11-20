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
package com.facebook.presto.spi.type;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;

public class TypeParameterSignature
{
    private final Optional<TypeSignature> typeSignature;
    private final Optional<Long> longLiteral;

    public static TypeParameterSignature of(TypeSignature typeSignature)
    {
        return new TypeParameterSignature(Optional.of(typeSignature), Optional.empty());
    }

    public static TypeParameterSignature of(long longLiteral)
    {
        return new TypeParameterSignature(Optional.empty(), Optional.of(longLiteral));
    }

    private TypeParameterSignature(Optional<TypeSignature> typeSignature, Optional<Long> longLiteral)
    {
        if (typeSignature.isPresent() && longLiteral.isPresent()) {
            throw new IllegalStateException(
                    format("typeSignature and longLiteral are mutual exclusive but [%s, %s] was found",
                            typeSignature.get(),
                            longLiteral.get()));
        }
        this.typeSignature = typeSignature;
        this.longLiteral = longLiteral;
    }

    @Override
    public String toString()
    {
        if (typeSignature.isPresent()) {
            return typeSignature.get().toString();
        }
        else {
            return longLiteral.get().toString();
        }
    }

    public Optional<TypeSignature> getTypeSignature()
    {
        return typeSignature;
    }

    public Optional<Long> getLongLiteral()
    {
        return longLiteral;
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

        TypeParameterSignature other = (TypeParameterSignature) o;

        return Objects.equals(this.typeSignature, other.typeSignature) &&
                Objects.equals(this.longLiteral, other.longLiteral);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(typeSignature, longLiteral);
    }

    public TypeParameterSignature bindParameters(Map<String, Type> boundParameters)
    {
        if (typeSignature.isPresent()) {
            return TypeParameterSignature.of(typeSignature.get().bindParameters(boundParameters));
        }
        else {
            return this;
        }
    }
}
