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

public class TypeSignatureParameter
{
    private final Optional<TypeSignature> typeSignature;
    private final Optional<Long> longLiteral;
    private final Optional<NamedTypeSignature> namedTypeSignature;

    private TypeSignatureParameter(
            Optional<TypeSignature> typeSignature,
            Optional<Long> longLiteral,
            Optional<NamedTypeSignature> namedTypeSignature)
    {
        int presentCount = (typeSignature.isPresent() ? 1 : 0) +
                (longLiteral.isPresent() ? 1 : 0) +
                (namedTypeSignature.isPresent() ? 1 : 0);

        if (presentCount != 1) {
            throw new IllegalStateException(
                    format("Parameters are mutual exclusive but [%s, %s, %s] was found",
                            typeSignature,
                            longLiteral,
                            namedTypeSignature));
        }
        this.typeSignature = typeSignature;
        this.longLiteral = longLiteral;
        this.namedTypeSignature = namedTypeSignature;
    }

    public static TypeSignatureParameter of(TypeSignature typeSignature)
    {
        return new TypeSignatureParameter(Optional.of(typeSignature), Optional.empty(), Optional.empty());
    }

    public static TypeSignatureParameter of(long longLiteral)
    {
        return new TypeSignatureParameter(Optional.empty(), Optional.of(longLiteral), Optional.empty());
    }

    public static TypeSignatureParameter of(NamedTypeSignature namedTypeSignature)
    {
        return new TypeSignatureParameter(Optional.empty(), Optional.empty(), Optional.of(namedTypeSignature));
    }

    @Override
    public String toString()
    {
        if (typeSignature.isPresent()) {
            return typeSignature.get().toString();
        }
        else if (longLiteral.isPresent()) {
            return longLiteral.get().toString();
        }
        else if (namedTypeSignature.isPresent()) {
            return namedTypeSignature.get().toString();
        }
        else {
            throw new UnsupportedOperationException("Unsupported TypeSignatureParameter in toString");
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

    public Optional<NamedTypeSignature> getNamedTypeSignature()
    {
        return namedTypeSignature;
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

        TypeSignatureParameter other = (TypeSignatureParameter) o;

        return Objects.equals(this.typeSignature, other.typeSignature) &&
                Objects.equals(this.longLiteral, other.longLiteral) &&
                Objects.equals(this.namedTypeSignature, other.namedTypeSignature);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(typeSignature, longLiteral, namedTypeSignature);
    }

    public TypeSignatureParameter bindParameters(Map<String, Type> boundParameters)
    {
        if (typeSignature.isPresent()) {
            return TypeSignatureParameter.of(typeSignature.get().bindParameters(boundParameters));
        }
        else if (namedTypeSignature.isPresent()) {
            return TypeSignatureParameter.of(new NamedTypeSignature(
                    namedTypeSignature.get().getName(),
                    namedTypeSignature.get().getTypeSignature().bindParameters(boundParameters)));
        }
        else {
            return this;
        }
    }
}
