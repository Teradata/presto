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

import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;

public class TypeParameter
{
    private final Optional<Type> typeSignature;
    private final Optional<Long> longLiteral;
    private final Optional<NamedType> namedType;
    private final Optional<TypeLiteralCalculation> literalCalculation;

    private TypeParameter(
            Optional<Type> typeSignature,
            Optional<Long> longLiteral,
            Optional<NamedType> namedType,
            Optional<TypeLiteralCalculation> literalCalculation)
    {
        int presentCount = (typeSignature.isPresent() ? 1 : 0) +
                (longLiteral.isPresent() ? 1 : 0) +
                (namedType.isPresent() ? 1 : 0) +
                (literalCalculation.isPresent() ? 1 : 0);

        if (presentCount != 1) {
            throw new IllegalStateException(
                    format("Parameters are mutual exclusive but [%s, %s, %s, %s] was found",
                            typeSignature,
                            longLiteral,
                            namedType,
                            literalCalculation));
        }
        this.typeSignature = typeSignature;
        this.longLiteral = longLiteral;
        this.namedType = namedType;
        this.literalCalculation = literalCalculation;
    }

    public static TypeParameter of(Type type)
    {
        return new TypeParameter(Optional.of(type), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static TypeParameter of(long longLiteral)
    {
        return new TypeParameter(Optional.empty(), Optional.of(longLiteral), Optional.empty(), Optional.empty());
    }

    public static TypeParameter of(NamedType namedType)
    {
        return new TypeParameter(Optional.empty(), Optional.empty(), Optional.of(namedType), Optional.empty());
    }

    public static TypeParameter of(TypeLiteralCalculation literalCalculation)
    {
        return new TypeParameter(Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(literalCalculation));
    }

    public static TypeParameter of(TypeSignatureParameter parameter, TypeManager typeManager)
    {
        if (parameter.getTypeSignature().isPresent()) {
            Type type = typeManager.getType(parameter.getTypeSignature().get());
            if (type == null) {
                return null;
            }
            return of(type);
        }
        else if (parameter.getLongLiteral().isPresent()) {
            return of(parameter.getLongLiteral().get());
        }
        else if (parameter.getNamedTypeSignature().isPresent()) {
            Type type = typeManager.getType(parameter.getNamedTypeSignature().get().getTypeSignature());
            if (type == null) {
                return null;
            }
            return of(new NamedType(
                    parameter.getNamedTypeSignature().get().getName(),
                    type));
        }
        else if (parameter.getLiteralCalculation().isPresent()) {
            return of(parameter.getLiteralCalculation().get());
        }
        else {
            throw new UnsupportedOperationException(format("Unsupported parameter [%s]", parameter));
        }
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
        else if (namedType.isPresent()) {
            return namedType.get().toString();
        }
        else {
            throw new UnsupportedOperationException("Unkown state of type parameter");
        }
    }

    public Optional<Type> getType()
    {
        return typeSignature;
    }

    public Optional<Long> getLongLiteral()
    {
        return longLiteral;
    }

    public Optional<NamedType> getNamedType()
    {
        return namedType;
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

        return Objects.equals(this.typeSignature, other.typeSignature) &&
                Objects.equals(this.longLiteral, other.longLiteral) &&
                Objects.equals(this.namedType, other.namedType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(typeSignature, longLiteral);
    }
}
