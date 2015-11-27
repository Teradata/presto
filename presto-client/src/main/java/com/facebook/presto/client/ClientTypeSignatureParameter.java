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
package com.facebook.presto.client;

import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;

@Immutable
public class ClientTypeSignatureParameter
{
    private final Optional<ClientTypeSignature> typeSignature;
    private final Optional<Long> longLiteral;
    // TODO: use NamedClientTypeSignature here?
    private final Optional<NamedTypeSignature> namedTypeSignature;

    public ClientTypeSignatureParameter(TypeSignatureParameter typeParameterSignature)
    {
        Optional<TypeSignature> typeSignature = typeParameterSignature.getTypeSignature();
        if (typeSignature.isPresent()) {
            this.typeSignature = Optional.of(new ClientTypeSignature(typeSignature.get()));
        }
        else {
            this.typeSignature = Optional.empty();
        }
        longLiteral = typeParameterSignature.getLongLiteral();
        namedTypeSignature = typeParameterSignature.getNamedTypeSignature();
    }

    @JsonCreator
    public ClientTypeSignatureParameter(
            @JsonProperty("typeSignature") Optional<ClientTypeSignature> typeSignature,
            @JsonProperty("longLiteral") Optional<Long> longLiteral,
            @JsonProperty("namedTypeSignature") Optional<NamedTypeSignature> namedTypeSignature)
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

    @JsonProperty
    public Optional<ClientTypeSignature> getTypeSignature()
    {
        return typeSignature;
    }

    @JsonProperty
    public Optional<Long> getLongLiteral()
    {
        return longLiteral;
    }

    @JsonProperty
    public Optional<NamedTypeSignature> getNamedTypeSignature()
    {
        return namedTypeSignature;
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
            throw new UnsupportedOperationException("ClientTypeSignatureParameter::toString has failed");
        }
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

        ClientTypeSignatureParameter other = (ClientTypeSignatureParameter) o;

        return Objects.equals(this.typeSignature, other.typeSignature) &&
                Objects.equals(this.longLiteral, other.longLiteral) &&
                Objects.equals(this.namedTypeSignature, other.namedTypeSignature);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(typeSignature, longLiteral, namedTypeSignature);
    }
}
