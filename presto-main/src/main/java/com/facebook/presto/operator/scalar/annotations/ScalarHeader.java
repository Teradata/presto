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
package com.facebook.presto.operator.scalar.annotations;

import com.facebook.presto.operator.Description;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ScalarHeader
{
    private final String name;
    private final Optional<String> description;
    private final boolean hidden;
    private final boolean deterministic;

    private ScalarHeader(String name, Optional<String> description, boolean hidden, boolean deterministic)
    {
        this.name = requireNonNull(name);
        this.description = requireNonNull(description);
        this.hidden = requireNonNull(hidden);
        this.deterministic = requireNonNull(deterministic);
    }

    private static String annotatedName(AnnotatedElement annotatedElement)
    {
        if (annotatedElement instanceof Class<?>) {
            return ((Class<?>) annotatedElement).getSimpleName();
        }
        else if (annotatedElement instanceof Method) {
            return ((Method) annotatedElement).getName();
        }

        checkArgument(false, "Only Classes and Methods are supported as annotated elements.");
        return null;
    }

    private static String camelToSnake(String name)
    {
        return LOWER_CAMEL.to(LOWER_UNDERSCORE, name);
    }

    public static List<ScalarHeader> fromAnnotatedElement(AnnotatedElement annotated)
    {
        ScalarFunction scalarFunction = annotated.getAnnotation(ScalarFunction.class);
        ScalarOperator scalarOperator = annotated.getAnnotation(ScalarOperator.class);
        Description descriptionAnnotation = annotated.getAnnotation(Description.class);

        ImmutableList.Builder<ScalarHeader> builder = ImmutableList.builder();

        Optional<String> description = Optional.empty();
        if (descriptionAnnotation != null) {
            description = Optional.of(descriptionAnnotation.value());
        }

        if (scalarFunction != null) {
            String baseName = scalarFunction.value().isEmpty() ? camelToSnake(annotatedName(annotated)) : scalarFunction.value();
            builder.add(new ScalarHeader(baseName, description, scalarFunction.hidden(), scalarFunction.deterministic()));

            for (String alias : scalarFunction.alias()) {
                builder.add(new ScalarHeader(alias, description, scalarFunction.hidden(), scalarFunction.deterministic()));
            }
        }

        if (scalarOperator != null) {
            builder.add(new ScalarHeader(mangleOperatorName(scalarOperator.value()), description, true, true));
        }

        List<ScalarHeader> result = builder.build();
        checkArgument(!result.isEmpty());
        return result;
    }

    public String getName()
    {
        return name;
    }

    public Optional<String> getDescription()
    {
        return description;
    }

    public boolean isHidden()
    {
        return hidden;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }
}
