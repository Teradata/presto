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

import com.facebook.presto.type.SqlType;
import com.google.common.collect.ImmutableList;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class ScalarHeaderAndMethods
{
    private final ScalarHeader header;
    private final List<Method> methods;

    public ScalarHeaderAndMethods(ScalarHeader header, List<Method> methods) {
        this.header = header;
        this.methods = methods;
    }

    public static List<ScalarHeaderAndMethods> fromFunctionDefinitionClassAnnotations(Class annotated)
    {
        ImmutableList.Builder<ScalarHeaderAndMethods> builder = ImmutableList.builder();
        List<ScalarHeader> classHeaders = ScalarHeader.fromAnnotatedElement(annotated);
        checkArgument(!classHeaders.isEmpty(), "Class that defines function must be annotated with @ScalarFunction or @ScalarOperator.");

        for (ScalarHeader header : classHeaders) {
            builder.add(new ScalarHeaderAndMethods(header, findPublicMethodsWithAnnotation(annotated, SqlType.class)));
        }

        return builder.build();
    }

    public static List<ScalarHeaderAndMethods> fromFunctionSetClassAnnotations(Class annotated)
    {
        ImmutableList.Builder<ScalarHeaderAndMethods> builder = ImmutableList.builder();
        for (Method method : findPublicMethodsWithAnnotation(annotated, SqlType.class)) {
            for (ScalarHeader header : ScalarHeader.fromAnnotatedElement(method)) {
                builder.add(new ScalarHeaderAndMethods(header, ImmutableList.of(method)));
            }
        }
        return builder.build();
    }

    public ScalarHeader getHeader()
    {
        return header;
    }

    public List<Method> getMethods()
    {
        return methods;
    }

    private static List<Method> findPublicMethodsWithAnnotation(Class<?> clazz, Class<?> annotationClass)
    {
        ImmutableList.Builder<Method> methods = ImmutableList.builder();
        for (Method method : clazz.getMethods()) {
            for (Annotation annotation : method.getAnnotations()) {
                if (annotationClass.isInstance(annotation)) {
                    checkArgument(Modifier.isPublic(method.getModifiers()), "%s annotated with %s must be public", method.getName(), annotationClass.getSimpleName());
                    methods.add(method);
                }
            }
        }
        return methods.build();
    }
}