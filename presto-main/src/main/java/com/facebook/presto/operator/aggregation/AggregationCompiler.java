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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SignatureBinder;
import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.aggregation.state.AccumulatorState;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateFactory;
import com.facebook.presto.operator.aggregation.state.AccumulatorStateSerializer;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.LiteralParameters;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.SAMPLE_WEIGHT;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class AggregationCompiler
{
    private AggregationCompiler()
    {
    }

    // This function should only be used for function matching for testing purposes.
    // General purpose function matching is done through FunctionRegistry.
    public static BindableAggregationFunction generateAggregationBindableFunction(Class<?> clazz)
    {
        List<BindableAggregationFunction> aggregations = generateBindableAggregationFunctions(clazz);
        checkArgument(aggregations.size() == 1, "More than one aggregation function found");
        return aggregations.get(0);
    }

    // This function should only be used for function matching for testing purposes.
    // General purpose function matching is done through FunctionRegistry.
    public static BindableAggregationFunction generateAggregationBindableFunction(Class<?> clazz, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(argumentTypes, "argumentTypes is null");
        for (BindableAggregationFunction aggregation : generateBindableAggregationFunctions(clazz)) {
            if (aggregation.getSignature().getReturnType().equals(returnType) &&
                    aggregation.getSignature().getArgumentTypes().equals(argumentTypes)) {
                return aggregation;
            }
        }
        throw new IllegalArgumentException(String.format("No method with return type %s and arguments %s", returnType, argumentTypes));
    }

    public static List<BindableAggregationFunction> generateBindableAggregationFunctions(Class<?> aggregationDefinition)
    {
        AggregationFunction aggregationAnnotation = aggregationDefinition.getAnnotation(AggregationFunction.class);
        requireNonNull(aggregationAnnotation, "aggregationAnnotation is null");

        DynamicClassLoader classLoader = new DynamicClassLoader(aggregationDefinition.getClassLoader());

        ImmutableList.Builder<BindableAggregationFunction> builder = ImmutableList.builder();

        for (Class<?> stateClass : getStateClasses(aggregationDefinition)) {
            AccumulatorStateSerializer<?> stateSerializer = new StateCompiler().generateStateSerializer(stateClass, classLoader);

            for (Method outputFunction : getOutputFunctions(aggregationDefinition, stateClass)) {
                for (Method inputFunction : getInputFunctions(aggregationDefinition, stateClass)) {
                    for (String name : getNames(outputFunction, aggregationAnnotation)) {
                        List<TypeSignature> inputTypes = getInputTypesSignatures(inputFunction);
                        TypeSignature outputType = AggregationUtils.getOutputTypeSignature(outputFunction);

                        builder.add(
                                new BindableAggregationFunction(
                                        new Signature(
                                                name,
                                                aggregationAnnotation.approximate() ? FunctionKind.APPROXIMATE_AGGREGATE : FunctionKind.AGGREGATE,
                                                ImmutableList.of(), // TODO parse constrains from annotations
                                                ImmutableList.of(), // TODO parse constrains from annotations
                                                outputType,
                                                inputTypes,
                                                false),
                                        getDescription(aggregationDefinition, outputFunction),
                                        aggregationAnnotation.approximate(),
                                        aggregationAnnotation.decomposable(),
                                        aggregationDefinition,
                                        stateClass,
                                        inputFunction,
                                        outputFunction));
                    }
                }
            }
        }

        return builder.build();
    }

    private static boolean isParameterNullable(Annotation[] annotations)
    {
        return Arrays.asList(annotations).stream().anyMatch(annotation -> annotation instanceof NullablePosition);
    }

    private static boolean isParameterBlock(Annotation[] annotations)
    {
        return Arrays.asList(annotations).stream().anyMatch(annotation -> annotation instanceof BlockPosition);
    }

    private static List<String> getNames(@Nullable Method outputFunction, AggregationFunction aggregationAnnotation)
    {
        List<String> defaultNames = ImmutableList.<String>builder().add(aggregationAnnotation.value()).addAll(Arrays.asList(aggregationAnnotation.alias())).build();

        if (outputFunction == null) {
            return defaultNames;
        }

        AggregationFunction annotation = outputFunction.getAnnotation(AggregationFunction.class);
        if (annotation == null) {
            return defaultNames;
        }
        else {
            return ImmutableList.<String>builder().add(annotation.value()).addAll(Arrays.asList(annotation.alias())).build();
        }
    }

    private static Method getIntermediateInputFunction(Class<?> clazz, Class<?> stateClass)
    {
        for (Method method : findPublicStaticMethodsWithAnnotation(clazz, IntermediateInputFunction.class)) {
            if (method.getParameterTypes()[0] == stateClass) {
                return method;
            }
        }
        return null;
    }

    private static Method getCombineFunction(Class<?> clazz, Class<?> stateClass)
    {
        for (Method method : findPublicStaticMethodsWithAnnotation(clazz, CombineFunction.class)) {
            if (method.getParameterTypes()[0] == stateClass) {
                return method;
            }
        }
        return null;
    }

    private static List<Method> getOutputFunctions(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> methods = findPublicStaticMethodsWithAnnotation(clazz, OutputFunction.class).stream()
                .filter(method -> method.getParameterTypes()[0] == stateClass)
                .collect(toImmutableList());

        checkArgument(!methods.isEmpty(), "Aggregation has no output functions");
        return methods;
    }

    private static List<Method> getInputFunctions(Class<?> clazz, Class<?> stateClass)
    {
        // Only include methods that match this state class
        List<Method> inputFunctions = findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class).stream()
                .filter(method -> method.getParameterTypes()[0] == stateClass)
                .collect(toImmutableList());

        checkArgument(!inputFunctions.isEmpty(), "Aggregation has no input functions");
        return inputFunctions;
    }

    private static List<TypeSignature> getInputTypesSignatures(Method inputFunction)
    {
        // FIXME Literal parameters should be part of class annotations.
        ImmutableList.Builder<TypeSignature> builder = ImmutableList.builder();
        Set<String> literalParameters = getLiteralParameter(inputFunction);

        Annotation[][] parameterAnnotations = inputFunction.getParameterAnnotations();
        for (Annotation[] annotations : parameterAnnotations) {
            for (Annotation annotation : annotations) {
                if (annotation instanceof SqlType) {
                    String typeName = ((SqlType) annotation).value();
                    builder.add(parseTypeSignature(typeName, literalParameters));
                }
            }
        }

        return builder.build();
    }

    private static Set<Class<?>> getStateClasses(Class<?> clazz)
    {
        ImmutableSet.Builder<Class<?>> builder = ImmutableSet.builder();
        for (Method inputFunction : findPublicStaticMethodsWithAnnotation(clazz, InputFunction.class)) {
            checkArgument(inputFunction.getParameterTypes().length > 0, "Input function has no parameters");
            Class<?> stateClass = inputFunction.getParameterTypes()[0];
            checkArgument(AccumulatorState.class.isAssignableFrom(stateClass), "stateClass is not a subclass of AccumulatorState");
            builder.add(stateClass);
        }
        ImmutableSet<Class<?>> stateClasses = builder.build();
        checkArgument(!stateClasses.isEmpty(), "No input functions found");

        return stateClasses;
    }

    private static String getDescription(AnnotatedElement base, AnnotatedElement override)
    {
        Description description = override.getAnnotation(Description.class);
        if (description != null) {
            return description.value();
        }
        description = base.getAnnotation(Description.class);
        return (description == null) ? null : description.value();
    }

    private static Set<String> getLiteralParameter(Method inputFunction)
    {
        ImmutableSet.Builder<String> literalParametersBuilder = ImmutableSet.builder();

        Annotation[] literalParameters = inputFunction.getAnnotations();
        for (Annotation annotation : literalParameters) {
            if (annotation instanceof LiteralParameters) {
                for (String literal : ((LiteralParameters) annotation).value()) {
                   literalParametersBuilder.add(literal);
                }
            }
        }

        return literalParametersBuilder.build();
    }

    private static List<Method> findPublicStaticMethodsWithAnnotation(Class<?> clazz, Class<?> annotationClass)
    {
        ImmutableList.Builder<Method> methods = ImmutableList.builder();
        for (Method method : clazz.getMethods()) {
            for (Annotation annotation : method.getAnnotations()) {
                if (annotationClass.isInstance(annotation)) {
                    checkArgument(Modifier.isStatic(method.getModifiers()) && Modifier.isPublic(method.getModifiers()), "%s annotated with %s must be static and public", method.getName(), annotationClass.getSimpleName());
                    methods.add(method);
                }
            }
        }
        return methods.build();
    }

    public static class BindableAggregationFunction
    {
        private final Signature signature;
        private final String description;
        private final boolean approximate;
        private final boolean decomposable;

        private final Class<?> definitionClass;
        private final Class<?> stateClass;
        private final Method inputFunction;
        private final Method outputFunction;

        public BindableAggregationFunction(Signature signature,
                                           String description,
                                           boolean approximate,
                                           boolean decomposable,
                                           Class<?> definitionClass,
                                           Class<?> stateClass,
                                           Method inputFunction,
                                           Method outputFunction)
        {
            this.signature = signature;
            this.description = description;
            this.approximate = approximate;
            this.decomposable = decomposable;
            this.definitionClass = definitionClass;
            this.stateClass = stateClass;
            this.inputFunction = inputFunction;
            this.outputFunction = outputFunction;
        }

        public Signature getSignature()
        {
            return signature;
        }

        public String getDescription()
        {
            return description;
        }

        public InternalAggregationFunction specialize(BoundVariables variables,
                                                      TypeManager typeManager,
                                                      int arity)
        {
            // bind variables
            Signature boundSignature = SignatureBinder.bindVariables(signature, variables, arity);
            List<Type> inputTypes = boundSignature.getArgumentTypes().stream().map(x -> typeManager.getType(x)).collect(toImmutableList());
            Type outputType = typeManager.getType(boundSignature.getReturnType());

            AggregationFunction aggregationAnnotation = definitionClass.getAnnotation(AggregationFunction.class);
            requireNonNull(aggregationAnnotation, "aggregationAnnotation is null");

            DynamicClassLoader classLoader = new DynamicClassLoader(definitionClass.getClassLoader());

            AggregationMetadata metadata;
            AccumulatorStateSerializer<?> stateSerializer = new StateCompiler().generateStateSerializer(stateClass, classLoader);
            Type intermediateType = stateSerializer.getSerializedType();
            Method intermediateInputFunction = getIntermediateInputFunction(definitionClass, stateClass);
            Method combineFunction = getCombineFunction(definitionClass, stateClass);
            AccumulatorStateFactory<?> stateFactory = new StateCompiler().generateStateFactory(stateClass, classLoader);

            try {
                MethodHandle inputHandle = lookup().unreflect(inputFunction);
                MethodHandle intermediateInputHandle = intermediateInputFunction == null ? null : lookup().unreflect(intermediateInputFunction);
                MethodHandle combineHandle = combineFunction == null ? null : lookup().unreflect(combineFunction);
                MethodHandle outputHandle = outputFunction == null ? null : lookup().unreflect(outputFunction);
                metadata = new AggregationMetadata(
                        generateAggregationName(signature.getName(), outputType.getTypeSignature(), signaturesFromTypes(inputTypes)),
                        getParameterMetadata(inputFunction, inputTypes),
                        inputHandle,
                        getParameterMetadata(intermediateInputFunction, inputTypes),
                        intermediateInputHandle,
                        combineHandle,
                        outputHandle,
                        stateClass,
                        stateSerializer,
                        stateFactory,
                        outputType,
                        aggregationAnnotation.approximate());
            }
            catch (IllegalAccessException e) {
                throw Throwables.propagate(e);
            }

            AccumulatorFactoryBinder factory = new LazyAccumulatorFactoryBinder(metadata, classLoader);

            return new InternalAggregationFunction(signature.getName(),
                    inputTypes,
                    intermediateType,
                    outputType,
                    decomposable,
                    approximate,
                    factory);
        }

        public InternalAggregationFunction getOnlySpecialization()
        {
            return getOnlySpecialization(new TypeRegistry());
        }

        public InternalAggregationFunction getOnlySpecialization(TypeManager typeManager)
        {
            // TODO check if default specialization exists
            return specialize(BoundVariables.builder().build(), typeManager, 0);
        }

        private static List<TypeSignature> signaturesFromTypes(List<Type> types)
        {
            return types
                    .stream()
                    .map(x -> x.getTypeSignature())
                    .collect(toImmutableList());
        }

        private static List<ParameterMetadata> getParameterMetadata(@Nullable Method method, List<Type> inputTypes)
        {
            if (method == null) {
                return null;
            }

            ImmutableList.Builder<ParameterMetadata> builder = ImmutableList.builder();
            builder.add(new ParameterMetadata(STATE));

            Annotation[][] annotations = method.getParameterAnnotations();
            String methodName = method.getDeclaringClass() + "." + method.getName();

            // Start at 1 because 0 is the STATE
            for (int i = 1; i < annotations.length; i++) {
                Annotation baseTypeAnnotation = baseTypeAnnotation(annotations[i], methodName);
                if (baseTypeAnnotation instanceof SqlType) {
                    builder.add(ParameterMetadata.fromSqlType(inputTypes.get(i - 1), isParameterBlock(annotations[i]), isParameterNullable(annotations[i]), methodName));
                }
                else if (baseTypeAnnotation instanceof BlockIndex) {
                    builder.add(new ParameterMetadata(BLOCK_INDEX));
                }
                else if (baseTypeAnnotation instanceof SampleWeight) {
                    builder.add(new ParameterMetadata(SAMPLE_WEIGHT));
                }
                else {
                    throw new IllegalArgumentException("Unsupported annotation: " + annotations[i]);
                }
            }
            return builder.build();
        }

        private static Annotation baseTypeAnnotation(Annotation[] annotations, String methodName)
        {
            List<Annotation> baseTypes = Arrays.asList(annotations).stream()
                    .filter(annotation -> annotation instanceof SqlType || annotation instanceof BlockIndex || annotation instanceof SampleWeight)
                    .collect(toImmutableList());

            checkArgument(baseTypes.size() == 1, "Parameter of %s must have exactly one of @SqlType, @BlockIndex, and @SampleWeight", methodName);

            boolean nullable = isParameterNullable(annotations);
            boolean isBlock = isParameterBlock(annotations);

            Annotation annotation = baseTypes.get(0);
            checkArgument((!isBlock && !nullable) || (annotation instanceof SqlType),
                    "%s contains a parameter with @BlockPosition and/or @NullablePosition that is not @SqlType", methodName);

            return annotation;
        }
    }
}
