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
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.AnnotationBasedOperators.BlockInputAggregationFunction;
import com.facebook.presto.operator.AnnotationBasedOperators.CustomStateSerializerAggregationFunction;
import com.facebook.presto.operator.AnnotationBasedOperators.GenericAggregationFunction;
import com.facebook.presto.operator.AnnotationBasedOperators.HiddenScalarFunction;
import com.facebook.presto.operator.AnnotationBasedOperators.ImplicitSpecializedAggregationFunction;
import com.facebook.presto.operator.AnnotationBasedOperators.InjectLiteralAggregateFunction;
import com.facebook.presto.operator.AnnotationBasedOperators.InjectOperatorAggregateFunction;
import com.facebook.presto.operator.AnnotationBasedOperators.InjectTypeAggregateFunction;
import com.facebook.presto.operator.AnnotationBasedOperators.MultiOutputAggregationFunction;
import com.facebook.presto.operator.AnnotationBasedOperators.NonDeterministicScalarFunction;
import com.facebook.presto.operator.AnnotationBasedOperators.NotAnnotatedAggregateStateAggregationFunction;
import com.facebook.presto.operator.AnnotationBasedOperators.NotDecomposableAggregationFunction;
import com.facebook.presto.operator.AnnotationBasedOperators.SingleImplementationScalarFunction;
import com.facebook.presto.operator.AnnotationBasedOperators.StateOnDifferentThanFirstPositionAggregationFunction;
import com.facebook.presto.operator.aggregation.AggregationImplementation;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.aggregation.LazyAccumulatorFactoryBinder;
import com.facebook.presto.operator.aggregation.ParametricAggregation;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.annotations.ImplementationDependency;
import com.facebook.presto.operator.annotations.LiteralImplementationDependency;
import com.facebook.presto.operator.annotations.OperatorImplementationDependency;
import com.facebook.presto.operator.annotations.TypeImplementationDependency;
import com.facebook.presto.operator.scalar.ParametricScalar;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.annotations.ScalarFromAnnotationsParser;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.AnnotationBasedOperators.ComplexParametricScalarFunction;
import static com.facebook.presto.operator.AnnotationBasedOperators.ConstructorInjectionScalarFunction;
import static com.facebook.presto.operator.AnnotationBasedOperators.ExactAggregationFunction;
import static com.facebook.presto.operator.AnnotationBasedOperators.ExplicitSpecializedAggregationFunction;
import static com.facebook.presto.operator.AnnotationBasedOperators.MultiScalarFunction;
import static com.facebook.presto.operator.AnnotationBasedOperators.ParametricScalarFunction;
import static com.facebook.presto.operator.AnnotationBasedOperators.SimpleInjectionScalarFunction;
import static com.facebook.presto.operator.AnnotationBasedOperators.StaticMethodScalarFunction;
import static com.facebook.presto.operator.AnnotationBasedOperators.WithNullableComplexArgScalarFunction;
import static com.facebook.presto.operator.AnnotationBasedOperators.WithNullablePrimitiveArgScalarFunction;
import static com.facebook.presto.operator.aggregation.AggregationFromAnnotationsParser.parseFunctionDefinition;
import static com.facebook.presto.operator.aggregation.AggregationFromAnnotationsParser.parseFunctionDefinitions;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAnnotationEngine
{
    @Test
    public void testSimpleExactAggregationParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "simple_exact_aggregate",
                FunctionKind.AGGREGATE,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));

        ParametricAggregation aggregation = parseFunctionDefinition(ExactAggregationFunction.class);
        assertEquals(aggregation.getDescription(), "Simple exact aggregate description");
        assertTrue(aggregation.isDeterministic());
        assertEquals(aggregation.getSignature(), expectedSignature);
        ParametricImplementationsGroup<AggregationImplementation> implementations = aggregation.getImplementations();
        assertEquals(implementations.getExactImplementations().size(), 1);
        assertEquals(implementations.getGenericImplementations().size(), 0);
        assertEquals(implementations.getSpecializedImplementations().size(), 0);
        AggregationImplementation implementation = (AggregationImplementation) implementations.getExactImplementations().values().toArray()[0];
        assertFalse(implementation.getStateSerializerFactory().isPresent());
        assertEquals(implementation.getDefinitionClass(), ExactAggregationFunction.class);
        assertEquals(implementation.getInputDependencies().size(), 0);
        assertEquals(implementation.getCombineDependencies().size(), 0);
        assertEquals(implementation.getOutputDependencies().size(), 0);
        assertFalse(implementation.hasSpecializedTypeParameters());
        List<ParameterType> expectedMetadataTypes = ImmutableList.of(ParameterType.STATE, ParameterType.INPUT_CHANNEL);
        assertTrue(implementation.getParameterMetadataTypes().equals(expectedMetadataTypes));

        InternalAggregationFunction specialized = aggregation.specialize(BoundVariables.builder().build(), 1, new TypeRegistry(), null);
        assertEquals(specialized.getFinalType(), DoubleType.DOUBLE);
        assertTrue(specialized.isDecomposable());
        assertEquals(specialized.name(), "simple_exact_aggregate");
    }

    @Test
    public void testStateOnDifferentThanFirstPositionAggregationParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "simple_exact_aggregate_aggregation_state_moved",
                FunctionKind.AGGREGATE,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));

        ParametricAggregation aggregation = parseFunctionDefinition(StateOnDifferentThanFirstPositionAggregationFunction.class);
        assertEquals(aggregation.getSignature(), expectedSignature);

        ParametricImplementationsGroup<AggregationImplementation> implementations = aggregation.getImplementations();
        assertEquals(implementations.getExactImplementations().size(), 1);
        AggregationImplementation implementation = (AggregationImplementation) implementations.getExactImplementations().values().toArray()[0];

        assertEquals(implementation.getDefinitionClass(), StateOnDifferentThanFirstPositionAggregationFunction.class);
        List<ParameterType> expectedMetadataTypes = ImmutableList.of(ParameterType.INPUT_CHANNEL, ParameterType.STATE);
        assertTrue(implementation.getParameterMetadataTypes().equals(expectedMetadataTypes));
    }

    @Test
    public void testNotAnnotatedAggregateStateAggregationParse()
            throws Exception
    {
        ParametricAggregation aggregation = parseFunctionDefinition(NotAnnotatedAggregateStateAggregationFunction.class);

        ParametricImplementationsGroup<AggregationImplementation> implementations = aggregation.getImplementations();
        assertEquals(implementations.getExactImplementations().size(), 1);
        AggregationImplementation implementation = (AggregationImplementation) implementations.getExactImplementations().values().toArray()[0];

        List<ParameterType> expectedMetadataTypes = ImmutableList.of(ParameterType.STATE, ParameterType.INPUT_CHANNEL);
        assertTrue(implementation.getParameterMetadataTypes().equals(expectedMetadataTypes));

        InternalAggregationFunction specialized = aggregation.specialize(BoundVariables.builder().build(), 1, new TypeRegistry(), null);
        assertEquals(specialized.getFinalType(), DoubleType.DOUBLE);
        assertTrue(specialized.isDecomposable());
        assertEquals(specialized.name(), "no_aggregation_state_aggregate");
    }

    @Test
    public void testCustomStateSerializerAggregationParse()
            throws Exception
    {
        ParametricAggregation aggregation = parseFunctionDefinition(CustomStateSerializerAggregationFunction.class);
        ParametricImplementationsGroup<AggregationImplementation> implementations = aggregation.getImplementations();
        AggregationImplementation implementation = (AggregationImplementation) implementations.getExactImplementations().values().toArray()[0];
        assertTrue(implementation.getStateSerializerFactory().isPresent());

        InternalAggregationFunction specialized = aggregation.specialize(BoundVariables.builder().build(), 1, new TypeRegistry(), null);
        Object createdSerializer = ((LazyAccumulatorFactoryBinder) specialized.getAccumulatorFactoryBinder())
                .getGenericAccumulatorFactoryBinder().getStateSerializer();
        Class<?> serializerFactory = implementation.getStateSerializerFactory().get().type().returnType();
        assertTrue(serializerFactory.isInstance(createdSerializer));
    }

    @Test
    public void testNotDecomposableAggregationParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "custom_decomposable_aggregate",
                FunctionKind.AGGREGATE,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));

        ParametricAggregation aggregation = parseFunctionDefinition(NotDecomposableAggregationFunction.class);
        assertEquals(aggregation.getDescription(), "Aggregate with Decomposable=true");
        assertTrue(aggregation.isDeterministic());
        assertEquals(aggregation.getSignature(), expectedSignature);

        InternalAggregationFunction specialized = aggregation.specialize(BoundVariables.builder().build(), 1, new TypeRegistry(), null);
        assertEquals(specialized.getFinalType(), DoubleType.DOUBLE);
        assertFalse(specialized.isDecomposable());
        assertEquals(specialized.name(), "custom_decomposable_aggregate");
    }

    @Test
    public void testSimpleGenericAggregationFunctionParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "simple_generic_implementations",
                FunctionKind.AGGREGATE,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("T"),
                ImmutableList.of(parseTypeSignature("T")),
                false);

        ParametricAggregation aggregation = parseFunctionDefinition(GenericAggregationFunction.class);
        assertEquals(aggregation.getDescription(), "Simple aggregate with two generic implementations");
        assertTrue(aggregation.isDeterministic());
        assertEquals(aggregation.getSignature(), expectedSignature);
        ParametricImplementationsGroup<AggregationImplementation> implementations = aggregation.getImplementations();
        assertEquals(implementations.getExactImplementations().size(), 0);
        assertEquals(implementations.getGenericImplementations().size(), 2);
        assertEquals(implementations.getSpecializedImplementations().size(), 0);

        AggregationImplementation implementationDouble = implementations.getGenericImplementations().stream().filter(impl -> impl.getStateClass() == NullableDoubleState.class).collect(toImmutableList()).get(0);
        assertFalse(implementationDouble.getStateSerializerFactory().isPresent());
        assertEquals(implementationDouble.getDefinitionClass(), GenericAggregationFunction.class);
        assertEquals(implementationDouble.getInputDependencies().size(), 0);
        assertEquals(implementationDouble.getCombineDependencies().size(), 0);
        assertEquals(implementationDouble.getOutputDependencies().size(), 0);
        assertFalse(implementationDouble.hasSpecializedTypeParameters());
        List<ParameterType> expectedMetadataTypes = ImmutableList.of(ParameterType.STATE, ParameterType.INPUT_CHANNEL);
        assertTrue(implementationDouble.getParameterMetadataTypes().equals(expectedMetadataTypes));
        assertEquals(implementationDouble.getStateClass(), NullableDoubleState.class);

        AggregationImplementation implementationLong = implementations.getGenericImplementations().stream().filter(impl -> impl.getStateClass() == NullableLongState.class).collect(toImmutableList()).get(0);
        assertFalse(implementationLong.getStateSerializerFactory().isPresent());
        assertEquals(implementationLong.getDefinitionClass(), GenericAggregationFunction.class);
        assertEquals(implementationLong.getInputDependencies().size(), 0);
        assertEquals(implementationLong.getCombineDependencies().size(), 0);
        assertEquals(implementationLong.getOutputDependencies().size(), 0);
        assertFalse(implementationLong.hasSpecializedTypeParameters());
        assertTrue(implementationLong.getParameterMetadataTypes().equals(expectedMetadataTypes));
        assertEquals(implementationLong.getStateClass(), NullableLongState.class);

        InternalAggregationFunction specialized = aggregation.specialize(
                BoundVariables.builder().setTypeVariable("T", DoubleType.DOUBLE).build(),
                1,
                new TypeRegistry(),
                null);
        assertEquals(specialized.getFinalType(), DoubleType.DOUBLE);
        assertTrue(specialized.getParameterTypes().equals(ImmutableList.of(DoubleType.DOUBLE)));
        assertTrue(specialized.isDecomposable());
        assertEquals(specialized.name(), "simple_generic_implementations");
    }

    @Test
    public void testSimpleBlockInputAggregationParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "block_input_aggregate",
                FunctionKind.AGGREGATE,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));

        ParametricAggregation aggregation = parseFunctionDefinition(BlockInputAggregationFunction.class);
        assertEquals(aggregation.getDescription(), "Simple aggregate with @BlockPosition usage");
        assertTrue(aggregation.isDeterministic());
        assertEquals(aggregation.getSignature(), expectedSignature);
        ParametricImplementationsGroup<AggregationImplementation> implementations = aggregation.getImplementations();
        assertEquals(implementations.getExactImplementations().size(), 1);
        assertEquals(implementations.getGenericImplementations().size(), 0);
        assertEquals(implementations.getSpecializedImplementations().size(), 0);
        AggregationImplementation implementation = (AggregationImplementation) implementations.getExactImplementations().values().toArray()[0];
        assertFalse(implementation.getStateSerializerFactory().isPresent());
        assertEquals(implementation.getDefinitionClass(), BlockInputAggregationFunction.class);
        assertEquals(implementation.getInputDependencies().size(), 0);
        assertEquals(implementation.getCombineDependencies().size(), 0);
        assertEquals(implementation.getOutputDependencies().size(), 0);
        assertFalse(implementation.hasSpecializedTypeParameters());
        List<ParameterType> expectedMetadataTypes = ImmutableList.of(ParameterType.STATE, ParameterType.BLOCK_INPUT_CHANNEL, ParameterType.BLOCK_INDEX);
        assertEquals(implementation.getParameterMetadataTypes(), expectedMetadataTypes);

        InternalAggregationFunction specialized = aggregation.specialize(BoundVariables.builder().build(), 1, new TypeRegistry(), null);
        assertEquals(specialized.getFinalType(), DoubleType.DOUBLE);
        assertTrue(specialized.isDecomposable());
        assertEquals(specialized.name(), "block_input_aggregate");
    }

    //@Test - this is not yet supported
    public void testSimpleImplicitSpecializedAggregationParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "implicit_specialized_aggregate",
                FunctionKind.AGGREGATE,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("T"),
                ImmutableList.of(new TypeSignature(ARRAY, TypeSignatureParameter.of(parseTypeSignature("T"))), parseTypeSignature("T")),
                false);

        ParametricAggregation aggregation = parseFunctionDefinition(ImplicitSpecializedAggregationFunction.class);
        assertEquals(aggregation.getDescription(), "Simple implicit specialized aggregate");
        assertTrue(aggregation.isDeterministic());
        assertEquals(aggregation.getSignature(), expectedSignature);
        ParametricImplementationsGroup<AggregationImplementation> implementations = aggregation.getImplementations();
        assertEquals(implementations.getExactImplementations().size(), 0);
        assertEquals(implementations.getGenericImplementations().size(), 0);
        assertEquals(implementations.getSpecializedImplementations().size(), 2);
        AggregationImplementation implementation1 = implementations.getSpecializedImplementations().get(0);
        assertTrue(implementation1.hasSpecializedTypeParameters());
        assertFalse(implementation1.hasSpecializedTypeParameters());
        List<ParameterType> expectedMetadataTypes = ImmutableList.of(ParameterType.STATE, ParameterType.INPUT_CHANNEL, ParameterType.INPUT_CHANNEL);
        assertTrue(implementation1.getParameterMetadataTypes().equals(expectedMetadataTypes));

        AggregationImplementation implementation2 = implementations.getSpecializedImplementations().get(1);
        assertTrue(implementation2.hasSpecializedTypeParameters());
        assertFalse(implementation2.hasSpecializedTypeParameters());
        assertTrue(implementation2.getParameterMetadataTypes().equals(expectedMetadataTypes));

        InternalAggregationFunction specialized = aggregation.specialize(BoundVariables.builder().setTypeVariable("T", DoubleType.DOUBLE).build(), 1, new TypeRegistry(), null);
        assertEquals(specialized.getFinalType(), DoubleType.DOUBLE);
        assertTrue(specialized.isDecomposable());
        assertEquals(specialized.name(), "implicit_specialized_aggregate");
    }

    // @Test - this is not yet supported
    public void testSimpleExplicitSpecializedAggregationParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "explicit_specialized_aggregate",
                FunctionKind.AGGREGATE,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("T"),
                ImmutableList.of(new TypeSignature(ARRAY, TypeSignatureParameter.of(parseTypeSignature("T")))),
                false);

        ParametricAggregation aggregation = parseFunctionDefinition(ExplicitSpecializedAggregationFunction.class);
        assertEquals(aggregation.getDescription(), "Simple explicit specialized aggregate");
        assertTrue(aggregation.isDeterministic());
        assertEquals(aggregation.getSignature(), expectedSignature);
        ParametricImplementationsGroup<AggregationImplementation> implementations = aggregation.getImplementations();
        assertEquals(implementations.getExactImplementations().size(), 0);
        assertEquals(implementations.getGenericImplementations().size(), 1);
        assertEquals(implementations.getSpecializedImplementations().size(), 1);
        AggregationImplementation implementation1 = implementations.getSpecializedImplementations().get(0);
        assertTrue(implementation1.hasSpecializedTypeParameters());
        assertFalse(implementation1.hasSpecializedTypeParameters());
        List<ParameterType> expectedMetadataTypes = ImmutableList.of(ParameterType.STATE, ParameterType.INPUT_CHANNEL);
        assertTrue(implementation1.getParameterMetadataTypes().equals(expectedMetadataTypes));

        AggregationImplementation implementation2 = implementations.getSpecializedImplementations().get(1);
        assertTrue(implementation2.hasSpecializedTypeParameters());
        assertFalse(implementation2.hasSpecializedTypeParameters());
        assertTrue(implementation2.getParameterMetadataTypes().equals(expectedMetadataTypes));

        InternalAggregationFunction specialized = aggregation.specialize(BoundVariables.builder().setTypeVariable("T", DoubleType.DOUBLE).build(), 1, new TypeRegistry(), null);
        assertEquals(specialized.getFinalType(), DoubleType.DOUBLE);
        assertTrue(specialized.isDecomposable());
        assertEquals(specialized.name(), "implicit_specialized_aggregate");
    }

    @Test
    public void testMultiOutputAggregationParse()
    {
        Signature expectedSignature1 = new Signature(
                "multi_output_aggregate_1",
                FunctionKind.AGGREGATE,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));

        Signature expectedSignature2 = new Signature(
                "multi_output_aggregate_2",
                FunctionKind.AGGREGATE,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));

        List<ParametricAggregation> aggregations = parseFunctionDefinitions(MultiOutputAggregationFunction.class);
        assertEquals(aggregations.size(), 2);

        ParametricAggregation aggregation1 = aggregations.stream().filter(aggregate -> aggregate.getSignature().getName().equals("multi_output_aggregate_1")).collect(toImmutableList()).get(0);
        assertEquals(aggregation1.getSignature(), expectedSignature1);
        assertEquals(aggregation1.getDescription(), "Simple multi output function aggregate specialized description");

        ParametricAggregation aggregation2 = aggregations.stream().filter(aggregate -> aggregate.getSignature().getName().equals("multi_output_aggregate_2")).collect(toImmutableList()).get(0);
        assertEquals(aggregation2.getSignature(), expectedSignature2);
        assertEquals(aggregation2.getDescription(), "Simple multi output function aggregate generic description");

        List<ParameterType> expectedMetadataTypes = ImmutableList.of(ParameterType.STATE, ParameterType.INPUT_CHANNEL);

        ParametricImplementationsGroup<AggregationImplementation> implementations1 = aggregation1.getImplementations();
        assertEquals(implementations1.getExactImplementations().size(), 1);
        assertEquals(implementations1.getGenericImplementations().size(), 0);
        assertEquals(implementations1.getSpecializedImplementations().size(), 0);

        ParametricImplementationsGroup<AggregationImplementation> implementations2 = aggregation2.getImplementations();
        assertEquals(implementations2.getExactImplementations().size(), 1);
        assertEquals(implementations2.getGenericImplementations().size(), 0);
        assertEquals(implementations2.getSpecializedImplementations().size(), 0);

        AggregationImplementation implementation = (AggregationImplementation) implementations1.getExactImplementations().values().toArray()[0];
        assertFalse(implementation.getStateSerializerFactory().isPresent());
        assertEquals(implementation.getDefinitionClass(), MultiOutputAggregationFunction.class);
        assertEquals(implementation.getInputDependencies().size(), 0);
        assertEquals(implementation.getCombineDependencies().size(), 0);
        assertEquals(implementation.getOutputDependencies().size(), 0);
        assertFalse(implementation.hasSpecializedTypeParameters());
        assertTrue(implementation.getParameterMetadataTypes().equals(expectedMetadataTypes));

        InternalAggregationFunction specialized = aggregation1.specialize(BoundVariables.builder().build(), 1, new TypeRegistry(), null);
        assertEquals(specialized.getFinalType(), DoubleType.DOUBLE);
        assertTrue(specialized.isDecomposable());
        assertEquals(specialized.name(), "multi_output_aggregate_1");
    }

    @Test
    public void testInjectOperatorAggregateParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "inject_operator_aggregate",
                FunctionKind.AGGREGATE,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));

        ParametricAggregation aggregation = parseFunctionDefinition(InjectOperatorAggregateFunction.class);
        assertEquals(aggregation.getDescription(), "Simple aggregate with operator injected");
        assertTrue(aggregation.isDeterministic());
        assertEquals(aggregation.getSignature(), expectedSignature);
        ParametricImplementationsGroup<AggregationImplementation> implementations = aggregation.getImplementations();

        assertEquals(implementations.getExactImplementations().size(), 1);
        AggregationImplementation implementation = (AggregationImplementation) implementations.getExactImplementations().values().toArray()[0];

        assertTrue(implementation.getStateSerializerFactory().isPresent());
        assertEquals(implementation.getDefinitionClass(), InjectOperatorAggregateFunction.class);

        assertEquals(implementation.getInputDependencies().size(), 1);
        assertEquals(implementation.getCombineDependencies().size(), 1);
        assertEquals(implementation.getOutputDependencies().size(), 1);
        assertEquals(implementation.getStateSerializerFactoryDependencies().size(), 1);

        assertTrue(implementation.getInputDependencies().get(0) instanceof OperatorImplementationDependency);
        assertTrue(implementation.getCombineDependencies().get(0) instanceof OperatorImplementationDependency);
        assertTrue(implementation.getOutputDependencies().get(0) instanceof OperatorImplementationDependency);
        assertTrue(implementation.getStateSerializerFactoryDependencies().get(0) instanceof OperatorImplementationDependency);

        assertFalse(implementation.hasSpecializedTypeParameters());
        List<ParameterType> expectedMetadataTypes = ImmutableList.of(ParameterType.STATE, ParameterType.INPUT_CHANNEL);
        assertTrue(implementation.getParameterMetadataTypes().equals(expectedMetadataTypes));

        TypeManager typeRegistry = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeRegistry, new BlockEncodingManager(typeRegistry), new FeaturesConfig());
        InternalAggregationFunction specialized = aggregation.specialize(BoundVariables.builder().build(), 1, typeRegistry, functionRegistry);
        assertEquals(specialized.getFinalType(), DoubleType.DOUBLE);
        assertTrue(specialized.isDecomposable());
        assertEquals(specialized.name(), "inject_operator_aggregate");
    }

    @Test
    public void testInjectTypeAggregateParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "inject_type_aggregate",
                FunctionKind.AGGREGATE,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("T"),
                ImmutableList.of(parseTypeSignature("T")),
                false);

        ParametricAggregation aggregation = parseFunctionDefinition(InjectTypeAggregateFunction.class);
        assertEquals(aggregation.getDescription(), "Simple aggregate with type injected");
        assertTrue(aggregation.isDeterministic());
        assertEquals(aggregation.getSignature(), expectedSignature);
        ParametricImplementationsGroup<AggregationImplementation> implementations = aggregation.getImplementations();

        assertEquals(implementations.getGenericImplementations().size(), 1);
        AggregationImplementation implementation = implementations.getGenericImplementations().get(0);

        assertTrue(implementation.getStateSerializerFactory().isPresent());
        assertEquals(implementation.getDefinitionClass(), InjectTypeAggregateFunction.class);

        assertEquals(implementation.getInputDependencies().size(), 1);
        assertEquals(implementation.getCombineDependencies().size(), 1);
        assertEquals(implementation.getOutputDependencies().size(), 1);
        assertEquals(implementation.getStateSerializerFactoryDependencies().size(), 1);

        assertTrue(implementation.getInputDependencies().get(0) instanceof TypeImplementationDependency);
        assertTrue(implementation.getCombineDependencies().get(0) instanceof TypeImplementationDependency);
        assertTrue(implementation.getOutputDependencies().get(0) instanceof TypeImplementationDependency);
        assertTrue(implementation.getStateSerializerFactoryDependencies().get(0) instanceof TypeImplementationDependency);

        assertFalse(implementation.hasSpecializedTypeParameters());
        List<ParameterType> expectedMetadataTypes = ImmutableList.of(ParameterType.STATE, ParameterType.INPUT_CHANNEL);
        assertTrue(implementation.getParameterMetadataTypes().equals(expectedMetadataTypes));

        TypeManager typeRegistry = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeRegistry, new BlockEncodingManager(typeRegistry), new FeaturesConfig());
        InternalAggregationFunction specialized = aggregation.specialize(BoundVariables.builder().setTypeVariable("T", DoubleType.DOUBLE).build(), 1, typeRegistry, functionRegistry);
        assertEquals(specialized.getFinalType(), DoubleType.DOUBLE);
        assertTrue(specialized.isDecomposable());
        assertEquals(specialized.name(), "inject_type_aggregate");
    }

    @Test
    public void testInjectLiteralAggregateParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "inject_literal_aggregate",
                FunctionKind.AGGREGATE,
                parseTypeSignature("varchar(x)", ImmutableSet.of("x")),
                ImmutableList.of(parseTypeSignature("varchar(x)", ImmutableSet.of("x"))));

        ParametricAggregation aggregation = parseFunctionDefinition(InjectLiteralAggregateFunction.class);
        assertEquals(aggregation.getDescription(), "Simple aggregate with type literal");
        assertTrue(aggregation.isDeterministic());
        assertEquals(aggregation.getSignature(), expectedSignature);
        ParametricImplementationsGroup<AggregationImplementation> implementations = aggregation.getImplementations();

        assertEquals(implementations.getGenericImplementations().size(), 1);
        AggregationImplementation implementation = implementations.getGenericImplementations().get(0);

        assertTrue(implementation.getStateSerializerFactory().isPresent());
        assertEquals(implementation.getDefinitionClass(), InjectLiteralAggregateFunction.class);

        assertEquals(implementation.getInputDependencies().size(), 1);
        assertEquals(implementation.getCombineDependencies().size(), 1);
        assertEquals(implementation.getOutputDependencies().size(), 1);
        assertEquals(implementation.getStateSerializerFactoryDependencies().size(), 1);

        assertTrue(implementation.getInputDependencies().get(0) instanceof LiteralImplementationDependency);
        assertTrue(implementation.getCombineDependencies().get(0) instanceof LiteralImplementationDependency);
        assertTrue(implementation.getOutputDependencies().get(0) instanceof LiteralImplementationDependency);
        assertTrue(implementation.getStateSerializerFactoryDependencies().get(0) instanceof LiteralImplementationDependency);

        assertFalse(implementation.hasSpecializedTypeParameters());
        List<ParameterType> expectedMetadataTypes = ImmutableList.of(ParameterType.STATE, ParameterType.INPUT_CHANNEL);
        assertTrue(implementation.getParameterMetadataTypes().equals(expectedMetadataTypes));

        TypeManager typeRegistry = new TypeRegistry();
        FunctionRegistry functionRegistry = new FunctionRegistry(typeRegistry, new BlockEncodingManager(typeRegistry), new FeaturesConfig());
        InternalAggregationFunction specialized = aggregation.specialize(BoundVariables.builder().setLongVariable("x", 17L).build(), 1, typeRegistry, functionRegistry);
        assertEquals(specialized.getFinalType(), VarcharType.createVarcharType(17));
        assertTrue(specialized.isDecomposable());
        assertEquals(specialized.name(), "inject_literal_aggregate");
    }

    @Test
    public void testSingleImplementationScalarParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "single_implementation_parametric_scalar",
                FunctionKind.SCALAR,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(SingleImplementationScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Simple scalar with single implemenatation based on class");

        assertEquals(scalar.getImplementations().getExactImplementations().size(), 1);
        assertEquals(scalar.getImplementations().getSpecializedImplementations().size(), 0);
        assertEquals(scalar.getImplementations().getGenericImplementations().size(), 0);

        ScalarFunctionImplementation specialized = scalar.specialize(BoundVariables.builder().build(), 1, new TypeRegistry(), null);
        assertFalse(specialized.getInstanceFactory().isPresent());
        assertTrue(specialized.getNullableArguments().stream().allMatch(v -> !v));
        assertTrue(specialized.getNullFlags().stream().allMatch(v -> !v));
    }

    @Test
    public void testHiddenScalarParse()
            throws Exception
    {
        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(HiddenScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        assertTrue(scalar.isDeterministic());
        assertTrue(scalar.isHidden());
    }

    @Test
    public void testNonDeterministicScalarParse()
            throws Exception
    {
        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(NonDeterministicScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        assertFalse(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
    }

    @Test
    public void testWithNullablePrimitiveArgScalarParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "scalar_with_nullable",
                FunctionKind.SCALAR,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature(), DoubleType.DOUBLE.getTypeSignature()));

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(WithNullablePrimitiveArgScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Simple scalar with nullable primitive");

        ScalarFunctionImplementation specialized = scalar.specialize(BoundVariables.builder().build(), 2, new TypeRegistry(), null);
        assertFalse(specialized.getInstanceFactory().isPresent());
        assertEquals(specialized.getNullableArguments(), ImmutableList.of(false, true));
        assertEquals(specialized.getNullFlags(), ImmutableList.of(false, true));
    }

    @Test
    public void testWithNullableComplexArgScalarParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "scalar_with_nullable_complex",
                FunctionKind.SCALAR,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature(), DoubleType.DOUBLE.getTypeSignature()));

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(WithNullableComplexArgScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Simple scalar with nullable complex type");

        ScalarFunctionImplementation specialized = scalar.specialize(BoundVariables.builder().build(), 2, new TypeRegistry(), null);
        assertFalse(specialized.getInstanceFactory().isPresent());
        assertEquals(specialized.getNullableArguments(), ImmutableList.of(false, true));
        assertEquals(specialized.getNullFlags(), ImmutableList.of(false, false));
    }

    @Test
    public void testStaticMethodScalarParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "static_method_scalar",
                FunctionKind.SCALAR,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinitions(StaticMethodScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Simple scalar with single implemenatation based on method");
    }

    @Test
    public void testMultiScalarParse()
            throws Exception
    {
        Signature expectedSignature1 = new Signature(
                "static_method_scalar_1",
                FunctionKind.SCALAR,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));

        Signature expectedSignature2 = new Signature(
                "static_method_scalar_2",
                FunctionKind.SCALAR,
                BigintType.BIGINT.getTypeSignature(),
                ImmutableList.of(BigintType.BIGINT.getTypeSignature()));

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinitions(MultiScalarFunction.class);
        assertEquals(functions.size(), 2);
        ParametricScalar scalar1 = (ParametricScalar) functions.stream().filter(signature -> signature.getSignature().equals(expectedSignature1)).collect(toImmutableList()).get(0);
        ParametricScalar scalar2 = (ParametricScalar) functions.stream().filter(signature -> signature.getSignature().equals(expectedSignature2)).collect(toImmutableList()).get(0);

        assertEquals(scalar1.getImplementations().getExactImplementations().size(), 1);
        assertEquals(scalar1.getImplementations().getSpecializedImplementations().size(), 0);
        assertEquals(scalar1.getImplementations().getGenericImplementations().size(), 0);

        assertEquals(scalar1.getImplementations().getExactImplementations().size(), 1);
        assertEquals(scalar1.getImplementations().getSpecializedImplementations().size(), 0);
        assertEquals(scalar1.getImplementations().getGenericImplementations().size(), 0);

        assertEquals(scalar1.getSignature(), expectedSignature1);
        assertTrue(scalar1.isDeterministic());
        assertFalse(scalar1.isHidden());
        assertEquals(scalar1.getDescription(), "Simple scalar with single implemenatation based on method 1");

        assertEquals(scalar2.getSignature(), expectedSignature2);
        assertFalse(scalar2.isDeterministic());
        assertTrue(scalar2.isHidden());
        assertEquals(scalar2.getDescription(), "Simple scalar with single implemenatation based on method 2");
    }

    @Test
    public void testParametricScalarParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "parametric_scalar",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("T"),
                ImmutableList.of(parseTypeSignature("T")),
                false);

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(ParametricScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertEquals(scalar.getImplementations().getExactImplementations().size(), 0);
        assertEquals(scalar.getImplementations().getSpecializedImplementations().size(), 2);
        assertEquals(scalar.getImplementations().getGenericImplementations().size(), 0);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Parametric scalar description");
    }

    @Test
    public void testComplexParametricScalarParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "with_exact_scalar",
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                BooleanType.BOOLEAN.getTypeSignature(),
                ImmutableList.of(parseTypeSignature("array(varchar(x))", ImmutableSet.of("x"))),
                false);

        Signature exactSignature = new Signature(
                "with_exact_scalar",
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                BooleanType.BOOLEAN.getTypeSignature(),
                ImmutableList.of(parseTypeSignature("array(varchar(17))")),
                false);

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(ComplexParametricScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertEquals(scalar.getImplementations().getExactImplementations().size(), 1);
        assertEquals(scalar.getImplementations().getSpecializedImplementations().size(), 0);
        assertEquals(scalar.getImplementations().getGenericImplementations().size(), 1);
        assertEquals(scalar.getImplementations().getExactImplementations().keySet().stream().findFirst().get(), exactSignature);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Parametric scalar with exact and generic implementations");
    }

    @Test
    public void testSimpleInjectionScalarParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "parametric_scalar_inject",
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                BigintType.BIGINT.getTypeSignature(),
                ImmutableList.of(parseTypeSignature("varchar(x)", ImmutableSet.of("x"))),
                false);

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(SimpleInjectionScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertEquals(scalar.getImplementations().getExactImplementations().size(), 0);
        assertEquals(scalar.getImplementations().getSpecializedImplementations().size(), 0);
        assertEquals(scalar.getImplementations().getGenericImplementations().size(), 1);
        List<ImplementationDependency> dependencies = scalar.getImplementations().getGenericImplementations().get(0).getDependencies();
        assertEquals(dependencies.size(), 1);
        assertTrue(dependencies.get(0) instanceof LiteralImplementationDependency);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Parametric scalar with literal injected");
    }

    @Test
    public void testConstructorInjectionScalarParse()
            throws Exception
    {
        Signature expectedSignature = new Signature(
                "parametric_scalar_inject_constructor",
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                BigintType.BIGINT.getTypeSignature(),
                ImmutableList.of(parseTypeSignature("array(T)")),
                false);

        List<SqlScalarFunction> functions = ScalarFromAnnotationsParser.parseFunctionDefinition(ConstructorInjectionScalarFunction.class);
        assertEquals(functions.size(), 1);
        ParametricScalar scalar = (ParametricScalar) functions.get(0);
        assertEquals(scalar.getImplementations().getExactImplementations().size(), 0);
        assertEquals(scalar.getImplementations().getSpecializedImplementations().size(), 0);
        assertEquals(scalar.getImplementations().getGenericImplementations().size(), 1);
        List<ImplementationDependency> dependencies = scalar.getImplementations().getGenericImplementations().get(0).getDependencies();
        assertEquals(dependencies.size(), 0);
        List<ImplementationDependency> constructorDependencies = scalar.getImplementations().getGenericImplementations().get(0).getConstructorDependencies();
        assertEquals(constructorDependencies.size(), 1);
        assertTrue(constructorDependencies.get(0) instanceof TypeImplementationDependency);

        assertEquals(scalar.getSignature(), expectedSignature);
        assertTrue(scalar.isDeterministic());
        assertFalse(scalar.isHidden());
        assertEquals(scalar.getDescription(), "Parametric scalar with type injected though constructor");
    }
}
