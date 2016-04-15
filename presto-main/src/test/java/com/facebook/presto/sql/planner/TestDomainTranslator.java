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
package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DomainTranslator.ExtractionResult;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.FunctionRegistry.getMagicLiteralFunctionSignature;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.FloatType.FLOAT;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.IS_DISTINCT_FROM;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.NOT_EQUAL;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.facebook.presto.type.ColorType.COLOR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDomainTranslator
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();

    private static final Symbol C_BIGINT = new Symbol("c_bigint");
    private static final Symbol C_DOUBLE = new Symbol("c_double");
    private static final Symbol C_VARCHAR = new Symbol("c_varchar");
    private static final Symbol C_BOOLEAN = new Symbol("c_boolean");
    private static final Symbol C_BIGINT_1 = new Symbol("c_bigint_1");
    private static final Symbol C_DOUBLE_1 = new Symbol("c_double_1");
    private static final Symbol C_VARCHAR_1 = new Symbol("c_varchar_1");
    private static final Symbol C_TIMESTAMP = new Symbol("c_timestamp");
    private static final Symbol C_DATE = new Symbol("c_date");
    private static final Symbol C_COLOR = new Symbol("c_color");
    private static final Symbol C_HYPER_LOG_LOG = new Symbol("c_hyper_log_log");
    private static final Symbol C_VARBINARY = new Symbol("c_varbinary");
    private static final Symbol C_DECIMAL_26_5 = new Symbol("c_decimal_26_5");
    private static final Symbol C_DECIMAL_23_4 = new Symbol("c_decimal_23_4");
    private static final Symbol C_INTEGER = new Symbol("c_integer");
    private static final Symbol C_CHAR = new Symbol("c_char");
    private static final Symbol C_DECIMAL_21_3 = new Symbol("c_decimal_21_3");
    private static final Symbol C_DECIMAL_12_2 = new Symbol("c_decimal_12_2");
    private static final Symbol C_DECIMAL_6_1 = new Symbol("c_decimal_6_1");
    private static final Symbol C_DECIMAL_3_0 = new Symbol("c_decimal_3_0");
    private static final Symbol C_DECIMAL_2_0 = new Symbol("c_decimal_2_0");
    private static final Symbol C_SMALLINT = new Symbol("c_smallint");
    private static final Symbol C_TINYINT = new Symbol("c_tinyint");

    private static final Map<Symbol, Type> TYPES = ImmutableMap.<Symbol, Type>builder()
            .put(C_BIGINT, BIGINT)
            .put(C_DOUBLE, DOUBLE)
            .put(C_VARCHAR, VARCHAR)
            .put(C_BOOLEAN, BOOLEAN)
            .put(C_BIGINT_1, BIGINT)
            .put(C_DOUBLE_1, DOUBLE)
            .put(C_VARCHAR_1, VARCHAR)
            .put(C_TIMESTAMP, TIMESTAMP)
            .put(C_DATE, DATE)
            .put(C_COLOR, COLOR) // Equatable, but not orderable
            .put(C_HYPER_LOG_LOG, HYPER_LOG_LOG) // Not Equatable or orderable
            .put(C_VARBINARY, VARBINARY)
            .put(C_DECIMAL_26_5, createDecimalType(26, 5))
            .put(C_DECIMAL_23_4, createDecimalType(23, 4))
            .put(C_INTEGER, INTEGER)
            .put(C_CHAR, createCharType(10))
            .put(C_DECIMAL_21_3, createDecimalType(21, 3))
            .put(C_DECIMAL_12_2, createDecimalType(12, 2))
            .put(C_DECIMAL_6_1, createDecimalType(6, 1))
            .put(C_DECIMAL_3_0, createDecimalType(3, 0))
            .put(C_DECIMAL_2_0, createDecimalType(2, 0))
            .put(C_SMALLINT, SMALLINT)
            .put(C_TINYINT, TINYINT)
            .build();

    private static final long TIMESTAMP_VALUE = new DateTime(2013, 3, 30, 1, 5, 0, 0, DateTimeZone.UTC).getMillis();
    private static final long DATE_VALUE = new DateTime(2001, 1, 22, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
    private static final long COLOR_VALUE_1 = 1;
    private static final long COLOR_VALUE_2 = 2;

    @Test
    public void testNoneRoundTrip()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = TupleDomain.none();
        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), tupleDomain);
    }

    @Test
    public void testAllRoundTrip()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = TupleDomain.all();
        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), tupleDomain);
    }

    @Test
    public void testRoundTrip()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder()
                .put(C_BIGINT, Domain.singleValue(BIGINT, 1L))
                .put(C_DOUBLE, Domain.onlyNull(DOUBLE))
                .put(C_VARCHAR, Domain.notNull(VARCHAR))
                .put(C_BOOLEAN, Domain.singleValue(BOOLEAN, true))
                .put(C_BIGINT_1, Domain.singleValue(BIGINT, 2L))
                .put(C_DOUBLE_1, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(DOUBLE, 1.1), Range.equal(DOUBLE, 2.0), Range.range(DOUBLE, 3.0, false, 3.5, true)), true))
                .put(C_VARCHAR_1, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARCHAR, utf8Slice("2013-01-01")), Range.greaterThan(VARCHAR, utf8Slice("2013-10-01"))), false))
                .put(C_TIMESTAMP, Domain.singleValue(TIMESTAMP, TIMESTAMP_VALUE))
                .put(C_DATE, Domain.singleValue(DATE, DATE_VALUE))
                .put(C_COLOR, Domain.singleValue(COLOR, COLOR_VALUE_1))
                .put(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG))
                .build());

        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), tupleDomain);
    }

    @Test
    public void testInOptimization()
            throws Exception
    {
        Domain testDomain = Domain.create(
                ValueSet.all(BIGINT)
                        .subtract(ValueSet.ofRanges(
                                Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))), false);

        TupleDomain<Symbol> tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(C_BIGINT, testDomain).build());
        assertEquals(toPredicate(tupleDomain), not(in(C_BIGINT, ImmutableList.of(1L, 2L, 3L))));

        testDomain = Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(BIGINT, 4L)).intersect(
                        ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L)))), false);

        tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(C_BIGINT, testDomain).build());
        assertEquals(toPredicate(tupleDomain), and(lessThan(C_BIGINT, bigintLiteral(4L)), not(in(C_BIGINT, ImmutableList.of(1L, 2L, 3L)))));

        testDomain = Domain.create(ValueSet.ofRanges(
                Range.range(BIGINT, 1L, true, 3L, true),
                Range.range(BIGINT, 5L, true, 7L, true),
                Range.range(BIGINT, 9L, true, 11L, true)),
                false);

        tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(C_BIGINT, testDomain).build());
        assertEquals(toPredicate(tupleDomain),
                or(between(C_BIGINT, bigintLiteral(1L), bigintLiteral(3L)), (between(C_BIGINT, bigintLiteral(5L), bigintLiteral(7L))), (between(C_BIGINT, bigintLiteral(9L), bigintLiteral(11L)))));

        testDomain = Domain.create(
                ValueSet.ofRanges(
                        Range.lessThan(BIGINT, 4L))
                        .intersect(ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))))
                        .union(ValueSet.ofRanges(Range.range(BIGINT, 7L, true, 9L, true))), false);

        tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(C_BIGINT, testDomain).build());
        assertEquals(toPredicate(tupleDomain), or(and(lessThan(C_BIGINT, bigintLiteral(4L)), not(in(C_BIGINT, ImmutableList.of(1L, 2L, 3L)))), between(C_BIGINT, bigintLiteral(7L), bigintLiteral(9L))));

        testDomain = Domain.create(
                ValueSet.ofRanges(Range.lessThan(BIGINT, 4L))
                        .intersect(ValueSet.all(BIGINT)
                                .subtract(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L), Range.equal(BIGINT, 3L))))
                        .union(ValueSet.ofRanges(Range.range(BIGINT, 7L, false, 9L, false), Range.range(BIGINT, 11L, false, 13L, false))), false);

        tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder().put(C_BIGINT, testDomain).build());
        assertEquals(toPredicate(tupleDomain), or(
                and(lessThan(C_BIGINT, bigintLiteral(4L)), not(in(C_BIGINT, ImmutableList.of(1L, 2L, 3L)))),
                and(greaterThan(C_BIGINT, bigintLiteral(7L)), lessThan(C_BIGINT, bigintLiteral(9L))),
                and(greaterThan(C_BIGINT, bigintLiteral(11L)), lessThan(C_BIGINT, bigintLiteral(13L)))));
    }

    @Test
    public void testToPredicateNone()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder()
                .put(C_BIGINT, Domain.singleValue(BIGINT, 1L))
                .put(C_DOUBLE, Domain.onlyNull(DOUBLE))
                .put(C_VARCHAR, Domain.notNull(VARCHAR))
                .put(C_BOOLEAN, Domain.none(BOOLEAN))
                .build());

        assertEquals(toPredicate(tupleDomain), FALSE_LITERAL);
    }

    @Test
    public void testToPredicateAllIgnored()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain = withColumnDomains(ImmutableMap.<Symbol, Domain>builder()
                .put(C_BIGINT, Domain.singleValue(BIGINT, 1L))
                .put(C_DOUBLE, Domain.onlyNull(DOUBLE))
                .put(C_VARCHAR, Domain.notNull(VARCHAR))
                .put(C_BOOLEAN, Domain.all(BOOLEAN))
                .build());

        ExtractionResult result = fromPredicate(toPredicate(tupleDomain));
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.<Symbol, Domain>builder()
                .put(C_BIGINT, Domain.singleValue(BIGINT, 1L))
                .put(C_DOUBLE, Domain.onlyNull(DOUBLE))
                .put(C_VARCHAR, Domain.notNull(VARCHAR))
                .build()));
    }

    @Test
    public void testToPredicate()
            throws Exception
    {
        TupleDomain<Symbol> tupleDomain;

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT)));
        assertEquals(toPredicate(tupleDomain), isNotNull(C_BIGINT));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.onlyNull(BIGINT)));
        assertEquals(toPredicate(tupleDomain), isNull(C_BIGINT));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.none(BIGINT)));
        assertEquals(toPredicate(tupleDomain), FALSE_LITERAL);

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.all(BIGINT)));
        assertEquals(toPredicate(tupleDomain), TRUE_LITERAL);

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), greaterThan(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), greaterThanOrEqual(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), lessThan(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, false, 1L, true)), false)));
        assertEquals(toPredicate(tupleDomain), and(greaterThan(C_BIGINT, bigintLiteral(0L)), lessThanOrEqual(C_BIGINT, bigintLiteral(1L))));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 1L)), false)));
        assertEquals(toPredicate(tupleDomain), lessThanOrEqual(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.singleValue(BIGINT, 1L)));
        assertEquals(toPredicate(tupleDomain), equal(C_BIGINT, bigintLiteral(1L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false)));
        assertEquals(toPredicate(tupleDomain), in(C_BIGINT, ImmutableList.of(1L, 2L)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), true)));
        assertEquals(toPredicate(tupleDomain), or(lessThan(C_BIGINT, bigintLiteral(1L)), isNull(C_BIGINT)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), true)));
        assertEquals(toPredicate(tupleDomain), or(equal(C_COLOR, colorLiteral(COLOR_VALUE_1)), isNull(C_COLOR)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true)));
        assertEquals(toPredicate(tupleDomain), or(not(equal(C_COLOR, colorLiteral(COLOR_VALUE_1))), isNull(C_COLOR)));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.onlyNull(HYPER_LOG_LOG)));
        assertEquals(toPredicate(tupleDomain), isNull(C_HYPER_LOG_LOG));

        tupleDomain = withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG)));
        assertEquals(toPredicate(tupleDomain), isNotNull(C_HYPER_LOG_LOG));
    }

    @Test
    public void testFromUnknownPredicate()
            throws Exception
    {
        ExtractionResult result = fromPredicate(unprocessableExpression1(C_BIGINT));
        assertTrue(result.getTupleDomain().isAll());
        assertEquals(result.getRemainingExpression(), unprocessableExpression1(C_BIGINT));

        // Test the complement
        result = fromPredicate(not(unprocessableExpression1(C_BIGINT)));
        assertTrue(result.getTupleDomain().isAll());
        assertEquals(result.getRemainingExpression(), not(unprocessableExpression1(C_BIGINT)));
    }

    @Test
    public void testFromAndPredicate()
            throws Exception
    {
        Expression originalPredicate = and(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)));
        ExtractionResult result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), and(unprocessableExpression1(C_BIGINT), unprocessableExpression2(C_BIGINT)));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, false, 5L, false)), false))));

        // Test complements
        originalPredicate = not(and(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(and(
                not(and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT))),
                not(and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));
    }

    @Test
    public void testFromOrPredicate()
            throws Exception
    {
        Expression originalPredicate = or(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)));
        ExtractionResult result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));

        originalPredicate = or(
                and(equal(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(equal(C_BIGINT, bigintLiteral(2L)), unprocessableExpression2(C_BIGINT)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        // Same unprocessableExpression means that we can do more extraction
        // If both sides are operating on the same single symbol
        originalPredicate = or(
                and(equal(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(equal(C_BIGINT, bigintLiteral(2L)), unprocessableExpression1(C_BIGINT)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), unprocessableExpression1(C_BIGINT));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        // And not if they have different symbols
        originalPredicate = or(
                and(equal(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(equal(C_DOUBLE, doubleLiteral(2.0)), unprocessableExpression1(C_BIGINT)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertTrue(result.getTupleDomain().isAll());

        // We can make another optimization if one side is the super set of the other side
        originalPredicate = or(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), greaterThan(C_DOUBLE, doubleLiteral(1.0)), unprocessableExpression1(C_BIGINT)),
                and(greaterThan(C_BIGINT, bigintLiteral(2L)), greaterThan(C_DOUBLE, doubleLiteral(2.0)), unprocessableExpression1(C_BIGINT)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), unprocessableExpression1(C_BIGINT));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(
                C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 1L)), false),
                C_DOUBLE, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 1.0)), false))));

        // We can't make those inferences if the unprocessableExpressions are non-deterministic
        originalPredicate = or(
                and(equal(C_BIGINT, bigintLiteral(1L)), randPredicate(C_BIGINT)),
                and(equal(C_BIGINT, bigintLiteral(2L)), randPredicate(C_BIGINT)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        // Test complements
        originalPredicate = not(or(
                and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)),
                and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), and(
                not(and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT))),
                not(and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)))));
        assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(or(
                not(and(greaterThan(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT))),
                not(and(lessThan(C_BIGINT, bigintLiteral(5L)), unprocessableExpression2(C_BIGINT)))));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), and(unprocessableExpression1(C_BIGINT), unprocessableExpression2(C_BIGINT)));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, false, 5L, false)), false))));
    }

    @Test
    public void testFromNotPredicate()
            throws Exception
    {
        Expression originalPredicate = not(and(equal(C_BIGINT, bigintLiteral(1L)), unprocessableExpression1(C_BIGINT)));
        ExtractionResult result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(unprocessableExpression1(C_BIGINT));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), originalPredicate);
        assertTrue(result.getTupleDomain().isAll());

        originalPredicate = not(TRUE_LITERAL);
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalPredicate = not(equal(C_BIGINT, bigintLiteral(1L)));
        result = fromPredicate(originalPredicate);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 1L)), false))));
    }

    @Test
    public void testFromUnprocessableComparison()
            throws Exception
    {
        // If it is not a simple comparison, we should not try to process it
        Expression predicate = comparison(GREATER_THAN, unprocessableExpression1(C_BIGINT), unprocessableExpression2(C_BIGINT));
        ExtractionResult result = fromPredicate(predicate);
        assertEquals(result.getRemainingExpression(), predicate);
        assertTrue(result.getTupleDomain().isAll());

        // Complement
        predicate = not(comparison(GREATER_THAN, unprocessableExpression1(C_BIGINT), unprocessableExpression2(C_BIGINT)));
        result = fromPredicate(predicate);
        assertEquals(result.getRemainingExpression(), predicate);
        assertTrue(result.getTupleDomain().isAll());
    }

    @Test
    public void testFromBasicComparisons()
            throws Exception
    {
        // Test out the extraction of all basic comparisons
        Expression originalExpression = greaterThan(C_BIGINT, bigintLiteral(2L));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = greaterThanOrEqual(C_BIGINT, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = lessThan(C_BIGINT, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = lessThanOrEqual(C_BIGINT, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = equal(C_BIGINT, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = notEqual(C_BIGINT, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = isDistinctFrom(C_BIGINT, bigintLiteral(2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true))));

        originalExpression = equal(C_COLOR, colorLiteral(COLOR_VALUE_1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false))));

        originalExpression = in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2), false))));

        originalExpression = isDistinctFrom(C_COLOR, colorLiteral(COLOR_VALUE_1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true))));

        // Test complement
        originalExpression = not(greaterThan(C_BIGINT, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(greaterThanOrEqual(C_BIGINT, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = not(lessThan(C_BIGINT, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(lessThanOrEqual(C_BIGINT, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(equal(C_BIGINT, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(notEqual(C_BIGINT, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = not(isDistinctFrom(C_BIGINT, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = not(equal(C_COLOR, colorLiteral(COLOR_VALUE_1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), false))));

        originalExpression = not(in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2).complement(), false))));

        originalExpression = not(isDistinctFrom(C_COLOR, colorLiteral(COLOR_VALUE_1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false))));
    }

    @Test
    public void testFromFlippedBasicComparisons()
            throws Exception
    {
        // Test out the extraction of all basic comparisons where the reference literal ordering is flipped
        ComparisonExpression originalExpression = comparison(GREATER_THAN, bigintLiteral(2L), reference(C_BIGINT));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = comparison(GREATER_THAN_OR_EQUAL, bigintLiteral(2L), reference(C_BIGINT));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = comparison(LESS_THAN, bigintLiteral(2L), reference(C_BIGINT));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = comparison(LESS_THAN_OR_EQUAL, bigintLiteral(2L), reference(C_BIGINT));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = comparison(EQUAL, bigintLiteral(2L), reference(C_BIGINT));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = comparison(EQUAL, colorLiteral(COLOR_VALUE_1), reference(C_COLOR));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1), false))));

        originalExpression = comparison(NOT_EQUAL, bigintLiteral(2L), reference(C_BIGINT));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = comparison(NOT_EQUAL, colorLiteral(COLOR_VALUE_1), reference(C_COLOR));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), false))));

        originalExpression = comparison(IS_DISTINCT_FROM, bigintLiteral(2L), reference(C_BIGINT));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true))));

        originalExpression = comparison(IS_DISTINCT_FROM, colorLiteral(COLOR_VALUE_1), reference(C_COLOR));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1).complement(), true))));

        originalExpression = comparison(IS_DISTINCT_FROM, nullLiteral(), reference(C_BIGINT));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));
    }

    @Test
    public void testFromBasicComparisonsWithNulls()
            throws Exception
    {
        // Test out the extraction of all basic comparisons with null literals
        Expression originalExpression = greaterThan(C_BIGINT, nullLiteral());
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = greaterThan(C_VARCHAR, new Cast(nullLiteral(), StandardTypes.VARCHAR));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_VARCHAR, Domain.create(ValueSet.none(VARCHAR), false))));

        originalExpression = greaterThanOrEqual(C_BIGINT, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = lessThan(C_BIGINT, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = lessThanOrEqual(C_BIGINT, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = equal(C_BIGINT, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = equal(C_COLOR, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = notEqual(C_BIGINT, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = notEqual(C_COLOR, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = isDistinctFrom(C_BIGINT, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));

        originalExpression = isDistinctFrom(C_COLOR, nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.notNull(COLOR))));

        // Test complements
        originalExpression = not(greaterThan(C_BIGINT, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(greaterThanOrEqual(C_BIGINT, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(lessThan(C_BIGINT, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(lessThanOrEqual(C_BIGINT, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(equal(C_BIGINT, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(equal(C_COLOR, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(notEqual(C_BIGINT, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(notEqual(C_COLOR, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(isDistinctFrom(C_BIGINT, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.onlyNull(BIGINT))));

        originalExpression = not(isDistinctFrom(C_COLOR, nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.onlyNull(COLOR))));
    }

    @Test
    public void testFromComparisonsWithImplictCoercions()
            throws Exception
    {
        // B is a double column. Check that it can be compared against longs
        Expression originalExpression = greaterThan(C_DOUBLE, bigintLiteral(2L));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_DOUBLE, Domain.create(ValueSet.ofRanges(Range.greaterThan(DOUBLE, 2.0)), false))));

        // C is a string column. Check that it can be compared.
        originalExpression = greaterThan(C_VARCHAR, stringLiteral("test"));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_VARCHAR, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("test"))), false))));

        // A is a long column. Check that it can be compared against doubles
        originalExpression = greaterThan(C_BIGINT, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = greaterThan(C_BIGINT, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = greaterThanOrEqual(C_BIGINT, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = greaterThanOrEqual(C_BIGINT, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = lessThan(C_BIGINT, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = lessThan(C_BIGINT, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = lessThanOrEqual(C_BIGINT, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = lessThanOrEqual(C_BIGINT, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = equal(C_BIGINT, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = equal(C_BIGINT, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.none(BIGINT))));

        originalExpression = notEqual(C_BIGINT, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = notEqual(C_BIGINT, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));

        originalExpression = isDistinctFrom(C_BIGINT, doubleLiteral(2.0));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), true))));

        originalExpression = isDistinctFrom(C_BIGINT, doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isAll());

        // Test complements

        // B is a double column. Check that it can be compared against longs
        originalExpression = not(greaterThan(C_DOUBLE, bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_DOUBLE, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(DOUBLE, 2.0)), false))));

        // C is a string column. Check that it can be compared.
        originalExpression = not(greaterThan(C_VARCHAR, stringLiteral("test")));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_VARCHAR, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARCHAR, utf8Slice("test"))), false))));

        // A is a long column. Check that it can be compared against doubles
        originalExpression = not(greaterThan(C_BIGINT, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(greaterThan(C_BIGINT, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(greaterThanOrEqual(C_BIGINT, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L)), false))));

        originalExpression = not(greaterThanOrEqual(C_BIGINT, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(lessThan(C_BIGINT, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 2L)), false))));

        originalExpression = not(lessThan(C_BIGINT, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(lessThanOrEqual(C_BIGINT, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(lessThanOrEqual(C_BIGINT, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(equal(C_BIGINT, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 2L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(equal(C_BIGINT, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));

        originalExpression = not(notEqual(C_BIGINT, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = not(notEqual(C_BIGINT, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.none(BIGINT))));

        originalExpression = not(isDistinctFrom(C_BIGINT, doubleLiteral(2.0)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 2L)), false))));

        originalExpression = not(isDistinctFrom(C_BIGINT, doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());
    }

    @Test
    public void testFromUnprocessableInPredicate()
            throws Exception
    {
        Expression originalExpression = new InPredicate(unprocessableExpression1(C_BIGINT), new InListExpression(ImmutableList.<Expression>of(TRUE_LITERAL)));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), originalExpression);
        assertTrue(result.getTupleDomain().isAll());

        originalExpression = new InPredicate(reference(C_BOOLEAN), new InListExpression(ImmutableList.of(unprocessableExpression1(C_BOOLEAN))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), equal(C_BOOLEAN, unprocessableExpression1(C_BOOLEAN)));
        assertTrue(result.getTupleDomain().isAll());

        originalExpression = new InPredicate(reference(C_BOOLEAN), new InListExpression(ImmutableList.of(TRUE_LITERAL, unprocessableExpression1(C_BOOLEAN))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), or(equal(C_BOOLEAN, TRUE_LITERAL), equal(C_BOOLEAN, unprocessableExpression1(C_BOOLEAN))));
        assertTrue(result.getTupleDomain().isAll());

        // Test complement
        originalExpression = not(new InPredicate(reference(C_BOOLEAN), new InListExpression(ImmutableList.of(unprocessableExpression1(C_BOOLEAN)))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), not(equal(C_BOOLEAN, unprocessableExpression1(C_BOOLEAN))));
        assertTrue(result.getTupleDomain().isAll());
    }

    @Test
    public void testFromInPredicate()
            throws Exception
    {
        Expression originalExpression = in(C_BIGINT, ImmutableList.of(1L));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.singleValue(BIGINT, 1L))));

        originalExpression = in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.singleValue(COLOR, COLOR_VALUE_1))));

        originalExpression = in(C_BIGINT, ImmutableList.of(1L, 2L));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));

        originalExpression = in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2), false))));

        originalExpression = not(in(C_BIGINT, ImmutableList.of(1L, 2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.range(BIGINT, 1L, false, 2L, false), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(in(C_COLOR, ImmutableList.of(colorLiteral(COLOR_VALUE_1), colorLiteral(COLOR_VALUE_2))));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_COLOR, Domain.create(ValueSet.of(COLOR, COLOR_VALUE_1, COLOR_VALUE_2).complement(), false))));

        // TODO update domain translator to properly handle cast
//        originalExpression = in(A, Arrays.asList(1L, 2L, (Expression) null));
//        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
//        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
//        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(ACH, Domain.create(ValueSet.ofRanges(Range.equal(BIGINT, 1L), Range.equal(BIGINT, 2L)), false))));
//
//        originalExpression = not(in(A, Arrays.asList(1L, 2L, (Expression) null)));
//        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
//        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
//        assertTrue(result.getTupleDomain().isNone());
//
//        originalExpression = in(A, Arrays.asList((Expression) null));
//        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
//        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
//        assertTrue(result.getTupleDomain().isNone());
//
//        originalExpression = not(in(A, Arrays.asList((Expression) null)));
//        result = fromPredicate(originalExpression, TYPES, COLUMN_HANDLES);
//        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
//        assertTrue(result.getTupleDomain().isNone());
    }

    @Test
    public void testFromBetweenPredicate()
            throws Exception
    {
        Expression originalExpression = between(C_BIGINT, bigintLiteral(1L), bigintLiteral(2L));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 2L, true)), false))));

        originalExpression = between(C_BIGINT, bigintLiteral(1L), doubleLiteral(2.1));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1L, true, 2L, true)), false))));

        originalExpression = between(C_BIGINT, bigintLiteral(1L), nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        // Test complements
        originalExpression = not(between(C_BIGINT, bigintLiteral(1L), bigintLiteral(2L)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(between(C_BIGINT, bigintLiteral(1L), doubleLiteral(2.1)));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L), Range.greaterThan(BIGINT, 2L)), false))));

        originalExpression = not(between(C_BIGINT, bigintLiteral(1L), nullLiteral()));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1L)), false))));
    }

    @Test
    public void testFromIsNullPredicate()
            throws Exception
    {
        Expression originalExpression = isNull(C_BIGINT);
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.onlyNull(BIGINT))));

        originalExpression = isNull(C_HYPER_LOG_LOG);
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.onlyNull(HYPER_LOG_LOG))));

        originalExpression = not(isNull(C_BIGINT));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));

        originalExpression = not(isNull(C_HYPER_LOG_LOG));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG))));
    }

    @Test
    public void testFromIsNotNullPredicate()
            throws Exception
    {
        Expression originalExpression = isNotNull(C_BIGINT);
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.notNull(BIGINT))));

        originalExpression = isNotNull(C_HYPER_LOG_LOG);
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.notNull(HYPER_LOG_LOG))));

        originalExpression = not(isNotNull(C_BIGINT));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_BIGINT, Domain.onlyNull(BIGINT))));

        originalExpression = not(isNotNull(C_HYPER_LOG_LOG));
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_HYPER_LOG_LOG, Domain.onlyNull(HYPER_LOG_LOG))));
    }

    @Test
    public void testFromBooleanLiteralPredicate()
            throws Exception
    {
        Expression originalExpression = TRUE_LITERAL;
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isAll());

        originalExpression = not(TRUE_LITERAL);
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = FALSE_LITERAL;
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(FALSE_LITERAL);
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isAll());
    }

    @Test
    public void testFromNullLiteralPredicate()
            throws Exception
    {
        Expression originalExpression = nullLiteral();
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());

        originalExpression = not(nullLiteral());
        result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        assertTrue(result.getTupleDomain().isNone());
    }

    @Test
    public void testExpressionConstantFolding()
            throws Exception
    {
        Expression originalExpression = comparison(GREATER_THAN, reference(C_VARBINARY), function("from_hex", stringLiteral("123456")));
        ExtractionResult result = fromPredicate(originalExpression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        Slice value = Slices.wrappedBuffer(BaseEncoding.base16().decode("123456"));
        assertEquals(result.getTupleDomain(), withColumnDomains(ImmutableMap.of(C_VARBINARY, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARBINARY, value)), false))));

        Expression expression = toPredicate(result.getTupleDomain());
        assertEquals(expression, comparison(GREATER_THAN, reference(C_VARBINARY), varbinaryLiteral(value)));
    }

    @Test
    public void testNumericTypeTranslation()
            throws Exception
    {
        List<NumericValues> translationChain = ImmutableList.of(
                new NumericValues(C_DOUBLE, (value) -> doubleLiteral((double) value), -1.0 * Double.MAX_VALUE, -22.0, -44.555678, 23.0, 44.555678, Double.MAX_VALUE),
                new NumericValues(C_DECIMAL_26_5, (value) -> decimalLiteral(Decimals.toString((Slice) value, 5)), longDecimal("-999999999999999999999.99999"), longDecimal("-22.00000"), longDecimal("-44.55568"), longDecimal("23.00000"), longDecimal("44.55567"), longDecimal("999999999999999999999.99999")),
                new NumericValues(C_DECIMAL_23_4, (value) -> decimalLiteral(Decimals.toString((Slice) value, 4)), longDecimal("-9999999999999999999.9999"), longDecimal("-22.0000"), longDecimal("-44.5557"), longDecimal("23.0000"), longDecimal("44.5556"), longDecimal("9999999999999999999.9999")),
                new NumericValues(C_BIGINT, (value) -> bigintLiteral((long) value), Long.MIN_VALUE, -22L, -45L, 23L, 44L, Long.MAX_VALUE),
                new NumericValues(C_DECIMAL_21_3, (value) -> decimalLiteral(Decimals.toString((Slice) value, 3)),
                        longDecimal("-999999999999999999.999"), longDecimal("-22.000"), longDecimal("-44.556"), longDecimal("23.000"), longDecimal("44.555"), longDecimal("999999999999999999.999")),
                new NumericValues(C_DECIMAL_12_2, (value) -> decimalLiteral(Decimals.toString((Long) value, 2)),
                        shortDecimal("-9999999999.99"), shortDecimal("-22.00"), shortDecimal("-44.56"), shortDecimal("23.00"), shortDecimal("44.55"), shortDecimal("9999999999.99")),
                new NumericValues(C_INTEGER, (value) -> integerLiteral((long) value), (long) Integer.MIN_VALUE, -22L, -45L, 23L, 44L, (long) Integer.MAX_VALUE),
                new NumericValues(C_DECIMAL_6_1, (value) -> decimalLiteral(Decimals.toString((Long) value, 1)),
                        shortDecimal("-99999.9"), shortDecimal("-22.0"), shortDecimal("-44.6"), shortDecimal("23.0"), shortDecimal("44.5"), shortDecimal("99999.9")),
                new NumericValues(C_SMALLINT, (value) -> smallintLiteral((long) value), (long) Short.MIN_VALUE, -22L, -45L, 23L, 44L, (long) Short.MAX_VALUE),
                new NumericValues(C_DECIMAL_3_0, (value) -> decimalLiteral(Decimals.toString((Long) value, 0)),
                        shortDecimal("-999"), shortDecimal("-22"), shortDecimal("-45"), shortDecimal("23"), shortDecimal("44"), shortDecimal("999")),
                new NumericValues(C_TINYINT, (value) -> tinyintLiteral((long) value), (long) Byte.MIN_VALUE, -22L, -45L, 23L, 44L, (long) Byte.MAX_VALUE),
                new NumericValues(C_DECIMAL_2_0, (value) -> decimalLiteral(Decimals.toString((Long) value, 0)),
                        shortDecimal("-99"), shortDecimal("-22"), shortDecimal("-45"), shortDecimal("23"), shortDecimal("44"), shortDecimal("99"))
        );

        for (int literalIndex = 0; literalIndex < translationChain.size(); literalIndex++) {
            for (int columnIndex = literalIndex + 1; columnIndex < translationChain.size(); columnIndex++) {
                NumericValues literal = translationChain.get(literalIndex);
                NumericValues column = translationChain.get(columnIndex);
                testNumericTypeTranslation(column, literal);
            }
        }
    }

    private void testNumericTypeTranslation(NumericValues columnValues, NumericValues literalValues)
    {
        Symbol columnSymbol = columnValues.getColumn();
        Type columnType = columnValues.getType();
        Function<Object, Expression> literalConstructor = literalValues.getLiteralConstructor();

        // greater than or equal
        testSimpleComparison(greaterThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getPositive())), columnSymbol, Range.greaterThanOrEqual(columnType, columnValues.getPositive()));
        testSimpleComparison(greaterThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getNegative())), columnSymbol, Range.greaterThanOrEqual(columnType, columnValues.getNegative()));
        testSimpleComparison(greaterThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getMax())), columnSymbol, Range.greaterThan(columnType, columnValues.getMax()));
        testSimpleComparison(greaterThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getMin())), columnSymbol, Range.greaterThanOrEqual(columnType, columnValues.getMin()));
        if (literalValues.isFractional()) {
            testSimpleComparison(greaterThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getFractionalPositive())), columnSymbol, Range.greaterThan(columnType, columnValues.getFractionalPositive()));
            testSimpleComparison(greaterThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getFractionalNegative())), columnSymbol, Range.greaterThan(columnType, columnValues.getFractionalNegative()));
        }

        // greater than
        testSimpleComparison(greaterThan(columnSymbol, literalConstructor.apply(literalValues.getPositive())), columnSymbol, Range.greaterThan(columnType, columnValues.getPositive()));
        testSimpleComparison(greaterThan(columnSymbol, literalConstructor.apply(literalValues.getNegative())), columnSymbol, Range.greaterThan(columnType, columnValues.getNegative()));
        testSimpleComparison(greaterThan(columnSymbol, literalConstructor.apply(literalValues.getMax())), columnSymbol, Range.greaterThan(columnType, columnValues.getMax()));
        testSimpleComparison(greaterThan(columnSymbol, literalConstructor.apply(literalValues.getMin())), columnSymbol, Range.greaterThanOrEqual(columnType, columnValues.getMin()));
        if (literalValues.isFractional()) {
            testSimpleComparison(greaterThan(columnSymbol, literalConstructor.apply(literalValues.getFractionalPositive())), columnSymbol, Range.greaterThan(columnType, columnValues.getFractionalPositive()));
            testSimpleComparison(greaterThan(columnSymbol, literalConstructor.apply(literalValues.getFractionalNegative())), columnSymbol, Range.greaterThan(columnType, columnValues.getFractionalNegative()));
        }

        // less than or equal
        testSimpleComparison(lessThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getPositive())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getPositive()));
        testSimpleComparison(lessThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getNegative())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getNegative()));
        testSimpleComparison(lessThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getMax())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getMax()));
        testSimpleComparison(lessThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getMin())), columnSymbol, Range.lessThan(columnType, columnValues.getMin()));
        if (literalValues.isFractional()) {
            testSimpleComparison(lessThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getFractionalPositive())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getFractionalPositive()));
            testSimpleComparison(lessThanOrEqual(columnSymbol, literalConstructor.apply(literalValues.getFractionalNegative())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getFractionalNegative()));
        }

        // less than
        testSimpleComparison(lessThan(columnSymbol, literalConstructor.apply(literalValues.getPositive())), columnSymbol, Range.lessThan(columnType, columnValues.getPositive()));
        testSimpleComparison(lessThan(columnSymbol, literalConstructor.apply(literalValues.getNegative())), columnSymbol, Range.lessThan(columnType, columnValues.getNegative()));
        testSimpleComparison(lessThan(columnSymbol, literalConstructor.apply(literalValues.getMax())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getMax()));
        testSimpleComparison(lessThan(columnSymbol, literalConstructor.apply(literalValues.getMin())), columnSymbol, Range.lessThan(columnType, columnValues.getMin()));
        if (literalValues.isFractional()) {
            testSimpleComparison(lessThan(columnSymbol, literalConstructor.apply(literalValues.getFractionalPositive())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getFractionalPositive()));
            testSimpleComparison(lessThan(columnSymbol, literalConstructor.apply(literalValues.getFractionalNegative())), columnSymbol, Range.lessThanOrEqual(columnType, columnValues.getFractionalNegative()));
        }

        // equal
        testSimpleComparison(equal(columnSymbol, literalConstructor.apply(literalValues.getPositive())), columnSymbol, Range.equal(columnType, columnValues.getPositive()));
        testSimpleComparison(equal(columnSymbol, literalConstructor.apply(literalValues.getNegative())), columnSymbol, Range.equal(columnType, columnValues.getNegative()));
        testSimpleComparison(equal(columnSymbol, literalConstructor.apply(literalValues.getMax())), columnSymbol, Domain.none(columnType));
        testSimpleComparison(equal(columnSymbol, literalConstructor.apply(literalValues.getMin())), columnSymbol, Domain.none(columnType));
        if (literalValues.isFractional()) {
            testSimpleComparison(equal(columnSymbol, literalConstructor.apply(literalValues.getFractionalPositive())), columnSymbol, Domain.none(columnType));
            testSimpleComparison(equal(columnSymbol, literalConstructor.apply(literalValues.getFractionalNegative())), columnSymbol, Domain.none(columnType));
        }

        // not equal
        testSimpleComparison(notEqual(columnSymbol, literalConstructor.apply(literalValues.getPositive())), columnSymbol, Domain.create(ValueSet.ofRanges(Range.lessThan(columnType, columnValues.getPositive()), Range.greaterThan(columnType, columnValues.getPositive())), false));
        testSimpleComparison(notEqual(columnSymbol, literalConstructor.apply(literalValues.getNegative())), columnSymbol, Domain.create(ValueSet.ofRanges(Range.lessThan(columnType, columnValues.getNegative()), Range.greaterThan(columnType, columnValues.getNegative())), false));
        testSimpleComparison(notEqual(columnSymbol, literalConstructor.apply(literalValues.getMax())), columnSymbol, Domain.notNull(columnType));
        testSimpleComparison(notEqual(columnSymbol, literalConstructor.apply(literalValues.getMin())), columnSymbol, Domain.notNull(columnType));
        if (literalValues.isFractional()) {
            testSimpleComparison(notEqual(columnSymbol, literalConstructor.apply(literalValues.getFractionalPositive())), columnSymbol, Domain.notNull(columnType));
            testSimpleComparison(notEqual(columnSymbol, literalConstructor.apply(literalValues.getFractionalNegative())), columnSymbol, Domain.notNull(columnType));
        }

        // is distinct from
        testSimpleComparison(isDistinctFrom(columnSymbol, literalConstructor.apply(literalValues.getPositive())), columnSymbol, Domain.create(ValueSet.ofRanges(Range.lessThan(columnType, columnValues.getPositive()), Range.greaterThan(columnType, columnValues.getPositive())), true));
        testSimpleComparison(isDistinctFrom(columnSymbol, literalConstructor.apply(literalValues.getNegative())), columnSymbol, Domain.create(ValueSet.ofRanges(Range.lessThan(columnType, columnValues.getNegative()), Range.greaterThan(columnType, columnValues.getNegative())), true));
        testSimpleComparison(isDistinctFrom(columnSymbol, literalConstructor.apply(literalValues.getMax())), columnSymbol, Domain.all(columnType));
        testSimpleComparison(isDistinctFrom(columnSymbol, literalConstructor.apply(literalValues.getMin())), columnSymbol, Domain.all(columnType));
        if (literalValues.isFractional()) {
            testSimpleComparison(isDistinctFrom(columnSymbol, literalConstructor.apply(literalValues.getFractionalPositive())), columnSymbol, Domain.all(columnType));
            testSimpleComparison(isDistinctFrom(columnSymbol, literalConstructor.apply(literalValues.getFractionalNegative())), columnSymbol, Domain.all(columnType));
        }
    }

    @Test
    public void testVarcharComparedToCharExpression()
            throws Exception
    {
        // greater than or equal
        testSimpleComparison(greaterThanOrEqual(C_CHAR, stringLiteral("123456789")), C_CHAR, Range.greaterThan(createCharType(10), Slices.utf8Slice("12345678")));
        testSimpleComparison(greaterThanOrEqual(C_CHAR, stringLiteral("1234567890")), C_CHAR, Range.greaterThanOrEqual(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(greaterThanOrEqual(C_CHAR, stringLiteral("12345678901")), C_CHAR, Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890")));

        // greater than
        testSimpleComparison(greaterThan(C_CHAR, stringLiteral("123456789")), C_CHAR, Range.greaterThan(createCharType(10), Slices.utf8Slice("12345678")));
        testSimpleComparison(greaterThan(C_CHAR, stringLiteral("1234567890")), C_CHAR, Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(greaterThan(C_CHAR, stringLiteral("12345678901")), C_CHAR, Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890")));

        // less than or equal
        testSimpleComparison(lessThanOrEqual(C_CHAR, stringLiteral("123456789")), C_CHAR, Range.lessThanOrEqual(createCharType(10), Slices.utf8Slice("12345678")));
        testSimpleComparison(lessThanOrEqual(C_CHAR, stringLiteral("1234567890")), C_CHAR, Range.lessThanOrEqual(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(lessThanOrEqual(C_CHAR, stringLiteral("12345678901")), C_CHAR, Range.lessThanOrEqual(createCharType(10), Slices.utf8Slice("1234567890")));

        // less than
        testSimpleComparison(lessThan(C_CHAR, stringLiteral("123456789")), C_CHAR, Range.lessThanOrEqual(createCharType(10), Slices.utf8Slice("12345678")));
        testSimpleComparison(lessThan(C_CHAR, stringLiteral("1234567890")), C_CHAR, Range.lessThan(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(lessThan(C_CHAR, stringLiteral("12345678901")), C_CHAR, Range.lessThanOrEqual(createCharType(10), Slices.utf8Slice("1234567890")));

        // equal
        testSimpleComparison(equal(C_CHAR, stringLiteral("123456789")), C_CHAR, Domain.none(createCharType(10)));
        testSimpleComparison(equal(C_CHAR, stringLiteral("1234567890")), C_CHAR, Range.equal(createCharType(10), Slices.utf8Slice("1234567890")));
        testSimpleComparison(equal(C_CHAR, stringLiteral("12345678901")), C_CHAR, Domain.none(createCharType(10)));

        // not equal
        testSimpleComparison(notEqual(C_CHAR, stringLiteral("123456789")), C_CHAR, Domain.notNull(createCharType(10)));
        testSimpleComparison(notEqual(C_CHAR, stringLiteral("1234567890")), C_CHAR, Domain.create(ValueSet.ofRanges(
                Range.lessThan(createCharType(10), Slices.utf8Slice("1234567890")), Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890"))), false));
        testSimpleComparison(notEqual(C_CHAR, stringLiteral("12345678901")), C_CHAR, Domain.notNull(createCharType(10)));

        // is distinct from
        testSimpleComparison(isDistinctFrom(C_CHAR, stringLiteral("123456789")), C_CHAR, Domain.all(createCharType(10)));
        testSimpleComparison(isDistinctFrom(C_CHAR, stringLiteral("1234567890")), C_CHAR, Domain.create(ValueSet.ofRanges(
                Range.lessThan(createCharType(10), Slices.utf8Slice("1234567890")), Range.greaterThan(createCharType(10), Slices.utf8Slice("1234567890"))), true));
        testSimpleComparison(isDistinctFrom(C_CHAR, stringLiteral("12345678901")), C_CHAR, Domain.all(createCharType(10)));
    }

    private static ExtractionResult fromPredicate(Expression originalPredicate)
    {
        return DomainTranslator.fromPredicate(METADATA, TEST_SESSION, originalPredicate, TYPES);
    }

    private static Expression toPredicate(TupleDomain<Symbol> tupleDomain)
    {
        return DomainTranslator.toPredicate(tupleDomain);
    }

    private static Expression unprocessableExpression1(Symbol symbol)
    {
        return comparison(GREATER_THAN, reference(symbol), reference(symbol));
    }

    private static Expression unprocessableExpression2(Symbol symbol)
    {
        return comparison(LESS_THAN, reference(symbol), reference(symbol));
    }

    private static Expression randPredicate(Symbol symbol)
    {
        return comparison(GREATER_THAN, reference(symbol), new FunctionCall(new QualifiedName("rand"), ImmutableList.<Expression>of()));
    }

    private static QualifiedNameReference reference(Symbol symbol)
    {
        return new QualifiedNameReference(symbol.toQualifiedName());
    }

    private static NotExpression not(Expression expression)
    {
        return new NotExpression(expression);
    }

    private static ComparisonExpression comparison(ComparisonExpression.Type type, Expression expression1, Expression expression2)
    {
        return new ComparisonExpression(type, expression1, expression2);
    }

    private static ComparisonExpression equal(Symbol symbol, Expression expression)
    {
        return comparison(EQUAL, reference(symbol), expression);
    }

    private static ComparisonExpression notEqual(Symbol symbol, Expression expression)
    {
        return comparison(NOT_EQUAL, reference(symbol), expression);
    }

    private static ComparisonExpression greaterThan(Symbol symbol, Expression expression)
    {
        return comparison(GREATER_THAN, reference(symbol), expression);
    }

    private static ComparisonExpression greaterThanOrEqual(Symbol symbol, Expression expression)
    {
        return comparison(GREATER_THAN_OR_EQUAL, reference(symbol), expression);
    }

    private static ComparisonExpression lessThan(Symbol symbol, Expression expression)
    {
        return comparison(LESS_THAN, reference(symbol), expression);
    }

    private static ComparisonExpression lessThanOrEqual(Symbol symbol, Expression expression)
    {
        return comparison(LESS_THAN_OR_EQUAL, reference(symbol), expression);
    }

    private static ComparisonExpression isDistinctFrom(Symbol symbol, Expression expression)
    {
        return comparison(IS_DISTINCT_FROM, reference(symbol), expression);
    }

    private static Expression isNotNull(Symbol symbol)
    {
        return new NotExpression(new IsNullPredicate(reference(symbol)));
    }

    private static IsNullPredicate isNull(Symbol symbol)
    {
        return new IsNullPredicate(reference(symbol));
    }

    private static InPredicate in(Symbol symbol, List<?> values)
    {
        List<Type> types = nCopies(values.size(), TYPES.get(symbol));
        List<Expression> expressions = LiteralInterpreter.toExpressions(values, types);
        return new InPredicate(reference(symbol), new InListExpression(expressions));
    }

    private static BetweenPredicate between(Symbol symbol, Expression min, Expression max)
    {
        return new BetweenPredicate(reference(symbol), min, max);
    }

    private static Literal bigintLiteral(long value)
    {
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return new GenericLiteral("BIGINT", Long.toString(value));
        }
        return new LongLiteral(Long.toString(value));
    }

    private static Literal integerLiteral(long value)
    {
        checkArgument(value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE);
        return new GenericLiteral("INTEGER", Long.toString(value));
    }

    private static Literal smallintLiteral(long value)
    {
        checkArgument(value >= Short.MIN_VALUE && value <= Short.MAX_VALUE);
        return new GenericLiteral("SMALLINT", Long.toString(value));
    }

    private static Literal tinyintLiteral(long value)
    {
        checkArgument(value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE);
        return new GenericLiteral("TINYINT", Long.toString(value));
    }

    private static DoubleLiteral doubleLiteral(double value)
    {
        return new DoubleLiteral(Double.toString(value));
    }

    private static DecimalLiteral decimalLiteral(String value)
    {
        return new DecimalLiteral(value);
    }

    private static StringLiteral stringLiteral(String value)
    {
        return new StringLiteral(value);
    }

    private static NullLiteral nullLiteral()
    {
        return new NullLiteral();
    }

    private static FunctionCall colorLiteral(long value)
    {
        return new FunctionCall(QualifiedName.of(getMagicLiteralFunctionSignature(COLOR).getName()), ImmutableList.<Expression>of(bigintLiteral(value)));
    }

    private static Expression varbinaryLiteral(Slice value)
    {
        return LiteralInterpreter.toExpression(value, VARBINARY);
    }

    private static FunctionCall function(String functionName, Expression... args)
    {
        return new FunctionCall(QualifiedName.of(functionName), ImmutableList.copyOf(args));
    }

    private static Long shortDecimal(String value)
    {
        return new BigDecimal(value).unscaledValue().longValueExact();
    }

    private static Slice longDecimal(String value)
    {
        return encodeScaledValue(new BigDecimal(value));
    }

    private static void testSimpleComparison(Expression expression, Symbol symbol, Range expectedDomainRange)
    {
        testSimpleComparison(expression, symbol, Domain.create(ValueSet.ofRanges(expectedDomainRange), false));
    }

    private static void testSimpleComparison(Expression expression, Symbol symbol, Domain domain)
    {
        ExtractionResult result = fromPredicate(expression);
        assertEquals(result.getRemainingExpression(), TRUE_LITERAL);
        TupleDomain<Symbol> actual = result.getTupleDomain();
        TupleDomain<Symbol> expected = withColumnDomains(ImmutableMap.of(symbol, domain));
        if (!actual.equals(expected)) {
            fail(format("for comparison [%s] expected %s but found %s", expression.toString(), expected.toString(SESSION), actual.toString(SESSION)));
        }
    }

    private static class NumericValues
    {
        private final Type type;
        private final Symbol column;
        private final Function<Object, Expression> literalConstructor;
        private final Object min;
        private final Object negative;
        private final Object fractionalNegative;
        private final Object positive;
        private final Object fractionalPositive;
        private final Object max;

        public NumericValues(Symbol column, Function<Object, Expression> literalConstructor, Object min, Object negative, Object fractionalNegative, Object positive, Object fractionalPositive, Object max)
        {
            this.type = TYPES.get(column);
            this.column = column;
            this.literalConstructor = literalConstructor;
            this.min = min;
            this.negative = negative;
            this.fractionalNegative = fractionalNegative;
            this.positive = positive;
            this.fractionalPositive = fractionalPositive;
            this.max = max;
        }

        public Type getType()
        {
            return type;
        }

        public Symbol getColumn()
        {
            return column;
        }

        public Function<Object, Expression> getLiteralConstructor()
        {
            return literalConstructor;
        }

        public Object getMin()
        {
            return min;
        }

        public Object getNegative()
        {
            return negative;
        }

        public Object getFractionalNegative()
        {
            return fractionalNegative;
        }

        public Object getPositive()
        {
            return positive;
        }

        public Object getFractionalPositive()
        {
            return fractionalPositive;
        }

        public Object getMax()
        {
            return max;
        }

        public boolean isFractional()
        {
            return type == DOUBLE || type == FLOAT || (type instanceof DecimalType && ((DecimalType) type).getScale() > 0);
        }
    }
}
