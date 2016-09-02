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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanTester;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.inject.Provider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.ExpressionUtils.binaryExpression;
import static com.facebook.presto.sql.ExpressionUtils.extractPredicates;
import static com.facebook.presto.sql.ExpressionUtils.rewriteQualifiedNamesToSymbolReferences;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestSimplifyExpressions
    implements PlanTester
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final SimplifyExpressions SIMPLIFIER = new SimplifyExpressions(createTestMetadataManager(), SQL_PARSER);
    private LocalQueryRunner queryRunner;
    private Map<Symbol, Expression> inputAssignments;
    private Map<Symbol, Expression> expectedAssignments;
    private Map<Symbol, Type> typeAssignments;

    @BeforeTest
    public void setUp()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(queryRunner.getNodeManager(), 1),
                ImmutableMap.<String, String>of());

        inputAssignments = new HashMap<Symbol, Expression>();
        expectedAssignments = new HashMap<Symbol, Expression>();
        typeAssignments = new HashMap<Symbol, Type>();
    }

    @Test
    public void testPushesDownNegations()
    {
        assertSimplifies("NOT X", "NOT X");
        assertSimplifies("NOT NOT X", "X");
        assertSimplifies("NOT NOT NOT X", "NOT X");
        assertSimplifies("NOT NOT NOT X", "NOT X");

        assertSimplifies("NOT (X > Y)", "X <= Y");
        assertSimplifies("NOT (X > (NOT NOT Y))", "X <= Y");
        assertSimplifies("X > (NOT NOT Y)", "X > Y");
        assertSimplifies("NOT (X AND Y AND (NOT (Z OR V)))", "(NOT X) OR (NOT Y) OR (Z OR V)");
        assertSimplifies("NOT (X OR Y OR (NOT (Z OR V)))", "(NOT X) AND (NOT Y) AND (Z OR V)");
        assertSimplifies("NOT (X OR Y OR (Z OR V))", "(NOT X) AND (NOT Y) AND ((NOT Z) AND (NOT V))");

        assertSimplifies("NOT (X IS DISTINCT FROM Y)", "NOT (X IS DISTINCT FROM Y)");
    }

    @Test
    public void testExtractsCommonPredicate()
    {
        assertSimplifies("X AND X", "X");
        assertSimplifies("X OR X", "X");
        assertSimplifies("(X OR Y) AND (X OR Y)", "X OR Y");

        assertSimplifies("(A AND V) OR V", "V");
        assertSimplifies("(A OR V) AND V", "V");
        assertSimplifies("(A OR B OR C) AND (A OR B)", "A OR B");
        assertSimplifies("(A AND B) OR (A AND B AND C)", "A AND B");
        assertSimplifies("I = ((A OR B) AND (A OR B OR C))", "I = (A OR B)");
        assertSimplifies("(X OR Y) AND (X OR Z)", "(X OR Y) AND (X OR Z)");
        assertSimplifies("(X AND Y AND V) OR (X AND Y AND Z)", "(X AND Y) AND (V OR Z)");
        assertSimplifies("((X OR Y OR V) AND (X OR Y OR Z)) = I", "((X OR Y) OR (V AND Z)) = I");

        assertSimplifies("((X OR V) AND V) OR ((X OR V) AND V)", "V");
        assertSimplifies("((X OR V) AND X) OR ((X OR V) AND V)", "X OR V");

        assertSimplifies("((X OR V) AND Z) OR ((X OR V) AND V)", "(((X OR V) AND Z) OR V)");
        assertSimplifies("X AND ((Y AND Z) OR (Y AND V) OR (Y AND X))", "X AND Y AND (Z OR V OR X)");
        assertSimplifies("(A AND B AND C AND D) OR (A AND B AND E) OR (A AND F)", "A AND ((B AND C AND D) OR (B AND E) OR F)");
    }

    @Test
    public void testRemoveIdentityCastBigintLiteral()
    {
        inputAssignments.put(new Symbol("expr"), new Cast(new GenericLiteral("BIGINT", "5"), "BIGINT"));
        expectedAssignments.put(new Symbol("expr"), new GenericLiteral("BIGINT", "5"));
        typeAssignments.put(new Symbol("expr"), BigintType.BIGINT);
        ProjectNode simplifiedProjectNode = simplifyProjectNode(inputAssignments, typeAssignments);
        Map<Symbol, Expression> actualAssignments = simplifiedProjectNode.getAssignments();
        assert (actualAssignments.equals(expectedAssignments));
    }

    @Test
    public void testRemoveIdentityCastMultiplyBigintLiteral()
    {
        inputAssignments.put(new Symbol("expr"),
                new Cast(new ArithmeticBinaryExpression(BigintType.BIGINT,
                        new GenericLiteral("BIGINT", "3"),
                        ),
                        "BIGINT"))
    }

        /*
        assertCastNotInPlan("SELECT 3 * CAST(BIGINT '5' as BIGINT)");
        assertCastNotInPlan("SELECT CAST(nationkey AS BIGINT) FROM nation");
        assertCastNotInPlan("SELECT 3 * CAST(nationkey AS BIGINT) FROM nation");
        assertCastNotInPlan("SELECT CAST(nationkey AS BIGINT) FROM nation WHERE nationkey > 10 " +
                "AND nationkey < 20");
        assertCastNotInPlan("SELECT CAST(COUNT(*) AS BIGINT) FROM nation WHERE nationkey > 10 " +
                "AND nationkey < 20");
        assertCastNotInPlan("SELECT CAST(COUNT(*) AS BIGINT) FROM nation WHERE nationkey > 10 " +
                "AND nationkey < 20");
        assertCastNotInPlan("SELECT CAST(COUNT(*) AS BIGINT) AS count FROM nation WHERE nationkey > 10 " +
                "AND nationkey < 20 GROUP BY regionkey HAVING COUNT(*) > 1 ORDER BY count ASC");
        assertCastNotInPlan("SELECT CAST(name AS VARCHAR(25)) FROM nation");

        assertCastInPlan("SELECT CAST(nationkey AS SMALLINT) FROM nation");
        assertCastInPlan("SELECT CAST(name AS VARCHAR(30)) FROM nation");
        assertCastInPlan("SELECT CAST(name AS VARCHAR) FROM nation");*/


    private void assertRemovesIdentityCast()

    public void assertPlanDoesNotMatch(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        Plan actualPlan = plan(sql);
        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertPlanDoesNotMatch(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    public void assertPlanMatches(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        Plan actualPlan = plan(sql);
        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertPlanMatches(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    public Plan plan(@Language("SQL") String sql)
    {
        return queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql));
    }

    private void assertUnitPlanMatches(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        Plan actualPlan = unitPlan(sql);
        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertPlanMatches(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }

    private Plan unitPlan(@Language("SQL") String sql)
    {
        FeaturesConfig featuresConfig = new FeaturesConfig()
                .setExperimentalSyntaxEnabled(true)
                .setDistributedIndexJoinsEnabled(false)
                .setOptimizeHashGeneration(true);
        Metadata metadata = new MetadataManager(featuresConfig,
                new TypeRegistry(),
                )
        Provider<List<PlanOptimizer>> optimizerProvider = () -> ImmutableList.of(
                new UnaliasSymbolReferences(),
                new PruneIdentityProjections(),
                new MergeIdenticalWindows(),
                new PruneUnreferencedOutputs(),
                new SimplifyExpressions());
        return queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql, featuresConfig, optimizerProvider));
    }

    private static void assertSimplifies(String expression, String expected)
    {
        Expression actualExpression = rewriteQualifiedNamesToSymbolReferences(SQL_PARSER.createExpression(expression));
        Expression expectedExpression = rewriteQualifiedNamesToSymbolReferences(SQL_PARSER.createExpression(expected));
        assertEquals(
                normalize(simplifyFilterNode(actualExpression)),
                normalize(expectedExpression));
    }

    private static Expression simplifyFilterNode(Expression expression)
    {
        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
        FilterNode filterNode = new FilterNode(
                planNodeIdAllocator.getNextId(),
                new ValuesNode(planNodeIdAllocator.getNextId(), emptyList(), emptyList()), expression);
        FilterNode simplifiedNode = (FilterNode) SIMPLIFIER.optimize(
                filterNode,
                TEST_SESSION,
                booleanSymbolTypeMapFor(expression),
                new SymbolAllocator(),
                planNodeIdAllocator);
        return simplifiedNode.getPredicate();
    }

    private static ProjectNode simplifyProjectNode(Map<Symbol, Expression> assignments, Map<Symbol, Type> typeAssignments)
    {
        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
        ProjectNode projectNode = new ProjectNode(planNodeIdAllocator.getNextId(),
                new ValuesNode(planNodeIdAllocator.getNextId(), emptyList(), emptyList()),
                assignments);
        return (ProjectNode) SIMPLIFIER.optimize(projectNode,
                TEST_SESSION,
                typeAssignments,
                new SymbolAllocator(),
                planNodeIdAllocator);
    }

    private static Map<Symbol, Type> booleanSymbolTypeMapFor(Expression expression)
    {
        return DependencyExtractor.extractUnique(expression).stream()
                .collect(Collectors.toMap(symbol -> symbol, symbol -> BOOLEAN));
    }

    private static Expression normalize(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new NormalizeExpressionRewriter(), expression);
    }

    private static class NormalizeExpressionRewriter
            extends ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteLogicalBinaryExpression(LogicalBinaryExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            List<Expression> predicates = extractPredicates(node.getType(), node).stream()
                    .map(p -> treeRewriter.rewrite(p, context))
                    .sorted((p1, p2) -> p1.toString().compareTo(p2.toString()))
                    .collect(toList());
            return binaryExpression(node.getType(), predicates);
        }
    }
}
