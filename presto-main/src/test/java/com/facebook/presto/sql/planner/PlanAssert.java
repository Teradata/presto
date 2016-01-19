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

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertTrue;

public class PlanAssert
{
    public static void assertPlan(PlanNode actualPlan, PlanMatcher planMatcher)
    {
        requireNonNull(actualPlan, "root is null");

        assertTrue(actualPlan.accept(new PlanMatchingVisitor(), new PlanMatchingContext(planMatcher)),
                "Plan does not match, because one of the following:");
    }

    public static PlanMatcher node(Class<? extends PlanNode> nodeClass, PlanMatcher... sources)
    {
        return new NodeClassPlanMatcher(nodeClass, ImmutableList.copyOf(sources));
    }

    public static PlanMatcher anyNode(PlanMatcher... sources)
    {
        return new PlanMatcher(ImmutableList.copyOf(sources));
    }

    public static PlanMatcher anyNodesTree(PlanMatcher... sources)
    {
        return new AnyNodesTreePlanNodeMatcher(ImmutableList.copyOf(sources));
    }

    public static PlanMatcher tableScanNode(String expectedTableName)
    {
        return new NodeClassPlanMatcher(TableScanNode.class, ImmutableList.of())
        {
            @Override
            public boolean matches(PlanNode node, SymbolAliases symbolAliases)
            {
                if (super.matches(node, symbolAliases)) {
                    TableScanNode tableScanNode = (TableScanNode) node;
                    String actualTableName = ((TpchTableHandle) tableScanNode.getTable().getConnectorHandle()).getTableName();
                    return expectedTableName.equalsIgnoreCase(actualTableName);
                }
                return false;
            }
        };
    }

    public static PlanMatcher projectNode(PlanMatcher... sources)
    {
        return new NodeClassPlanMatcher(ProjectNode.class, ImmutableList.copyOf(sources));
    }

    public static PlanMatcher semiJoinNode(String sourceSymbolAlias, String filteringSymbolAlias, String outputAlias, PlanMatcher... sources)
    {
        return new NodeClassPlanMatcher(SemiJoinNode.class, ImmutableList.copyOf(sources))
        {
            @Override
            public boolean matches(PlanNode node, SymbolAliases symbolAliases)
            {
                if (super.matches(node, symbolAliases)) {
                    SemiJoinNode semiJoinNode = (SemiJoinNode) node;
                    symbolAliases.put(sourceSymbolAlias, semiJoinNode.getSourceJoinSymbol());
                    symbolAliases.put(filteringSymbolAlias, semiJoinNode.getFilteringSourceJoinSymbol());
                    symbolAliases.put(outputAlias, semiJoinNode.getSemiJoinOutput());

                    return true;
                }
                return false;
            }
        };
    }

    public static PlanMatcher joinNode(List<AliasPair> expectedEquiCriteria, PlanMatcher... sources)
    {
        return new NodeClassPlanMatcher(JoinNode.class, ImmutableList.copyOf(sources)) {
            @Override
            public boolean matches(PlanNode node, SymbolAliases symbolAliases)
            {
                if (super.matches(node, symbolAliases)) {
                    JoinNode joinNode = (JoinNode) node;
                    if (joinNode.getCriteria().size() == expectedEquiCriteria.size()) {
                        int i = 0;
                        for (JoinNode.EquiJoinClause equiJoinClause : joinNode.getCriteria()) {
                            // TODO this does not work as joinNode.getCriteria() order is not deterministic
                            AliasPair expectedEquiClause = expectedEquiCriteria.get(i++);
                            symbolAliases.put(expectedEquiClause.left, equiJoinClause.getLeft());
                            symbolAliases.put(expectedEquiClause.right, equiJoinClause.getRight());
                        }
                        return true;
                    }
                }
                return false;
            }
        };
    }

    public static AliasPair aliasPair(String left, String right) {
        return new AliasPair(left, right);
    }

    public static PlanMatcher filterNode(String predicate, PlanMatcher... sources)
    {
        final Expression expectedPredicate = new SqlParser().createExpression(predicate);
        return new NodeClassPlanMatcher(FilterNode.class, ImmutableList.copyOf(sources))
        {
            @Override
            public boolean matches(PlanNode node, SymbolAliases symbolAliases)
            {
                if (super.matches(node, symbolAliases)) {
                    FilterNode filterNode = (FilterNode) node;
                    Expression actualPredicate = filterNode.getPredicate();

                    return new ExpressionVerifier(symbolAliases).process(actualPredicate, expectedPredicate);
                }
                return false;
            }
        };
    }

    private static class ExpressionVerifier extends AstVisitor<Boolean, Expression> {

        private final SymbolAliases symbolAliases;

        public ExpressionVerifier(SymbolAliases symbolAliases)
        {
            this.symbolAliases = requireNonNull(symbolAliases, "symbolAliases is null");
        }

        @Override
        protected Boolean visitNode(Node node, Expression context)
        {
            throw new IllegalStateException(format("Node %s is not supported", node));
        }

        @Override
        protected Boolean visitNotExpression(NotExpression actual, Expression context)
        {
            if (context instanceof NotExpression) {
                NotExpression expected = (NotExpression) context;
                return process(actual.getValue(), expected.getValue());
            }
            return false;
        }

        @Override
        protected Boolean visitQualifiedNameReference(QualifiedNameReference actual, Expression context)
        {
            if (context instanceof QualifiedNameReference) {
                QualifiedNameReference expected = (QualifiedNameReference) context;
                symbolAliases.put(expected.toString(), Symbol.fromQualifiedName(actual.getName()));
                return true;
            }
            return false;
        }
    }

    public static SymbolMatcherBuilder symbol(String symbolPattern)
    {
        return new SymbolMatcherBuilder(symbolPattern);
    }

    public static class SymbolMatcherBuilder
    {
        private final String pattern;

        private SymbolMatcherBuilder(String pattern)
        {
            this.pattern = pattern;
        }

        public SymbolMatcher as(String alias)
        {
            return new SymbolMatcher(pattern, alias);
        }
    }

    private interface Matcher
    {
        boolean matches(PlanNode node, SymbolAliases symbolAliases);
    }

    private static class SymbolMatcher
            implements Matcher
    {
        private final Pattern pattern;
        private final String alias;

        public SymbolMatcher(String pattern, String alias)
        {
            this.pattern = Pattern.compile(pattern);
            this.alias = alias;
        }

        @Override
        public boolean matches(PlanNode node, SymbolAliases symbolAliases)
        {
            Symbol symbol = null;
            for (Symbol outputSymbol : node.getOutputSymbols()) {
                if (pattern.matcher(outputSymbol.getName()).find()) {
                    checkState(symbol == null, "% symbol was found multiple times in %s", pattern, node.getOutputSymbols());
                    symbol = outputSymbol;
                }
            }
            if (symbol != null) {
                symbolAliases.put(alias, symbol);
                return true;
            }
            return false;
        }
    }

    public static class PlanMatcher
            implements Matcher
    {
        protected final List<PlanMatcher> sources;
        protected final List<Matcher> matchers = new ArrayList<>();

        public PlanMatcher(List<PlanMatcher> sources)
        {
            requireNonNull(sources, "sources are null");

            this.sources = ImmutableList.copyOf(sources);
        }

        public List<PlanMatchState> planMatches(PlanNode planNode, SymbolAliases symbolAliasMap)
        {
            if (matches(planNode, symbolAliasMap)) {
                return ImmutableList.of(new PlanMatchState(sources, symbolAliasMap));
            }
            return ImmutableList.of();
        }

        public boolean isTerminated()
        {
            return false;
        }

        @Override
        public boolean matches(PlanNode node, SymbolAliases symbolAliases)
        {
            return node.getSources().size() == sources.size() && matchers.stream().allMatch(it -> it.matches(node, symbolAliases));
        }

        public PlanMatcher with(Matcher matcher)
        {
            matchers.add(matcher);
            return this;
        }
    }

    private static class NodeClassPlanMatcher
            extends PlanMatcher
    {
        private final Class<? extends PlanNode> nodeClass;

        public NodeClassPlanMatcher(Class<? extends PlanNode> nodeClass, List<PlanMatcher> planMatchers)
        {
            super(planMatchers);
            this.nodeClass = requireNonNull(nodeClass, "nodeClass is null");
        }

        @Override
        public boolean matches(PlanNode node, SymbolAliases symbolAliases)
        {
            return super.matches(node, symbolAliases) && node.getClass().equals(nodeClass);
        }
    }

    private static class AnyNodesTreePlanNodeMatcher
            extends PlanMatcher
    {
        public AnyNodesTreePlanNodeMatcher(List<PlanMatcher> sources)
        {
            super(sources);
        }

        @Override
        public List<PlanMatchState> planMatches(PlanNode planNode, SymbolAliases symbolAliases)
        {
            ImmutableList.Builder<PlanMatchState> states = ImmutableList.builder();
            int sourcesCount = planNode.getSources().size();
            if (sourcesCount > 1) {
                states.add(new PlanMatchState(nCopies(sourcesCount, this), symbolAliases));
            }
            else {
                states.add(new PlanMatchState(ImmutableList.of(this), symbolAliases));
            }
            if (super.matches(planNode, symbolAliases)) {
                states.add(new PlanMatchState(sources, symbolAliases));
            }
            return states.build();
        }

        @Override
        public boolean isTerminated()
        {
            return sources.isEmpty();
        }
    }

    private static class PlanMatchState
    {
        private final List<PlanMatcher> planMatchers;
        private final SymbolAliases symbolAliases;

        private PlanMatchState(List<PlanMatcher> planMatchers, SymbolAliases symbolAliases)
        {
            requireNonNull(symbolAliases, "symbolAliases is null");
            requireNonNull(planMatchers, "matchers is null");
            this.symbolAliases = new SymbolAliases(symbolAliases);
            this.planMatchers = ImmutableList.copyOf(planMatchers);
        }

        public boolean isTerminated()
        {
            return planMatchers.isEmpty() || planMatchers.stream().allMatch(PlanMatcher::isTerminated);
        }

        public PlanMatchingContext createContext(int matcherId)
        {
            checkArgument(matcherId < planMatchers.size(), "mactcherId out of scope");
            return new PlanMatchingContext(symbolAliases, planMatchers.get(matcherId));
        }
    }

    private static class PlanMatchingVisitor
            extends PlanVisitor<PlanMatchingContext, Boolean>
    {
        @Override
        protected Boolean visitPlan(PlanNode node, PlanMatchingContext context)
        {
            List<PlanMatchState> states = context.getPlanMatcher().planMatches(node, context.getSymbolAliasMap());

            if (states.isEmpty()) {
                return false;
            }

            if (node.getSources().isEmpty()) {
                return !filterTerminated(states).isEmpty();
            }

            for (PlanMatchState state : states) {
                checkState(node.getSources().size() == state.planMatchers.size(), "Matchers count does not match count of sources");
                int i = 0;
                boolean sourcesMatch = true;
                for (PlanNode source : node.getSources()) {
                    sourcesMatch = sourcesMatch && source.accept(this, state.createContext(i++));
                }
                if (sourcesMatch) {
                    return true;
                }
            }
            return false;
        }

        private List<PlanMatchState> filterTerminated(List<PlanMatchState> states)
        {
            return states.stream()
                    .filter(PlanMatchState::isTerminated)
                    .collect(toImmutableList());
        }
    }

    private static class PlanMatchingContext
    {
        private final SymbolAliases symbolAliases;
        private final PlanMatcher planMatcher;

        private PlanMatchingContext(PlanMatcher planMatcher) {
            this(new SymbolAliases(), planMatcher);
        }

        private PlanMatchingContext(SymbolAliases symbolAliases, PlanMatcher planMatcher)
        {
            requireNonNull(symbolAliases, "symbolAliases is null");
            requireNonNull(planMatcher, "planMatcher is null");
            this.symbolAliases = new SymbolAliases(symbolAliases);
            this.planMatcher = planMatcher;
        }

        public SymbolAliases getSymbolAliasMap()
        {
            return symbolAliases;
        }

        public PlanMatcher getPlanMatcher()
        {
            return planMatcher;
        }
    }

    private static class SymbolAliases
    {
        private final Multimap<String, Symbol> map;

        private SymbolAliases()
        {
            this.map = ArrayListMultimap.create();
        }

        private SymbolAliases(SymbolAliases symbolAliases)
        {
            requireNonNull(symbolAliases, "symbolAliases are null");
            this.map = ArrayListMultimap.create(symbolAliases.map);
        }

        public void put(String alias, Symbol symbol)
        {
            if (map.containsKey(alias)) {
                checkState(map.get(alias).contains(symbol), "Alias %s points to different symbols %s and %s", alias, symbol, map.get(alias));
            } else {
                map.put(alias, symbol);
            }
        }
    }

    private PlanAssert() {}

    private static class AliasPair {
        private final String left;
        private final String right;

        public AliasPair(String left, String right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }
    }
}
