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
package com.facebook.presto.sql.rewrite;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.ShowStats;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.Values;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.sql.QueryUtil.aliased;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.rewrite.ShowColumnStatsRewriteResultBuilder.buildColumnsNames;
import static com.facebook.presto.sql.rewrite.ShowColumnStatsRewriteResultBuilder.buildSelectItems;
import static com.facebook.presto.sql.rewrite.ShowColumnStatsRewriteResultBuilder.buildStatisticsRows;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class ShowStatsRewrite
        implements StatementRewrite.Rewrite
{
    @Override
    public Statement rewrite(Session session, Metadata metadata, SqlParser parser, Optional<QueryExplainer> queryExplainer, Statement node, List<Expression> parameters, AccessControl accessControl)
    {
        return (Statement) new Visitor(metadata, session, parser, parameters, accessControl).process(node, null);
    }

    private static class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final SqlParser sqlParser;
        private final List<Expression> parameters;
        private final AccessControl accessControl;

        public Visitor(Metadata metadata, Session session, SqlParser sqlParser, List<Expression> parameters, AccessControl accessControl)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser can not be null");
            this.parameters = requireNonNull(parameters, "parameters is null");
            this.accessControl = requireNonNull(accessControl, "accessControl can not be null");
        }

        @Override
        protected Node visitShowStats(ShowStats node, Void context)
        {
            checkArgument(node.getQuery().getQueryBody() instanceof QuerySpecification);
            QuerySpecification specification = (QuerySpecification) node.getQuery().getQueryBody();

            checkArgument(specification.getFrom().isPresent());
            checkArgument(specification.getFrom().get() instanceof Table);
            Table table = (Table) specification.getFrom().get();
            TableHandle tableHandle = getTableHandle(node, table.getName());
            Constraint<ColumnHandle> constraint = getConstraint(node, tableHandle);

            TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle, constraint);

            List<String> statisticsNames = findUniqueStatisticsNames(tableStatistics);

            List<String> resultColumnNames = buildColumnsNames(statisticsNames);
            SelectItem[] selectItems = buildSelectItems(resultColumnNames);

            Map<ColumnHandle, String> columnNames = getStatisticsColumnNames(tableStatistics, node, table.getName());
            List<Expression> resultRows = buildStatisticsRows(tableStatistics, columnNames, statisticsNames);

            return simpleQuery(selectList(selectItems),
                    aliased(new Values(resultRows),
                            "table_stats_for_" + table.getName(),
                            resultColumnNames));
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }

        private Constraint<ColumnHandle> getConstraint(ShowStats node, TableHandle tableHandle)
        {
            checkArgument(node.getQuery().getQueryBody() instanceof QuerySpecification);
            QuerySpecification specification = (QuerySpecification) node.getQuery().getQueryBody();

            if (specification.getWhere().isPresent()) {
                return getConstraintWithWhere(node, tableHandle);
            }
            return Constraint.alwaysTrue();
        }

        private Constraint<ColumnHandle> getConstraintWithWhere(ShowStats node, TableHandle tableHandle)
        {
            // analyze statement
            QuerySpecification querySpecification = (QuerySpecification) node.getQuery().getQueryBody();
            Query query = new Query(Optional.empty(), querySpecification, ImmutableList.of(), Optional.empty());
            Analyzer analyzer = new Analyzer(session, metadata, sqlParser, accessControl, Optional.empty(), parameters);
            Analysis analysis = analyzer.analyze(query);

            // create domain
            Expression filter = ((QuerySpecification) node.getQuery().getQueryBody()).getWhere().get();
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);

            // rewrite filter expression to use symbols in place of column references
            final Map<QualifiedNameReference, Symbol> symbolMap = new HashMap<>();
            Expression filterUsingSymbols = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Object>() {
                @Override
                public Expression rewriteQualifiedNameReference(QualifiedNameReference node, Object context, ExpressionTreeRewriter<Object> treeRewriter)
                {
                    String symbolName = node.getName().getSuffix();
                    symbolMap.put(node, new Symbol(symbolName));
                    return new SymbolReference(symbolName);
                }
            }, filter);

            // compute types for symbols
            Map<Symbol, Type> symbolTypes = symbolMap.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getValue,
                    entry -> analysis.getType(entry.getKey())
            ));

            // compute tuple domain
            TupleDomain<Symbol> symbolTupleDomain = DomainTranslator.fromPredicate(metadata, session, filterUsingSymbols, symbolTypes).getTupleDomain();

            // transform computed tuple domain to use ColumnHandles
            TupleDomain<ColumnHandle> columnTupleDomain = symbolTupleDomain.transform(symbol -> columnHandles.get(symbol.getName()));

            return new Constraint<>(columnTupleDomain, bindings -> true);
        }

        private Map<ColumnHandle, String> getStatisticsColumnNames(TableStatistics statistics, ShowStats node, QualifiedName tableName)
        {
            ImmutableMap.Builder<ColumnHandle, String> resultBuilder = ImmutableMap.builder();
            TableHandle tableHandle = getTableHandle(node, tableName);

            for (ColumnHandle column : statistics.getColumnStatistics().keySet()) {
                resultBuilder.put(column, metadata.getColumnMetadata(session, tableHandle, column).getName());
            }

            return resultBuilder.build();
        }

        private TableHandle getTableHandle(ShowStats node, QualifiedName table)
        {
            QualifiedObjectName qualifiedTableName = createQualifiedObjectName(session, node, table);
            return metadata.getTableHandle(session, qualifiedTableName)
                    .orElseThrow(() -> new SemanticException(MISSING_TABLE, node, "Table %s not found", table));
        }

        private static List<String> findUniqueStatisticsNames(TableStatistics tableStatistics)
        {
            TreeSet<String> statisticsKeys = new TreeSet<>();
            statisticsKeys.addAll(tableStatistics.getTableStatistics().keySet());
            for (ColumnStatistics columnStats : tableStatistics.getColumnStatistics().values()) {
                statisticsKeys.addAll(columnStats.getStatistics().keySet());
            }
            return unmodifiableList(new ArrayList(statisticsKeys));
        }

        private static Optional<TableScanNode> findTableScanNode(Plan plan)
        {
            TableScanNode scanNode = searchFrom(plan.getRoot()).where(TableScanNode.class::isInstance).findOnlyElement(null);
            return Optional.ofNullable(scanNode);
        }

        private static Expression findFilterExpression(Plan plan)
        {
            FilterNode filterNode = searchFrom(plan.getRoot()).where(FilterNode.class::isInstance).findOnlyElement(null);
            return Optional.ofNullable(filterNode).map(FilterNode::getPredicate).orElse(TRUE_LITERAL);
        }
    }
}
