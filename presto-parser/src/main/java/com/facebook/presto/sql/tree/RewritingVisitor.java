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
package com.facebook.presto.sql.tree;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.sql.util.ImmutableCollectors.toImmutableMap;

public abstract class RewritingVisitor<C>
        extends AstVisitor<Node, C>
{
    @Override
    protected Node visitNode(Node node, C context)
    {
        throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
    }

    @Override
    protected Node visitExpression(Expression node, C context)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>(), node, context);
    }

    private List<Expression> processExpressionList(List<Expression> expressions, C context)
    {
        return expressions.stream().map(value -> (Expression) process(value, context)).collect(toImmutableList());
    }

    @Override
    protected Node visitQuery(Query node, C context)
    {
        Optional<With> with = Optional.empty();
        if (node.getWith().isPresent()) {
            with = Optional.of((With) process(node.getWith().get(), context));
        }

        QueryBody queryBody = (QueryBody) process(node.getQueryBody(), context);

        List<SortItem> orderBy = node.getOrderBy().stream().map(value -> (SortItem) process(value, context)).collect(toImmutableList());
        return new Query(node.getLocation(), with, queryBody, orderBy, node.getLimit(), node.getApproximate());
    }

    @Override
    protected Node visitWith(With node, C context)
    {
        List<WithQuery> queries = node.getQueries().stream().map(value -> (WithQuery) process(value, context)).collect(toImmutableList());
        return new With(node.getLocation(), node.isRecursive(), queries);
    }

    @Override
    protected Node visitWithQuery(WithQuery node, C context)
    {
        Query query = (Query) process(node.getQuery(), context);
        return new WithQuery(node.getLocation(), node.getName(), query, node.getColumnNames());
    }

    @Override
    protected Node visitApproximate(Approximate node, C context)
    {
        return node;
    }

    @Override
    protected Node visitSelect(Select node, C context)
    {
        List<SelectItem> selectItems = node.getSelectItems().stream().map(value -> (SelectItem) process(value, context)).collect(toImmutableList());
        return new Select(node.getLocation(), node.isDistinct(), selectItems);
    }

    @Override
    protected Node visitSingleColumn(SingleColumn node, C context)
    {
        Expression expression = (Expression) process(node.getExpression(), context);

        return new SingleColumn(node.getLocation(), expression, node.getAlias());
    }

    @Override
    protected Node visitAllColumns(AllColumns node, C context)
    {
        return node;
    }

    @Override
    public Node visitWindow(Window node, C context)
    {
        List<Expression> partitionBy = processExpressionList(node.getPartitionBy(), context);

        List<SortItem> orderBy = node.getOrderBy().stream().map(value -> (SortItem) process(value, context)).collect(toImmutableList());

        Optional<WindowFrame> frame = Optional.empty();
        if (node.getFrame().isPresent()) {
            frame = Optional.of((WindowFrame) process(node.getFrame().get(), context));
        }

        return new Window(node.getLocation(), partitionBy, orderBy, frame);
    }

    @Override
    public Node visitWindowFrame(WindowFrame node, C context)
    {
        FrameBound start = (FrameBound) process(node.getStart(), context);
        Optional<FrameBound> end = Optional.empty();
        if (node.getEnd().isPresent()) {
            end = Optional.of((FrameBound) process(node.getEnd().get(), context));
        }

        return new WindowFrame(node.getLocation(), node.getType(), start, end);
    }

    @Override
    public Node visitFrameBound(FrameBound node, C context)
    {
        Expression value = null;
        if (node.getValue().isPresent()) {
            value = (Expression) process(node.getValue().get(), context);
        }

        return new FrameBound(node.getLocation(), node.getType(), value);
    }

    @Override
    protected Node visitSortItem(SortItem node, C context)
    {
        Expression sortKey = (Expression) process(node.getSortKey(), context);
        return new SortItem(node.getLocation(), sortKey, node.getOrdering(), node.getNullOrdering());
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, C context)
    {
        Select select = (Select) process(node.getSelect(), context);

        Optional<Relation> from = Optional.empty();
        if (node.getFrom().isPresent()) {
            from = Optional.of((Relation) process(node.getFrom().get(), context));
        }

        Optional<Expression> where = Optional.empty();
        if (node.getWhere().isPresent()) {
            where = Optional.of((Expression) process(node.getWhere().get(), context));
        }

        Optional<GroupBy> groupBy = Optional.empty();
        if (node.getGroupBy().isPresent()) {
            groupBy = Optional.of((GroupBy) process(node.getGroupBy().get(), context));
        }

        Optional<Expression> having = Optional.empty();
        if (node.getHaving().isPresent()) {
            having = Optional.of((Expression) process(node.getHaving().get(), context));
        }

        List<SortItem> orderBy = node.getOrderBy().stream().map(value -> (SortItem) process(value, context)).collect(toImmutableList());

        return new QuerySpecification(node.getLocation(), select, from, where, groupBy, having, orderBy, node.getLimit());
    }

    @Override
    protected Node visitUnion(Union node, C context)
    {
        List<Relation> relations = node.getRelations().stream().map(value -> (Relation) process(value, context)).collect(toImmutableList());
        return new Union(node.getLocation(), relations, node.isDistinct());
    }

    @Override
    protected Node visitIntersect(Intersect node, C context)
    {
        List<Relation> relations = (node.getRelations().stream().map(value -> (Relation) process(value, context)).collect(toImmutableList()));
        return new Intersect(node.getLocation(), relations, node.isDistinct());
    }

    @Override
    protected Node visitExcept(Except node, C context)
    {
        Relation left = (Relation) process(node.getLeft(), context);
        Relation right = (Relation) process(node.getRight(), context);
        return new Except(node.getLocation(), left, right, node.isDistinct());
    }

    @Override
    protected Node visitValues(Values node, C context)
    {
        List<Expression> rows = processExpressionList(node.getRows(), context);
        return new Values(node.getLocation(), rows);
    }

    @Override
    protected Node visitRow(Row node, C context)
    {
        List<Expression> items = processExpressionList(node.getItems(), context);
        return new Row(node.getLocation(), items);
    }

    @Override
    protected Node visitTable(Table node, C context)
    {
        return node;
    }

    @Override
    protected Node visitTableSubquery(TableSubquery node, C context)
    {
        Query query = (Query) process(node.getQuery(), context);
        return new TableSubquery(node.getLocation(), query);
    }

    @Override
    protected Node visitAliasedRelation(AliasedRelation node, C context)
    {
        Relation relation = (Relation) process(node.getRelation(), context);
        return new AliasedRelation(node.getLocation(), relation, node.getAlias(), node.getColumnNames());
    }

    @Override
    protected Node visitSampledRelation(SampledRelation node, C context)
    {
        Relation relation = (Relation) process(node.getRelation(), context);
        Expression sampledPercentage = (Expression) process(node.getSamplePercentage(), context);
        Optional<List<Expression>> columnsToStratifyOn = Optional.empty();
        if (node.getColumnsToStratifyOn().isPresent()) {
            columnsToStratifyOn = Optional.of(processExpressionList(node.getColumnsToStratifyOn().get(), context));
        }
        return new SampledRelation(node.getLocation(), relation, node.getType(), sampledPercentage, node.isRescaled(), columnsToStratifyOn);
    }

    @Override
    protected Node visitJoin(Join node, C context)
    {
        Relation left = (Relation) process(node.getLeft(), context);
        Relation right = (Relation) process(node.getRight(), context);

        Optional<JoinCriteria> criteria = node.getCriteria();
        if (criteria.isPresent() && criteria.get() instanceof JoinOn) {
            Expression expression = (Expression) process(((JoinOn) criteria.get()).getExpression(), context);
            criteria = Optional.of(new JoinOn(expression));
        }

        return new Join(node.getLocation(), node.getType(), left, right, criteria);
    }

    @Override
    protected Node visitUnnest(Unnest node, C context)
    {
        List<Expression> expression = processExpressionList(node.getExpressions(), context);

        return new Unnest(node.getLocation(), expression, node.isWithOrdinality());
    }

    @Override
    protected Node visitInsert(Insert node, C context)
    {
        Query query = (Query) process(node.getQuery(), context);
        return new Insert(node.getLocation(), node.getColumns(), node.getTarget(), query);
    }

    @Override
    protected Node visitGroupBy(GroupBy node, C context)
    {
        List<GroupingElement> groupingElements = node.getGroupingElements().stream().map(value -> (GroupingElement) process(value, context)).collect(toImmutableList());
        return new GroupBy(node.getLocation(), node.isDistinct(), groupingElements);
    }

    @Override
    protected Node visitSimpleGroupBy(SimpleGroupBy node, C context)
    {
        List<Expression> groupByExpressions = processExpressionList(node.getColumnExpressions(), context);
        return new SimpleGroupBy(node.getLocation(), groupByExpressions);
    }

    @Override
    protected Node visitGroupingElement(GroupingElement node, C context)
    {
        return node;
    }

    @Override
    protected Node visitCube(Cube node, C context)
    {
        return node;
    }

    @Override
    protected Node visitGroupingSets(GroupingSets node, C context)
    {
        return node;
    }

    @Override
    protected Node visitRollup(Rollup node, C context)
    {
        return visitGroupingElement(node, context);
    }

    @Override
    protected Node visitShowTables(ShowTables node, C context)
    {
        return node;
    }

    @Override
    protected Node visitShowSchemas(ShowSchemas node, C context)
    {
        return node;
    }

    @Override
    protected Node visitShowCatalogs(ShowCatalogs node, C context)
    {
        return node;
    }

    @Override
    protected Node visitShowColumns(ShowColumns node, C context)
    {
        return node;
    }

    @Override
    protected Node visitShowPartitions(ShowPartitions node, C context)
    {
        Optional<Expression> where = Optional.empty();
        if (node.getWhere().isPresent()) {
            where = Optional.of((Expression) process(node.getWhere().get(), context));
        }
        List<SortItem> orderBy = node.getOrderBy().stream().map(value -> (SortItem) process(value, context)).collect(toImmutableList());
        return new ShowPartitions(node.getLocation(), node.getTable(), where, orderBy, node.getLimit());
    }

    @Override
    protected Node visitShowCreate(ShowCreate node, C context)
    {
        return node;
    }

    @Override
    protected Node visitShowFunctions(ShowFunctions node, C context)
    {
        return node;
    }

    @Override
    protected Node visitUse(Use node, C context)
    {
        return node;
    }

    @Override
    protected Node visitShowSession(ShowSession node, C context)
    {
        return node;
    }

    @Override
    protected Node visitSetSession(SetSession node, C context)
    {
        return new SetSession(node.getLocation(), node.getName(), (Expression) process(node.getValue(), context));
    }

    @Override
    public Node visitResetSession(ResetSession node, C context)
    {
        return node;
    }

    @Override
    protected Node visitCallArgument(CallArgument node, C context)
    {
        return new CallArgument(node.getLocation(), node.getName(), (Expression) process(node, context));
    }

    @Override
    protected Node visitTableElement(TableElement node, C context)
    {
        return node;
    }

    @Override
    protected Node visitCreateTable(CreateTable node, C context)
    {
        Map<String, Expression> properties = node.getProperties().entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, e -> (Expression) process(node, context)));
        return new CreateTable(node.getLocation(), node.getName(), node.getElements(), node.isNotExists(), properties);
    }

    @Override
    protected Node visitCreateTableAsSelect(CreateTableAsSelect node, C context)
    {
        Query query = (Query) process(node.getQuery(), context);
        Map<String, Expression> properties = node.getProperties().entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, e -> (Expression) process(node, context)));
        return new CreateTableAsSelect(node.getLocation(), node.getName(), query, node.isNotExists(), properties, node.isWithData());
    }

    @Override
    protected Node visitDropTable(DropTable node, C context)
    {
        return node;
    }

    @Override
    protected Node visitRenameTable(RenameTable node, C context)
    {
        return node;
    }

    @Override
    protected Node visitRenameColumn(RenameColumn node, C context)
    {
        return node;
    }

    @Override
    protected Node visitAddColumn(AddColumn node, C context)
    {
        return new AddColumn(node.getLocation(), node.getName(), (TableElement) process(node.getColumn(), context));
    }

    @Override
    protected Node visitCreateView(CreateView node, C context)
    {
        return new CreateView(node.getLocation(), node.getName(), (Query) process(node.getQuery(), context), node.isReplace());
    }

    @Override
    protected Node visitDropView(DropView node, C context)
    {
        return node;
    }

    @Override
    protected Node visitCall(Call node, C context)
    {
        List<CallArgument> callArguments = node.getArguments().stream().map(value -> (CallArgument) process(value, context)).collect(toImmutableList());
        return new Call(node.getLocation(), node.getName(), callArguments);
    }

    @Override
    protected Node visitDelete(Delete node, C context)
    {
        Table table = (Table) process(node.getTable(), context);
        Optional<Expression> where = Optional.empty();
        if (node.getWhere().isPresent()) {
            where = Optional.of((Expression) process(node.getWhere().get(), context));
        }
        return new Delete(node.getLocation(), table, where);
    }

    @Override
    protected Node visitStartTransaction(StartTransaction node, C context)
    {
        List<TransactionMode> transactionModes = node.getTransactionModes().stream().map(value -> (TransactionMode) process(node, context)).collect(toImmutableList());
        return new StartTransaction(node.getLocation(), transactionModes);
    }

    @Override
    protected Node visitGrant(Grant node, C context)
    {
        return node;
    }

    @Override
    protected Node visitRevoke(Revoke node, C context)
    {
        return node;
    }

    @Override
    protected Node visitTransactionMode(TransactionMode node, C context)
    {
        return node;
    }

    @Override
    protected Node visitIsolationLevel(Isolation node, C context)
    {
        return node;
    }

    @Override
    protected Node visitTransactionAccessMode(TransactionAccessMode node, C context)
    {
        return node;
    }

    @Override
    protected Node visitCommit(Commit node, C context)
    {
        return node;
    }

    @Override
    protected Node visitRollback(Rollback node, C context)
    {
        return node;
    }

    @Override
    protected Node visitExplain(Explain node, C context)
    {
        Statement statement = (Statement) process(node.getStatement(), context);
        List<ExplainOption> explainOptions = node.getOptions().stream().map(value -> (ExplainOption) process(node, context)).collect(toImmutableList());

        return new Explain(node.getLocation(), node.isAnalyze(), statement, explainOptions);
    }
}
