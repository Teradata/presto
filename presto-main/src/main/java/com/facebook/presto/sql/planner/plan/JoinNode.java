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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Join;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.ScalarQueryUtil.isScalar;
import static com.facebook.presto.sql.planner.plan.JoinNode.Method.DISTRIBUTED;
import static java.util.Objects.requireNonNull;

@Immutable
public class JoinNode
        extends PlanNode
{
    private final Type type;
    private final Method method;
    private final PlanNode left;
    private final PlanNode right;
    private final List<EquiJoinClause> criteria;
    private final Optional<Expression> filter;
    private final Optional<Symbol> leftHashSymbol;
    private final Optional<Symbol> rightHashSymbol;

    @JsonCreator
    public JoinNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("method") Method method,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("right") PlanNode right,
            @JsonProperty("criteria") List<EquiJoinClause> criteria,
            @JsonProperty("filter") Optional<Expression> filter,
            @JsonProperty("leftHashSymbol") Optional<Symbol> leftHashSymbol,
            @JsonProperty("rightHashSymbol") Optional<Symbol> rightHashSymbol)
    {
        super(id);
        requireNonNull(type, "type is null");
        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");
        requireNonNull(criteria, "criteria is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(leftHashSymbol, "leftHashSymbol is null");
        requireNonNull(rightHashSymbol, "rightHashSymbol is null");

        this.type = type;
        this.method = method;
        this.left = left;
        this.right = right;
        this.criteria = ImmutableList.copyOf(criteria);
        this.filter = filter;
        this.leftHashSymbol = leftHashSymbol;
        this.rightHashSymbol = rightHashSymbol;
    }

    public enum Method
    {
        BROADCAST,
        DISTRIBUTED;

        public static Method booleanToJoinMethod(boolean isDistributed)
        {
            return isDistributed ? DISTRIBUTED : BROADCAST;
        }
    }

    public enum Type
    {
        INNER("InnerJoin"),
        LEFT("LeftJoin"),
        RIGHT("RightJoin"),
        FULL("FullJoin");

        private final String joinLabel;

        Type(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }

        public static Type typeConvert(Join.Type joinType)
        {
            // Omit SEMI join types because they must be inferred by the planner and not part of the SQL parse tree
            switch (joinType) {
                case CROSS:
                case IMPLICIT:
                case INNER:
                    return Type.INNER;
                case LEFT:
                    return Type.LEFT;
                case RIGHT:
                    return Type.RIGHT;
                case FULL:
                    return Type.FULL;
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + joinType);
            }
        }
    }

    @JsonProperty("method")
    public Method getMethod()
    {
        return method;
    }

    @JsonProperty("type")
    public Type getType()
    {
        return type;
    }

    @JsonProperty("left")
    public PlanNode getLeft()
    {
        return left;
    }

    @JsonProperty("right")
    public PlanNode getRight()
    {
        return right;
    }

    @JsonProperty("criteria")
    public List<EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty("filter")
    public Optional<Expression> getFilter()
    {
        return filter;
    }

    @JsonProperty("leftHashSymbol")
    public Optional<Symbol> getLeftHashSymbol()
    {
        return leftHashSymbol;
    }

    @JsonProperty("rightHashSymbol")
    public Optional<Symbol> getRightHashSymbol()
    {
        return rightHashSymbol;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(left, right);
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(left.getOutputSymbols())
                .addAll(right.getOutputSymbols())
                .build();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitJoin(this, context);
    }

    public static class EquiJoinClause
    {
        private final Symbol left;
        private final Symbol right;

        @JsonCreator
        public EquiJoinClause(@JsonProperty("left") Symbol left, @JsonProperty("right") Symbol right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }

        @JsonProperty("left")
        public Symbol getLeft()
        {
            return left;
        }

        @JsonProperty("right")
        public Symbol getRight()
        {
            return right;
        }
    }

    public boolean isDistributed()
    {
        return method == DISTRIBUTED;
    }

    public static boolean canDistributeJoin(Session session, PlanNode rightNode, List<JoinNode.EquiJoinClause> equiJoinClauses, JoinNode.Type type)
    {
        // The implementation of full outer join only works if the data is hash partitioned. See LookupJoinOperators#buildSideOuterJoinUnvisitedPositions
        boolean isCrossJoin = type == Type.INNER && equiJoinClauses.isEmpty();
        return mustDistributeJoin(type) || (SystemSessionProperties.isDistributedJoinEnabled(session) && !isScalar(rightNode) && !isCrossJoin);
    }

    public static boolean mustDistributeJoin(JoinNode.Type type)
    {
        // The implementation of full outer join only works if the data is hash partitioned. See LookupJoinOperators#buildSideOuterJoinUnvisitedPositions
        return type == Type.RIGHT || type == Type.FULL;
    }
}
