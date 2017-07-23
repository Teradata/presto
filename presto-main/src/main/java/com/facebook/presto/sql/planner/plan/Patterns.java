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

import com.facebook.presto.matching.v2.Pattern;
import com.facebook.presto.matching.v2.Property;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.matching.v2.Pattern.typeOf;
import static com.facebook.presto.matching.v2.Property.optionalProperty;
import static com.facebook.presto.matching.v2.Property.property;
import static java.util.Optional.empty;

//FIXME add patterns and properties for all current plan nodes
public class Patterns
{
    private Patterns() {}

    public static Pattern<DeleteNode> delete()
    {
        return typeOf(DeleteNode.class);
    }

    public static Pattern<ExchangeNode> exchange()
    {
        return typeOf(ExchangeNode.class);
    }

    public static Pattern<FilterNode> filter()
    {
        return typeOf(FilterNode.class);
    }

    public static Pattern<IndexSourceNode> indexSource()
    {
        return typeOf(IndexSourceNode.class);
    }

    public static Pattern<JoinNode> join()
    {
        return typeOf(JoinNode.class);
    }

    public static Pattern<MarkDistinctNode> markDistinct()
    {
        return typeOf(MarkDistinctNode.class);
    }

    public static Pattern<ProjectNode> project()
    {
        return typeOf(ProjectNode.class);
    }

    public static Pattern<SemiJoinNode> semiJoin()
    {
        return typeOf(SemiJoinNode.class);
    }

    public static Pattern<TableFinishNode> tableFinish()
    {
        return typeOf(TableFinishNode.class);
    }

    public static Pattern<TableScanNode> tableScan()
    {
        return typeOf(TableScanNode.class);
    }

    public static Pattern<ValuesNode> values()
    {
        return typeOf(ValuesNode.class);
    }

    public static Property<SingleSourcePlanNode, PlanNode> source()
    {
        return property(SingleSourcePlanNode::getSource);
    }

    public static Property<PlanNode, List<PlanNode>> sources()
    {
        return property(PlanNode::getSources);
    }

    public static Property<PlanNode, PlanNode> onlySource()
    {
        return optionalProperty(node -> node.getSources().size() == 1 ?
                Optional.of(node.getSources().get(0)) :
                empty());
    }
}
