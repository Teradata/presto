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

import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class GroupingOperationAnalyzer
{
    private GroupingOperationAnalyzer()
    {
    }

    public static boolean containsGroupingOperation(Expression expression)
    {
        Extractor extractor = new Extractor();
        extractor.process(expression, null);
        return extractor.containsGroupingOperations();
    }

    public static class Extractor
            extends DefaultExpressionTraversalVisitor<Void, Void>
    {
        private final ImmutableList.Builder<GroupingOperation> groupingOperationBuilder = ImmutableList.builder();

        public Extractor()
        {
        }

        @Override
        protected Void visitGroupingOperation(GroupingOperation node, Void context)
        {
            groupingOperationBuilder.add(node);
            return null;
        }

        public boolean containsGroupingOperations()
        {
            return !groupingOperationBuilder.build().isEmpty();
        }

        public List<GroupingOperation> getGroupingOperations()
        {
            return groupingOperationBuilder.build();
        }
    }
}
