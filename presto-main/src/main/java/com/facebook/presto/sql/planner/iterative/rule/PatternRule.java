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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.v2.Captures;
import com.facebook.presto.matching.v2.Match;
import com.facebook.presto.matching.v2.Pattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Optional;

import static com.facebook.presto.matching.Pattern.v2Adapter;

public interface PatternRule<T>
        extends Rule
{
    Pattern<T> pattern();

    //TODO: remove the Rule#getPattern method once the migration is complete,
    @Override
    default com.facebook.presto.matching.Pattern getPattern()
    {
        return v2Adapter(pattern());
    }

    default Optional<PlanNode> apply(PlanNode node, Context context)
    {
        //TODO could be stored in Context, or we could move the pattern matcher usage to IterativeOptimizer.
        //creating the matcher here is temporary for the migration to new pattern matcher
        //and will disappear later in the PR
        PrestoMatcher prestoMatcher = new PrestoMatcher(context.getLookup());
        Match<T> match = prestoMatcher.match(pattern(), node);
        return match
                .map(value -> apply(value, match.captures(), context))
                .orElse(Optional.empty());
    }

    //TODO depending on how flexible pattern matching is we may drop the 'optional' here
    Optional<PlanNode> apply(T node, Captures captures, Context context);
}
