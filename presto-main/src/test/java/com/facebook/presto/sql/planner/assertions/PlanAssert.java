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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.Plan;

import static com.facebook.presto.sql.planner.PlanPrinter.textLogicalPlan;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.fail;

public final class PlanAssert
{
    private PlanAssert() {}

    public static void assertPlanMatches(Session session, Metadata metadata, Plan actual, PlanMatchPattern pattern)
    {
        assertPlan(session, metadata, actual, pattern, true);
    }

    public static void assertPlanDoesNotMatch(Session session, Metadata metadata, Plan actual, PlanMatchPattern pattern)
    {
        assertPlan(session, metadata, actual, pattern, false);
    }

    private static void assertPlan(Session session, Metadata metadata, Plan actual, PlanMatchPattern pattern, boolean expectedMatch)
    {
        requireNonNull(actual, "root is null");

        boolean actualMatch = actual.getRoot().accept(new PlanMatchingVisitor(session, metadata), new PlanMatchingContext(pattern));
        if (expectedMatch != actualMatch) {
            String logicalPlan = textLogicalPlan(actual.getRoot(), actual.getTypes(), metadata, session);
            String errorMessage;
            if (expectedMatch) {
                errorMessage = format("Plan does not match:\n%s\nto pattern:\n%s", logicalPlan, pattern);
            }
            else {
                errorMessage = format("Plan matches:\n%s\nto pattern:\n%s", logicalPlan, pattern);
            }
            fail(errorMessage);
        }
    }
}
