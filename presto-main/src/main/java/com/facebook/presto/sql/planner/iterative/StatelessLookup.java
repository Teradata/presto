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

package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.StatsCalculator;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

// TODO: remove.
@ThreadSafe
public class StatelessLookup
{
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;

    @Inject
    public StatelessLookup(StatsCalculator statsCalculator, CostCalculator costCalculator)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
    }

    public Lookup createCachingNonResolvingLookup()
    {
        return new NonResolvingCachingLookup(statsCalculator, costCalculator);
    }
}
