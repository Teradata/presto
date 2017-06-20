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
package com.facebook.presto.cost;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;

public class StatisticRange
{
    private final double low;
    private final double high;
    private final double distinctValues;

    public StatisticRange(double low, double high, double distinctValues)
    {
        checkState(low <= high || (isNaN(low) && isNaN(high)));
        this.low = low;
        this.high = high;
        this.distinctValues = distinctValues;
    }

    public static StatisticRange empty()
    {
        return new StatisticRange(NaN, NaN, 0);
    }

    public double getLow()
    {
        return low;
    }

    public double getHigh()
    {
        return high;
    }

    public double getDistinctValuesCount()
    {
        return distinctValues;
    }

    public double length()
    {
        return high - low;
    }

    public double overlapPercentWith(StatisticRange other)
    {
        if (length() > 0) {
            return intersect(other).length() / length();
        }
        if (length() == 0) {
            return 1 / distinctValues;
        }
        return NaN;
    }

    public StatisticRange intersect(StatisticRange other)
    {
        double newLow = Math.max(low, other.low);
        double newHigh = Math.min(high, other.high);
        if (newLow < newHigh) {
            return new StatisticRange(newLow, newHigh, (newHigh - newLow) / length());
        }
        return empty();
    }

    public StatisticRange union(StatisticRange other)
    {
        return new StatisticRange(Math.min(low, other.low),
                Math.max(high, other.high),
                (distinctValues * (1 - overlapPercentWith(other))) + other.distinctValues);
    }
}
