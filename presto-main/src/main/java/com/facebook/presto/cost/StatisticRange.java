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
import static java.lang.Math.max;
import static java.lang.Math.min;

public class StatisticRange
{
    private final double low;
    private final double high;
    private final double distinctValues;

    public StatisticRange(double low, double high, double distinctValues)
    {
        checkState(low <= high || (isNaN(low) && isNaN(high)), "Low must be smaller or equal to high or range must be empty (NaN, NaN)");
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
        double lengthOfIntersect = min(high, other.high) - max(low, other.low);
        if (lengthOfIntersect > 0) {
            return lengthOfIntersect / length();
        }
        if (lengthOfIntersect == 0) {
            return 1 / distinctValues;
        }
        if (lengthOfIntersect < 0) {
            return 0;
        }
        return NaN;
    }

    private double overlappingDistinctValues(StatisticRange other) {
        double overlapPercentOfLeft = overlapPercentWith(other);
        double overlapPercentOfRight = other.overlapPercentWith(this);
        double overlapDistinctValuesLeft = overlapPercentOfLeft * distinctValues;
        double overlapDistinctValuesRight = overlapPercentOfRight * other.distinctValues;
        return min(overlapDistinctValuesLeft, overlapDistinctValuesRight);
    }

    public StatisticRange intersect(StatisticRange other)
    {
        double newLow = max(low, other.low);
        double newHigh = min(high, other.high);
        if (newLow < newHigh) {
            return new StatisticRange(newLow, newHigh, overlappingDistinctValues(other));
        }
        return empty();
    }

    public StatisticRange union(StatisticRange other)
    {
        double overlapPercentOfLeft = overlapPercentWith(other);
        double overlapPercentOfRight = other.overlapPercentWith(this);
        double overlapDistinctValuesLeft = overlapPercentOfLeft * distinctValues;
        double overlapDistinctValuesRight = overlapPercentOfRight * other.distinctValues;
        double overlapDistinctValuesOptimistic = min(overlapDistinctValuesLeft, overlapDistinctValuesRight);
        double newDistinctValues = distinctValues + other.distinctValues - overlapDistinctValuesOptimistic;

        return new StatisticRange(min(low, other.low), max(high, other.high), newDistinctValues);
    }

    public StatisticRange subtract(StatisticRange rightRange)
    {
        return null;
    }
}
