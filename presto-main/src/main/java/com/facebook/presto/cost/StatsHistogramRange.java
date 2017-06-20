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

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.DoubleStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;

public class StatsHistogramRange
{
    private final double low;
    private final double high;
    private Optional<SimplifiedHistogramStats> statsParent;

    public StatsHistogramRange(double low, double high, Optional<SimplifiedHistogramStats> statsParent)
    {
        checkArgument(!isNaN(low), "NaN is not valid low");
        checkArgument(!isNaN(high), "NaN is not valid low");
        checkArgument(low <= high, "low must be less than or equal high");
        this.low = low;
        this.high = high;
        this.statsParent = statsParent;
    }

    public void setStatsParent(Optional<SimplifiedHistogramStats> statsParent)
    {
        this.statsParent = statsParent;
    }

    public double getHigh()
    {
        return high;
    }

    public double getLow()
    {
        return low;
    }

    public OptionalDouble minKnown(double first, double second)
    {
        return DoubleStream.of(first, second).filter(Double::isFinite).min();
    }

    public OptionalDouble maxKnown(double first, double second)
    {
        return DoubleStream.of(first, second).filter(Double::isFinite).max();
    }

    public double overlapPartOf(StatsHistogramRange other)
    {
        if (low != NEGATIVE_INFINITY && high != POSITIVE_INFINITY) {
            return overlapPartOfClosedRange(other);
        }
        if (low == NEGATIVE_INFINITY && high != POSITIVE_INFINITY) {
            return overlapPartOfRightClosedRange(other);
        }
        if (low != NEGATIVE_INFINITY && high == POSITIVE_INFINITY) {
            return overlapPartOfLeftClosedRange(other);
        }
        return overlapPartOfUnknownRange(other);
    }

    private double overlapPartOfRightClosedRange(StatsHistogramRange other)
    {
        if (isFinite(other.low) || isFinite(other.high)) {
            double lowestKnown = minKnown(other.high, other.low).getAsDouble();
            if (lowestKnown < high) {
                return 0.5; // Heuristic? FIXME multiply by nulls factor
            }
            return 0;
        }
        return 1; // FIXME multiply by nulls factor
    }

    private double overlapPartOfLeftClosedRange(StatsHistogramRange other)
    {
        // TODO check arguments
        if (isFinite(other.low) || isFinite(other.high)) {
            double highestKnown = maxKnown(other.high, other.low).getAsDouble();
            if (highestKnown > low) {
                return 0.5; // Heuristic? FIXME multiply by nulls factor
            }
            return 0;
        }
        return 1; // FIXME multiply by nulls factor
    }

    private double overlapPartOfUnknownRange(StatsHistogramRange other)
    {
        // TODO check arguments
        if (isFinite(other.low) && isFinite(other.high)) {
            return 0;
        }
        if (isFinite(other.low) || isFinite(other.high)) {
            return 0.5; // Heuristic? TODO multiply by  nullsfact
        }
        return 1.0; // TODO multiply by  nullsfact
    }

    private double overlapPartOfClosedRange(StatsHistogramRange other)
    {
        // TODO check argumentss

        double leftValue = maxKnown(low, other.low).getAsDouble();
        double rightValue = minKnown(high, other.high).getAsDouble();

        if (leftValue > rightValue) {
            return 0;
        }
        if (leftValue == rightValue) {
            return (1 - statsParent.get().getNullsFraction()) / statsParent.get().getDistinctValuesCount();
        }
        double histogramOverlap = (rightValue - leftValue) / (high - low);
        return histogramOverlap * (1 - orDefault(statsParent.get().getNullsFraction(), 0));
    }

    double orDefault(double value, double defaultValue)
    {
        if (isNaN(value)) {
            return defaultValue;
        }
        return value;
    }

    public StatsHistogramRange intersect(StatsHistogramRange other)
    {
        return new StatsHistogramRange(Double.max(low, other.low), Double.min(high, other.high), Optional.empty());
    }
}
