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
package com.facebook.presto.statistics;

import com.facebook.presto.spi.statistics.Estimate;

import java.util.function.Function;

import static com.facebook.presto.spi.statistics.Estimate.unknownValue;
import static java.util.Objects.requireNonNull;

public class PlanNodeStatistics
{
    public static final PlanNodeStatistics EMPTY_STATISTICS = PlanNodeStatistics.builder().build();

    private final Estimate outputRowsCount;
    private final Estimate outputSizeInBytes;

    private PlanNodeStatistics(Estimate outputRowsCount, Estimate outputSizeInBytes)
    {
        this.outputRowsCount = requireNonNull(outputRowsCount, "outputRowsCount can not be null");
        this.outputSizeInBytes = requireNonNull(outputSizeInBytes, "outputSizeInBytes can not be null");
    }

    public Estimate getOutputRowsCount()
    {
        return outputRowsCount;
    }

    public Estimate getOutputSizeInBytes()
    {
        return outputSizeInBytes;
    }

    public PlanNodeStatistics mapOutputRowsCount(Function<Double, Double> mappingFunction)
    {
        return builder().setFrom(this).setOutputRowsCount(outputRowsCount.map(mappingFunction)).build();
    }

    public PlanNodeStatistics mapOutputSizeInBytes(Function<Double, Double> mappingFunction)
    {
        return builder().setFrom(this).setOutputSizeInBytes(outputRowsCount.map(mappingFunction)).build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Estimate outputRowsCount = unknownValue();
        private Estimate outputSizeInBytes = unknownValue();

        public Builder setFrom(PlanNodeStatistics otherStatistics)
        {
            return setOutputRowsCount(otherStatistics.getOutputRowsCount())
                    .setOutputSizeInBytes(otherStatistics.getOutputSizeInBytes());
        }

        public Builder setOutputRowsCount(Estimate outputRowsCount)
        {
            this.outputRowsCount = outputRowsCount;
            return this;
        }

        public Builder setOutputSizeInBytes(Estimate outputSizeInBytes)
        {
            this.outputSizeInBytes = outputSizeInBytes;
            return this;
        }

        public PlanNodeStatistics build()
        {
            return new PlanNodeStatistics(outputRowsCount, outputSizeInBytes);
        }
    }
}
