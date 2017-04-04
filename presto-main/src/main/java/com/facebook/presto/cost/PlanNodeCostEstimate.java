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

import com.facebook.presto.spi.statistics.Estimate;

import java.util.Objects;

import static com.facebook.presto.spi.statistics.Estimate.unknownValue;
import static com.facebook.presto.spi.statistics.Estimate.zeroValue;
import static java.util.Objects.requireNonNull;

public class PlanNodeCostEstimate
{
    public static final PlanNodeCostEstimate UNKNOWN_COST = new PlanNodeCostEstimate(unknownValue(), unknownValue(), unknownValue());
    public static final PlanNodeCostEstimate ZERO_COST = PlanNodeCostEstimate.builder().build();

    private final Estimate cpuCost;
    private final Estimate memoryCost;
    private final Estimate networkCost;

    private PlanNodeCostEstimate(Estimate cpuCost, Estimate memoryCost, Estimate networkCost)
    {
        this.cpuCost = requireNonNull(cpuCost, "cpuCost can not be null");
        this.memoryCost = requireNonNull(memoryCost, "memoryCost can not be null");
        this.networkCost = requireNonNull(networkCost, "networkCost can not be null");
    }

    public Estimate getCpuCost()
    {
        return cpuCost;
    }

    public Estimate getMemoryCost()
    {
        return memoryCost;
    }

    public Estimate getNetworkCost()
    {
        return networkCost;
    }

    @Override
    public String toString()
    {
        return "PlanNodeCostEstimate{" +
                "cpuCost=" + cpuCost +
                ", memoryCost=" + memoryCost +
                ", networkCost=" + networkCost + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlanNodeCostEstimate that = (PlanNodeCostEstimate) o;
        return Objects.equals(cpuCost, that.cpuCost) &&
                Objects.equals(memoryCost, that.memoryCost) &&
                Objects.equals(networkCost, that.networkCost);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cpuCost, memoryCost, networkCost);
    }

    public PlanNodeCostEstimate add(PlanNodeCostEstimate other)
    {
        return new PlanNodeCostEstimate(
                getCpuCost().add(other.getCpuCost()), getMemoryCost().add(other.getMemoryCost()), getNetworkCost().add(other.getNetworkCost())
        );
    }

    public static PlanNodeCostEstimate networkCost(Estimate networkCost)
    {
        return builder().setNetworkCost(networkCost).build();
    }

    public static PlanNodeCostEstimate cpuCost(Estimate cpuCost)
    {
        return builder().setCpuCost(cpuCost).build();
    }

    public static PlanNodeCostEstimate memoryCost(Estimate memoryCost)
    {
        return builder().setCpuCost(memoryCost).build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Estimate cpuCost = zeroValue();
        private Estimate memoryCost = zeroValue();
        private Estimate networkCost = zeroValue();

        public Builder setFrom(PlanNodeCostEstimate otherStatistics)
        {
            return setCpuCost(otherStatistics.getCpuCost())
                    .setMemoryCost(otherStatistics.getMemoryCost())
                    .setNetworkCost(otherStatistics.getNetworkCost());
        }

        public Builder setCpuCost(Estimate cpuCost)
        {
            this.cpuCost = cpuCost;
            return this;
        }

        public Builder setMemoryCost(Estimate memoryCost)
        {
            this.memoryCost = memoryCost;
            return this;
        }

        public Builder setNetworkCost(Estimate networkCost)
        {
            this.networkCost = networkCost;
            return this;
        }

        public PlanNodeCostEstimate build()
        {
            return new PlanNodeCostEstimate(cpuCost, memoryCost, networkCost);
        }
    }
}
