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

package com.facebook.presto.spi.statistics;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class Reliability
{
    private final boolean fullyReliable;

    private final Optional<Double> reliabilityLevel;

    /**
     * Statistics reliability in [0,1] range.
     */
    public static Reliability reliabilityLevel(Double reliabilityLevel)
    {
        if (reliabilityLevel < 0.0 || reliabilityLevel > 1.0) {
            throw new IllegalArgumentException("reliability level not in [0.0, 1.0] range");
        }
        return new Reliability(false, Optional.of(reliabilityLevel));
    }

    /**
     * Enforces that statistics is fully in sync with actual data in the table
     */
    public static Reliability fullyReliable()
    {
        return new Reliability(true, Optional.empty());
    }

    private Reliability(boolean fullyReliable, Optional<Double> reliabilityLevel)
    {
        reliabilityLevel = requireNonNull(reliabilityLevel, "reliabilityLevel can not be null");
        if (!fullyReliable ^ reliabilityLevel.isPresent()) {
            throw new IllegalArgumentException("cannot set both fullyReliable to true and specify reliabilityLevel");
        }
        this.fullyReliable = fullyReliable;
        this.reliabilityLevel = reliabilityLevel;
    }

    public boolean isFullyReliable()
    {
        return fullyReliable;
    }

    public Double getReliabilityLevel()
    {
        if (!reliabilityLevel.isPresent()) {
            throw new IllegalStateException("reliabilityLevel not set");
        }
        return reliabilityLevel.get();
    }
}
