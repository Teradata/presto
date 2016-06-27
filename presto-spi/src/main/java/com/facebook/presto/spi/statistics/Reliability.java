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

public class Reliability
{
    private static final double FULLY_RELIABLE = Double.POSITIVE_INFINITY;
    private static final double MIN_RELIABILITY = 0.0;
    private static final double MAX_RELIABILITY = 1.0;

    private final double reliabilityLevel;

    /**
     * Statistics reliability in [0,1] range.
     */
    public static Reliability reliabilityLevel(double reliabilityLevel)
    {
        if (reliabilityLevel < MIN_RELIABILITY || reliabilityLevel > MAX_RELIABILITY) {
            throw new IllegalArgumentException("reliability level not in [0.0, 1.0] range");
        }
        return new Reliability(reliabilityLevel);
    }

    /**
     * Enforces that statistics is fully in sync with actual data in the table
     */
    public static Reliability fullyReliable()
    {
        return new Reliability(FULLY_RELIABLE);
    }

    private Reliability(Double reliabilityLevel)
    {
        this.reliabilityLevel = reliabilityLevel;
    }

    public boolean isFullyReliable()
    {
        return reliabilityLevel == FULLY_RELIABLE;
    }

    public double getReliabilityLevel()
    {
        return Math.min(reliabilityLevel, MAX_RELIABILITY);
    }
}
