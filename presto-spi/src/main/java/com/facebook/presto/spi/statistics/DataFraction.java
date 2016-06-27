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

public final class DataFraction
{
    private static final double FULLY_RELIABLE = Double.POSITIVE_INFINITY;

    private final double dataFraction;

    /**
     * Data fraction used for calculating statistics in [0,1] range.
     */
    public static DataFraction dataFraction(double dataFraction)
    {
        if (dataFraction < 0.0 || dataFraction > 1.0) {
            throw new IllegalArgumentException("reliability level not in [0.0, 1.0] range");
        }
        return new DataFraction(dataFraction);
    }

    /**
     * Enforces that statistics is fully in sync with actual data in the table
     */
    public static DataFraction fullyReliable()
    {
        return new DataFraction(FULLY_RELIABLE);
    }

    private DataFraction(Double dataFraction)
    {
        this.dataFraction = dataFraction;
    }

    public boolean isFullyReliable()
    {
        return dataFraction == FULLY_RELIABLE;
    }

    public double getDataFraction()
    {
        return Math.min(dataFraction, 1.0);
    }
}
