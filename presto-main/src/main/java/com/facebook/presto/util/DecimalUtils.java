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
package com.facebook.presto.util;

import com.facebook.presto.spi.type.DecimalType;

import java.math.BigInteger;

import static java.lang.Math.abs;
import static java.lang.Math.pow;
import static java.lang.Math.round;
import static java.math.BigInteger.TEN;

public final class DecimalUtils
{
    public static final long[] TEN_TO_NTH_SHORT = new long[DecimalType.MAX_SHORT_PRECISION + 1];

    static {
        for (int i = 0; i < TEN_TO_NTH_SHORT.length; ++i) {
            TEN_TO_NTH_SHORT[i] = round(pow(10, i));
        }
    }

    public static final BigInteger[] TEN_TO_NTH_LONG = new BigInteger[DecimalType.MAX_PRECISION + 1];

    static {
        for (int i = 0; i < TEN_TO_NTH_LONG.length; ++i) {
            TEN_TO_NTH_LONG[i] = TEN.pow(i);
        }
    }

    private DecimalUtils() {}

    public static boolean doesFitPrecision(long value, int precision)
    {
        return abs(value) < TEN_TO_NTH_SHORT[precision];
    }

    public static boolean doesFitPrecision(BigInteger value, int precision)
    {
        return value.abs().compareTo(TEN_TO_NTH_LONG[precision]) < 0;
    }
}
