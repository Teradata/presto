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

package com.facebook.presto.spi.type;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

public abstract class DecimalType
        extends AbstractFixedWidthType
{
    protected static final int UNSET = -1;
    public static final int MAX_PRECISION = 38;
    public static final int MAX_SHORT_PRECISION = 18;

    public static DecimalType createDecimalType(int precision, int scale)
    {
        if (precision <= MAX_SHORT_PRECISION) {
            return new ShortDecimalType(precision, scale);
        }
        else {
            return new LongDecimalType(precision, scale);
        }
    }

    public static DecimalType createDecimalType(int precision)
    {
        return createDecimalType(precision, 0);
    }

    public static DecimalType createUnparametrizedDecimal()
    {
        return new UnparametrizedDecimalType();
    }

    protected final int precision;
    protected final int scale;

    protected DecimalType(int precision, int scale, Class<?> javaType, int fixedSize)
    {
        super(new TypeSignature(StandardTypes.DECIMAL, emptyList(), buildPrecisionScaleList(precision, scale)), javaType, fixedSize);
        this.precision = precision;
        this.scale = scale;
    }

    protected DecimalType()
    {
        super(new TypeSignature(StandardTypes.DECIMAL, emptyList(), emptyList()), long.class, 0);
        this.precision = UNSET;
        this.scale = UNSET;
    }

    public static Object unscaledValueToObject(String unscaledValue, int precision)
    {
        Object value;
        if (precision <= MAX_SHORT_PRECISION) {
            value = Long.parseLong(unscaledValue);
        }
        else {
            value = LongDecimalType.unscaledValueToSlice(unscaledValue);
        }
        return value;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    protected void validatePrecisionScale(int precision, int scale, int maxPrecision)
    {
        if (precision < 0 || precision > maxPrecision) {
            throw new IllegalArgumentException("Invalid DECIMAL precision " + precision);
        }

        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException("Invalid DECIMAL scale " + scale);
        }
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }

    private static List<Object> buildPrecisionScaleList(int precision, int scale)
    {
        List<Object> literalArguments = new ArrayList<>();
        literalArguments.add((long) precision);
        literalArguments.add((long) scale);
        return unmodifiableList(literalArguments);
    }
}
