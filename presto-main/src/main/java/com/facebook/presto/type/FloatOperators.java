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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.MathFunctions;
import com.facebook.presto.operator.scalar.annotations.ScalarOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;

import static com.facebook.presto.metadata.OperatorType.ADD;
import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.OperatorType.DIVIDE;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.HASH_CODE;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.MODULUS;
import static com.facebook.presto.metadata.OperatorType.MULTIPLY;
import static com.facebook.presto.metadata.OperatorType.NEGATION;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.OperatorType.SATURATED_FLOOR_CAST;
import static com.facebook.presto.metadata.OperatorType.SUBTRACT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.valueOf;
import static java.math.RoundingMode.FLOOR;

public final class FloatOperators
{
    private static final float MIN_LONG_AS_FLOAT = -0x1p63f;
    private static final float MAX_LONG_PLUS_ONE_AS_FLOAT = 0x1p63f;
    private static final float MIN_INTEGER_AS_FLOAT = -0x1p31f;
    private static final float MAX_INTEGER_PLUS_ONE_AS_FLOAT = 0x1p31f;
    private static final float MIN_SHORT_AS_FLOAT = -0x1p15f;
    private static final float MAX_SHORT_PLUS_ONE_AS_FLOAT = 0x1p15f;
    private static final float MIN_BYTE_AS_FLOAT = -0x1p7f;
    private static final float MAX_BYTE_PLUS_ONE_AS_FLOAT = 0x1p7f;

    private FloatOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.FLOAT)
    public static long add(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) + intBitsToFloat((int) right));
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.FLOAT)
    public static long subtract(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) - intBitsToFloat((int) right));
    }

    @ScalarOperator(MULTIPLY)
    @SqlType(StandardTypes.FLOAT)
    public static long multiply(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) * intBitsToFloat((int) right));
    }

    @ScalarOperator(DIVIDE)
    @SqlType(StandardTypes.FLOAT)
    public static long divide(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) / intBitsToFloat((int) right));
    }

    @ScalarOperator(MODULUS)
    @SqlType(StandardTypes.FLOAT)
    public static long modulus(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return floatToRawIntBits(intBitsToFloat((int) left) % intBitsToFloat((int) right));
    }

    @ScalarOperator(NEGATION)
    @SqlType(StandardTypes.FLOAT)
    public static long negate(@SqlType(StandardTypes.FLOAT) long value)
    {
        return floatToRawIntBits(-intBitsToFloat((int) value));
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean equal(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return intBitsToFloat((int) left) == intBitsToFloat((int) right);
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean notEqual(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return intBitsToFloat((int) left) != intBitsToFloat((int) right);
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return intBitsToFloat((int) left) < intBitsToFloat((int) right);
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return intBitsToFloat((int) left) <= intBitsToFloat((int) right);
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return intBitsToFloat((int) left) > intBitsToFloat((int) right);
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.FLOAT) long left, @SqlType(StandardTypes.FLOAT) long right)
    {
        return intBitsToFloat((int) left) >= intBitsToFloat((int) right);
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.FLOAT) long value, @SqlType(StandardTypes.FLOAT) long min, @SqlType(StandardTypes.FLOAT) long max)
    {
        return intBitsToFloat((int) min) <= intBitsToFloat((int) value) &&
                intBitsToFloat((int) value) <= intBitsToFloat((int) max);
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.FLOAT) long value)
    {
        return value;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.VARCHAR)
    public static Slice castToVarchar(@SqlType(StandardTypes.FLOAT) long value)
    {
        return utf8Slice(valueOf(intBitsToFloat((int) value)));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToLong(@SqlType(StandardTypes.FLOAT) long value)
    {
        return (long) MathFunctions.round((double) intBitsToFloat((int) value));
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(StandardTypes.FLOAT) long value)
    {
        try {
            return Ints.checkedCast((long) MathFunctions.round((double) intBitsToFloat((int) value)));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType(StandardTypes.FLOAT) long value)
    {
        try {
            return Shorts.checkedCast((long) MathFunctions.round((double) intBitsToFloat((int) value)));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType(StandardTypes.FLOAT) long value)
    {
        try {
            return SignedBytes.checkedCast((long) MathFunctions.round((double) intBitsToFloat((int) value)));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, e);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType(StandardTypes.FLOAT) long value)
    {
        return (double) intBitsToFloat((int) value);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean castToBoolean(@SqlType(StandardTypes.FLOAT) long value)
    {
        return intBitsToFloat((int) value) != 0.0f;
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long saturatedFloorCastToBigint(@SqlType(StandardTypes.FLOAT) long value)
    {
        return saturatedFloorCastToLong(value, Long.MIN_VALUE, MIN_LONG_AS_FLOAT, Long.MAX_VALUE, MAX_LONG_PLUS_ONE_AS_FLOAT);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long saturatedFloorCastToInteger(@SqlType(StandardTypes.FLOAT) long value)
    {
        return saturatedFloorCastToLong(value, Integer.MIN_VALUE, MIN_INTEGER_AS_FLOAT, Integer.MAX_VALUE, MAX_INTEGER_PLUS_ONE_AS_FLOAT);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long saturatedFloorCastToSmallint(@SqlType(StandardTypes.FLOAT) long value)
    {
        return saturatedFloorCastToLong(value, Short.MIN_VALUE, MIN_SHORT_AS_FLOAT, Short.MAX_VALUE, MAX_SHORT_PLUS_ONE_AS_FLOAT);
    }

    @ScalarOperator(SATURATED_FLOOR_CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long saturatedFloorCastToTinyint(@SqlType(StandardTypes.FLOAT) long value)
    {
        return saturatedFloorCastToLong(value, Byte.MIN_VALUE, MIN_BYTE_AS_FLOAT, Byte.MAX_VALUE, MAX_BYTE_PLUS_ONE_AS_FLOAT);
    }

    private static long saturatedFloorCastToLong(long valueBits, long minValue, float minValueAsDouble, long maxValue, float maxValuePlusOneAsDouble)
    {
        float value = intBitsToFloat((int) valueBits);
        if (value <= minValueAsDouble) {
            return minValue;
        }
        if (value + 1 >= maxValuePlusOneAsDouble) {
            return maxValue;
        }
        return DoubleMath.roundToLong(value, FLOOR);
    }
}
