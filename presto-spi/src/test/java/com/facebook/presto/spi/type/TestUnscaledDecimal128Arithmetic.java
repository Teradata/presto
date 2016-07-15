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

import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static com.facebook.presto.spi.type.Decimals.MAX_DECIMAL_UNSCALED_VALUE;
import static com.facebook.presto.spi.type.Decimals.MIN_DECIMAL_UNSCALED_VALUE;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.compare;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.hash;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.isNegative;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.multiply;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.negate;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToBigInteger;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToUnscaledLong;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.fail;

public class TestUnscaledDecimal128Arithmetic
{
    private static final Slice MAX_DECIMAL = unscaledDecimal(MAX_DECIMAL_UNSCALED_VALUE);
    private static final Slice MIN_DECIMAL = unscaledDecimal(MIN_DECIMAL_UNSCALED_VALUE);

    @Test
    public void testUnscaledBigIntegerToDecimal()
    {
        assertconvertsUnscaledBigIntegerToDecimal(MAX_DECIMAL_UNSCALED_VALUE);
        assertconvertsUnscaledBigIntegerToDecimal(MIN_DECIMAL_UNSCALED_VALUE);
        assertconvertsUnscaledBigIntegerToDecimal(BigInteger.ZERO);
        assertconvertsUnscaledBigIntegerToDecimal(BigInteger.ONE);
        assertconvertsUnscaledBigIntegerToDecimal(BigInteger.ONE.negate());
    }

    @Test
    public void testUnscaledBigIntegerToDecimalOverflow()
    {
        assertUnscaledBigIntegerToDecimalOverflows(MAX_DECIMAL_UNSCALED_VALUE.add(BigInteger.ONE));
        assertUnscaledBigIntegerToDecimalOverflows(MIN_DECIMAL_UNSCALED_VALUE.subtract(BigInteger.ONE));
    }

    @Test
    public void testUnscaledLongToDecimal()
    {
        assertConvertsUnscaledLongToDecimal(0);
        assertConvertsUnscaledLongToDecimal(1);
        assertConvertsUnscaledLongToDecimal(-1);
        assertConvertsUnscaledLongToDecimal(Long.MAX_VALUE);
        assertConvertsUnscaledLongToDecimal(Long.MIN_VALUE);
    }

    @Test
    public void testDecimalToUnscaledLongOverflow()
    {
        assertDecimalToUnscaledLongOverflows(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE));
        assertDecimalToUnscaledLongOverflows(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE));
        assertDecimalToUnscaledLongOverflows(MAX_DECIMAL_UNSCALED_VALUE);
        assertDecimalToUnscaledLongOverflows(MIN_DECIMAL_UNSCALED_VALUE);
    }

    @Test
    public void testMultiply()
    {
        assertEquals(multiply(unscaledDecimal(0), MAX_DECIMAL), unscaledDecimal(0));
        assertEquals(multiply(unscaledDecimal(1), MAX_DECIMAL), MAX_DECIMAL);
        assertEquals(multiply(unscaledDecimal(1), MIN_DECIMAL), MIN_DECIMAL);
        assertEquals(multiply(unscaledDecimal(-1), MAX_DECIMAL), MIN_DECIMAL);
        assertEquals(multiply(unscaledDecimal(-1), MIN_DECIMAL), MAX_DECIMAL);

        assertEquals(multiply(unscaledDecimal(Integer.MAX_VALUE), unscaledDecimal(Integer.MIN_VALUE)), unscaledDecimal((long) Integer.MAX_VALUE * Integer.MIN_VALUE));
        assertEquals(multiply(unscaledDecimal("99999999999999"), unscaledDecimal("-1000000000000000000000000")), unscaledDecimal("-99999999999999000000000000000000000000"));
    }

    @Test
    public void testMultiplyOverflow()
    {
        assertMultiplyOverflows(unscaledDecimal("99999999999999"), unscaledDecimal("-10000000000000000000000000"));
        assertMultiplyOverflows(MAX_DECIMAL, unscaledDecimal("10"));
    }

    @Test
    public void testCompare()
    {
        assertCompare(unscaledDecimal(0), unscaledDecimal(0), 0);
        assertCompare(negateReturnByValue(unscaledDecimal(0)), unscaledDecimal(0), 0);

        assertCompare(unscaledDecimal(0), unscaledDecimal(10), -1);
        assertCompare(unscaledDecimal(10), unscaledDecimal(0), 1);
        assertCompare(negateReturnByValue(unscaledDecimal(0)), unscaledDecimal(10), -1);
        assertCompare(unscaledDecimal(10), negateReturnByValue(unscaledDecimal(0)), 1);

        assertCompare(negateReturnByValue(unscaledDecimal(0)), MAX_DECIMAL, -1);
        assertCompare(MAX_DECIMAL, negateReturnByValue(unscaledDecimal(0)), 1);

        assertCompare(unscaledDecimal(-10), unscaledDecimal(-11), 1);
        assertCompare(unscaledDecimal(-11), unscaledDecimal(-11), 0);
        assertCompare(unscaledDecimal(-12), unscaledDecimal(-11), -1);

        assertCompare(unscaledDecimal(10), unscaledDecimal(11), -1);
        assertCompare(unscaledDecimal(11), unscaledDecimal(11), 0);
        assertCompare(unscaledDecimal(12), unscaledDecimal(11), 1);
    }

    @Test
    public void testNegate()
    {
        assertEquals(negateReturnByValue(negateReturnByValue(MIN_DECIMAL)), MIN_DECIMAL);
        assertEquals(negateReturnByValue(MIN_DECIMAL), MAX_DECIMAL);
        assertEquals(negateReturnByValue(MIN_DECIMAL), MAX_DECIMAL);

        assertEquals(negateReturnByValue(unscaledDecimal(1)), unscaledDecimal(-1));
        assertEquals(negateReturnByValue(unscaledDecimal(-1)), unscaledDecimal(1));
        assertEquals(negateReturnByValue(negateReturnByValue(unscaledDecimal(0))), unscaledDecimal(0));
    }

    @Test
    public void testIsNegative()
    {
        assertEquals(isNegative(MIN_DECIMAL), true);
        assertEquals(isNegative(MAX_DECIMAL), false);
        assertEquals(isNegative(unscaledDecimal(0)), false);
    }

    @Test
    public void testHash()
    {
        assertEquals(hash(unscaledDecimal(0)), hash(negateReturnByValue(unscaledDecimal(0))));
        assertNotEquals(hash(unscaledDecimal(0)), unscaledDecimal(1));
    }

    private static void assertUnscaledBigIntegerToDecimalOverflows(BigInteger value)
    {
        try {
            unscaledDecimal(value);
            fail();
        }
        catch (ArithmeticException ignored) {
        }
    }

    private static void assertDecimalToUnscaledLongOverflows(BigInteger value)
    {
        Slice decimal = unscaledDecimal(value);
        try {
            unscaledDecimalToUnscaledLong(decimal);
            fail();
        }
        catch (ArithmeticException ignored) {
        }
    }

    private static void assertMultiplyOverflows(Slice left, Slice right)
    {
        try {
            multiply(left, right);
            fail();
        }
        catch (ArithmeticException ignored) {
        }
    }

    private static void assertCompare(Slice left, Slice right, int expectedResult)
    {
        assertEquals(compare(left, right), expectedResult);
        assertEquals(compare(left.getLong(0), left.getLong(SIZE_OF_LONG), right.getLong(0), right.getLong(SIZE_OF_LONG)), expectedResult);
    }

    private static void assertconvertsUnscaledBigIntegerToDecimal(BigInteger value)
    {
        assertEquals(unscaledDecimalToBigInteger(unscaledDecimal(value)), value);
    }

    private static void assertConvertsUnscaledLongToDecimal(long value)
    {
        assertEquals(unscaledDecimalToUnscaledLong(unscaledDecimal(value)), value);
        assertEquals(unscaledDecimal(value), unscaledDecimal(BigInteger.valueOf(value)));
    }

    private static Slice negateReturnByValue(Slice slice)
    {
        Slice copy = unscaledDecimal(slice);
        negate(copy);
        return copy;
    }
}
