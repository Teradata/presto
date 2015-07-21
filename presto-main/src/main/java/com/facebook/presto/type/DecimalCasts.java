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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.LongDecimalType;
import com.facebook.presto.spi.type.ShortDecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.pow;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.math.BigDecimal.ROUND_HALF_UP;
import static java.math.BigInteger.TEN;
import static java.math.BigInteger.ZERO;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class DecimalCasts
{
    public static final DecimalToBooleanCast DECIMAL_TO_BOOLEAN_CAST = new DecimalToBooleanCast();
    public static final BooleanToDecimalCast BOOLEAN_TO_DECIMAL_CAST = new BooleanToDecimalCast();
    public static final DecimalToBigintCast DECIMAL_TO_BIGINT_CAST = new DecimalToBigintCast();
    public static final BigintToDecimalCast BIGINT_TO_DECIMAL_CAST = new BigintToDecimalCast();
    public static final DoubleToDecimalCast DOUBLE_TO_DECIMAL_CAST = new DoubleToDecimalCast();
    public static final DecimalToDoubleCast DECIMAL_TO_DOUBLE_CAST = new DecimalToDoubleCast();
    public static final VarcharToDecimalCast VARCHAR_TO_DECIMAL_CAST = new VarcharToDecimalCast();
    public static final DecimalToVarcharCast DECIMAL_TO_VARCHAR_CAST = new DecimalToVarcharCast();

    private static final long[] TEN_TO_NTH_SHORT = new long[DecimalType.MAX_SHORT_PRECISION + 1];

    static {
        for (int i = 0; i < TEN_TO_NTH_SHORT.length; ++i) {
            TEN_TO_NTH_SHORT[i] = round(pow(10, i));
        }
    }

    private static final BigInteger[] TEN_TO_NTH_LONG = new BigInteger[DecimalType.MAX_PRECISION + 1];

    static {
        for (int i = 0; i < TEN_TO_NTH_LONG.length; ++i) {
            TEN_TO_NTH_LONG[i] = TEN.pow(i);
        }
    }

    private DecimalCasts() {}

    public static class BooleanToDecimalCast
            extends BaseToDecimalCast
    {
        public BooleanToDecimalCast()
        {
            super(BOOLEAN);
        }

        public static long castToShortDecimal(boolean booleanValue, long precision, long scale, long tenToScale)
        {
            return booleanValue ? tenToScale : 0;
        }

        public static Slice castToLongDecimal(boolean booleanValue, long precision, long scale, long tenToScale)
        {
            return LongDecimalType.unscaledValueToSlice(BigInteger.valueOf(booleanValue ? tenToScale : 0L));
        }
    }

    public static class DecimalToBooleanCast
            extends BaseFromDecimalCast
    {
        public DecimalToBooleanCast()
        {
            super(BOOLEAN);
        }

        public static boolean castFromShortDecimal(long decimal, long precision, long scale, long tenToScale)
        {
            return decimal != 0;
        }

        public static boolean castFromLongDecimal(Slice decimal, long precision, long scale, long tenToScale)
        {
            BigInteger decimalBigInteger = LongDecimalType.unscaledValueToBigInteger(decimal);
            return !decimalBigInteger.equals(ZERO);
        }
    }

    public static class BigintToDecimalCast
            extends BaseToDecimalCast
    {
        public BigintToDecimalCast()
        {
            super(BIGINT);
        }

        public static long castToShortDecimal(long bigint, long precision, long scale, long tenToScale)
        {
            try {
                long decimal = multiplyExact(bigint, tenToScale);
                if (Math.abs(decimal) >= TEN_TO_NTH_SHORT[(int) precision]) {
                    throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", bigint, precision, scale));
                }
                return decimal;
            }
            catch (ArithmeticException e) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", bigint, precision, scale));
            }
        }

        public static Slice castToLongDecimal(long bigint, long precision, long scale, long tenToScale)
        {
            BigInteger decimalBigInteger = BigInteger.valueOf(bigint);
            decimalBigInteger = decimalBigInteger.multiply(BigInteger.valueOf(tenToScale));
            if (decimalBigInteger.abs().compareTo(TEN_TO_NTH_LONG[(int) precision]) >= 0) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", bigint, precision, scale));
            }
            return LongDecimalType.unscaledValueToSlice(decimalBigInteger);
        }
    }

    public static class DecimalToBigintCast
            extends BaseFromDecimalCast
    {
        public DecimalToBigintCast()
        {
            super(BIGINT);
        }

        public static long castFromShortDecimal(long decimal, long precision, long scale, long tenToScale)
        {
            if (decimal >= 0) {
                return (decimal + tenToScale / 2) / tenToScale;
            }
            else {
                return -((-decimal + tenToScale / 2) / tenToScale);
            }
        }

        public static long castFromLongDecimal(Slice decimal, long precision, long scale, long tenToScale)
        {
            BigInteger decimalBigInteger = LongDecimalType.unscaledValueToBigInteger(decimal);
            BigDecimal bigDecimal = new BigDecimal(decimalBigInteger, (int) scale);
            bigDecimal = bigDecimal.setScale(0, RoundingMode.HALF_UP);
            try {
                return bigDecimal.longValueExact();
            }
            catch (ArithmeticException e) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to BIGINT", bigDecimal));
            }
        }
    }

    public static class DoubleToDecimalCast
            extends BaseToDecimalCast
    {
        public DoubleToDecimalCast()
        {
            super(DOUBLE);
        }

        public static long castToShortDecimal(double doubleValue, long precision, long scale, long tenToScale)
        {
            BigDecimal decimal = new BigDecimal(doubleValue);
            decimal = decimal.setScale((int) scale, ROUND_HALF_UP);
            if (decimal.precision() > precision) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", doubleValue, precision, scale));
            }
            return decimal.unscaledValue().longValue();
        }

        public static Slice castToLongDecimal(double doubleValue, long precision, long scale, long tenToScale)
        {
            BigDecimal decimal = new BigDecimal(doubleValue);
            decimal = decimal.setScale((int) scale, ROUND_HALF_UP);
            if (decimal.precision() > precision) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", doubleValue, precision, scale));
            }
            BigInteger decimalBigInteger = decimal.unscaledValue();
            return LongDecimalType.unscaledValueToSlice(decimalBigInteger);
        }
    }

    public static class DecimalToDoubleCast
            extends BaseFromDecimalCast
    {
        public DecimalToDoubleCast()
        {
            super(DOUBLE);
        }

        public static double castFromShortDecimal(long decimal, long precision, long scale, long tenToScale)
        {
            return ((double) decimal) / tenToScale;
        }

        public static double castFromLongDecimal(Slice decimal, long precision, long scale, long tenToScale)
        {
            BigInteger decimalBigInteger = LongDecimalType.unscaledValueToBigInteger(decimal);
            BigDecimal bigDecimal = new BigDecimal(decimalBigInteger, (int) scale);
            return bigDecimal.doubleValue();
        }
    }

    public static class VarcharToDecimalCast
            extends BaseToDecimalCast
    {
        public VarcharToDecimalCast()
        {
            super(VARCHAR);
        }

        public static long castToShortDecimal(Slice varchar, long precision, long scale, long tenToScale)
        {
            try {
                String varcharStr = varchar.toString(UTF_8);
                BigDecimal decimal = new BigDecimal(varcharStr);
                decimal = decimal.setScale((int) scale, ROUND_HALF_UP);
                if (decimal.precision() > precision) {
                    throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s)", varcharStr, precision, scale));
                }
                return decimal.unscaledValue().longValue();
            }
            catch (NumberFormatException e) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s)", varchar.toString(UTF_8), precision, scale));
            }
        }

        public static Slice castToLongDecimal(Slice varchar, long precision, long scale, long tenToScale)
        {
            String varcharStr = varchar.toString(UTF_8);
            BigDecimal decimal = new BigDecimal(varcharStr);
            decimal = decimal.setScale((int) scale, ROUND_HALF_UP);
            if (decimal.precision() > precision) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s)", varcharStr, precision, scale));
            }
            return LongDecimalType.unscaledValueToSlice(decimal.unscaledValue());
        }
    }

    public static class DecimalToVarcharCast
            extends BaseFromDecimalCast
    {
        public DecimalToVarcharCast()
        {
            super(VARCHAR);
        }

        public static Slice castFromShortDecimal(long decimal, long precision, long scale, long tenToScale)
        {
            return Slices.copiedBuffer(ShortDecimalType.toString(decimal, (int) precision, (int) scale), UTF_8);
        }

        public static Slice castFromLongDecimal(Slice decimal, long precision, long scale, long tenToScale)
        {
            return Slices.copiedBuffer(LongDecimalType.toString(decimal, (int) precision, (int) scale), UTF_8);
        }
    }

    private abstract static class BaseFromDecimalCast
            extends ParametricOperator
    {
        private final String resultType;

        protected BaseFromDecimalCast(String resultType)
        {
            super(CAST, ImmutableList.of(comparableWithVariadicBound("D", DECIMAL)), resultType, ImmutableList.of("D"));
            this.resultType = resultType;
        }

        @Override
        public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            checkArgument(arity == 1, "Expected arity to be 1");

            DecimalType aType = (DecimalType) types.get("D");
            long tenToScale = round(pow(10, aType.getScale()));

            MethodHandle methodHandle;
            if (aType.getPrecision() <= DecimalType.MAX_SHORT_PRECISION) {
                methodHandle = Reflection.methodHandle(getClass(), "castFromShortDecimal", long.class, long.class, long.class, long.class);
            }
            else {
                methodHandle = Reflection.methodHandle(getClass(), "castFromLongDecimal", Slice.class, long.class, long.class, long.class);
            }

            methodHandle = MethodHandles.insertArguments(methodHandle, 1, aType.getPrecision(), aType.getScale(), tenToScale);
            return operatorInfo(CAST, parseTypeSignature(resultType), ImmutableList.of(aType.getTypeSignature()), methodHandle, false, ImmutableList.of(false));
        }
    }

    private abstract static class BaseToDecimalCast
            extends ParametricOperator
    {
        private final String argumentType;

        protected BaseToDecimalCast(String argumentType)
        {
            super(CAST, ImmutableList.of(comparableWithVariadicBound("D", DECIMAL)), "D", ImmutableList.of(argumentType));
            this.argumentType = argumentType;
        }

        @Override
        public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            checkArgument(arity == 1, "Expected arity to be 1");

            DecimalType aType = (DecimalType) types.get("D");
            long tenToScale = round(pow(10, aType.getScale()));

            Type argumentConcreteType = typeManager.getType(parseTypeSignature(argumentType));
            MethodHandle methodHandle;
            if (aType.getPrecision() <= DecimalType.MAX_SHORT_PRECISION) {
                methodHandle = Reflection.methodHandle(getClass(), "castToShortDecimal", argumentConcreteType.getJavaType(), long.class, long.class, long.class);
            }
            else {
                methodHandle = Reflection.methodHandle(getClass(), "castToLongDecimal", argumentConcreteType.getJavaType(), long.class, long.class, long.class);
            }

            methodHandle = MethodHandles.insertArguments(methodHandle, 1, aType.getPrecision(), aType.getScale(), tenToScale);
            return operatorInfo(CAST, aType.getTypeSignature(), ImmutableList.of(argumentConcreteType.getTypeSignature()), methodHandle, false, ImmutableList.of(false));
        }
    }
}
