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
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.LongDecimalType;
import com.facebook.presto.spi.type.ShortDecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.ADD;
import static com.facebook.presto.metadata.OperatorType.DIVIDE;
import static com.facebook.presto.metadata.OperatorType.MODULUS;
import static com.facebook.presto.metadata.OperatorType.MULTIPLY;
import static com.facebook.presto.metadata.OperatorType.SUBTRACT;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.type.LongDecimalType.MAX_DECIMAL_UNSCALED_VALUE;
import static com.facebook.presto.spi.type.LongDecimalType.MAX_PRECISION;
import static com.facebook.presto.spi.type.LongDecimalType.MIN_DECIMAL_UNSCALED_VALUE;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.math.BigInteger.ONE;
import static java.math.BigInteger.TEN;
import static java.math.BigInteger.ZERO;

public final class DecimalOperators
{
    public static final DecimalAddOperator DECIMAL_ADD_OPERATOR = new DecimalAddOperator();
    public static final DecimalSubtractOperator DECIMAL_SUBTRACT_OPERATOR = new DecimalSubtractOperator();
    public static final DecimalMultiplyOperator DECIMAL_MULTIPLY_OPERATOR = new DecimalMultiplyOperator();
    public static final DecimalDivideOperator DECIMAL_DIVIDE_OPERATOR = new DecimalDivideOperator();
    public static final DecimalModulusOperator DECIMAL_MODULUS_OPERATOR = new DecimalModulusOperator();

    private DecimalOperators()
    {
    }

    private abstract static class BaseDecimalBinaryOperator
            extends ParametricOperator
    {
        private OperatorType operatorType;

        protected BaseDecimalBinaryOperator(OperatorType operatorType)
        {
            super(operatorType, ImmutableList.of(
                    comparableWithVariadicBound("A", DECIMAL),
                    comparableWithVariadicBound("B", DECIMAL)), DECIMAL, ImmutableList.of("A", "B"));
            this.operatorType = operatorType;
        }

        @Override
        public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            checkArgument(arity == 2, "Expected arity to be 2");
            DecimalType aType = (DecimalType) types.get("A");
            DecimalType bType = (DecimalType) types.get("B");

            int aScale = aType.getScale();
            int bScale = bType.getScale();
            int aPrecision = aType.getPrecision();
            int bPrecision = bType.getPrecision();
            int resultPrecision = getResultPrecision(aPrecision, aScale, bPrecision, bScale);
            int resultScale = getResultScale(aPrecision, aScale, bPrecision, bScale);

            DecimalType resultType = DecimalType.createDecimalType(resultPrecision, resultScale);

            MethodHandle baseMethodHandle = getBaseMethodHandle(aType, bType, resultType);
            List<Object> extraArguments = getExtraArguments(aPrecision, aScale, bPrecision, bScale, resultPrecision, resultScale, baseMethodHandle);
            MethodHandle methodHandle = MethodHandles.insertArguments(baseMethodHandle, 2, extraArguments.toArray());

            return operatorInfo(operatorType, resultType.getTypeSignature(), ImmutableList.of(aType.getTypeSignature(), bType.getTypeSignature()), methodHandle, false, ImmutableList.of(false, false));
        }

        protected abstract MethodHandle getBaseMethodHandle(DecimalType aType, DecimalType bType, DecimalType resultType);

        protected abstract List<Object> getExtraArguments(int aPrecision, int aScale, int bPrecision, int bScale, int resultPrecision, int resultScale, MethodHandle baseMethodHandle);

        protected abstract int getResultPrecision(int aPrecision, int aScale, int bPrecision, int bScale);

        protected abstract int getResultScale(int aPrecision, int aScale, int bPrecision, int bScale);
    }

    private abstract static class BaseAddSubtractDecimalBinaryOperator
            extends BaseDecimalBinaryOperator
    {
        protected BaseAddSubtractDecimalBinaryOperator(OperatorType operatorType)
        {
            super(operatorType);
        }

        @Override
        protected List<Object> getExtraArguments(int aPrecision, int aScale, int bPrecision, int bScale, int resultPrecision, int resultScale, MethodHandle baseMethodHandle)
        {
            BigInteger aRescale = TEN.pow(resultScale - aScale);
            BigInteger bRescale = TEN.pow(resultScale - bScale);
            if (rescaleParamsAreLongs(baseMethodHandle)) {
                return ImmutableList.of(aRescale.longValue(), bRescale.longValue());
            }
            else {
                return ImmutableList.of(aRescale, bRescale);
            }
        }

        private boolean rescaleParamsAreLongs(MethodHandle baseMethodHandle)
        {
            return baseMethodHandle.type().parameterType(baseMethodHandle.type().parameterCount() - 1).isAssignableFrom(long.class);
        }

        protected int getResultScale(int aPrecision, int aScale, int bPrecision, int bScale)
        {
            return max(aScale, bScale);
        }

        protected int getResultPrecision(int aPrecision, int aScale, int bPrecision, int bScale)
        {
            return min(
                    DecimalType.MAX_PRECISION,
                    1 + max(aScale, bScale) + max(aPrecision - aScale, bPrecision - bScale));
        }
    }

    public static class DecimalAddOperator
            extends BaseAddSubtractDecimalBinaryOperator
    {
        private static final MethodHandle SHORT_SHORT_SHORT_ADD_METHOD_HANDLE =
                methodHandle(DecimalAddOperator.class, "addShortShortShort", long.class, long.class, long.class, long.class);
        private static final MethodHandle SHORT_SHORT_LONG_ADD_METHOD_HANDLE =
                methodHandle(DecimalAddOperator.class, "addShortShortLong", long.class, long.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle LONG_LONG_LONG_ADD_METHOD_HANDLE =
                methodHandle(DecimalAddOperator.class, "addLongLongLong", Slice.class, Slice.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle LONG_SHORT_LONG_ADD_METHOD_HANDLE =
                methodHandle(DecimalAddOperator.class, "addLongShortLong", Slice.class, long.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle SHORT_LONG_LONG_ADD_METHOD_HANDLE =
                methodHandle(DecimalAddOperator.class, "addShortLongLong", long.class, Slice.class, BigInteger.class, BigInteger.class);

        protected DecimalAddOperator()
        {
            super(ADD);
        }

        @Override
        protected MethodHandle getBaseMethodHandle(DecimalType aType, DecimalType bType, DecimalType resultType)
        {
            MethodHandle baseMethodHandle;
            if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType && resultType instanceof ShortDecimalType) {
                baseMethodHandle = SHORT_SHORT_SHORT_ADD_METHOD_HANDLE;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType && resultType instanceof LongDecimalType) {
                baseMethodHandle = SHORT_SHORT_LONG_ADD_METHOD_HANDLE;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof LongDecimalType) {
                baseMethodHandle = SHORT_LONG_LONG_ADD_METHOD_HANDLE;
            }
            else if (aType instanceof LongDecimalType && bType instanceof ShortDecimalType) {
                baseMethodHandle = LONG_SHORT_LONG_ADD_METHOD_HANDLE;
            }
            else {
                baseMethodHandle = LONG_LONG_LONG_ADD_METHOD_HANDLE;
            }
            return baseMethodHandle;
        }

        public static long addShortShortShort(long a, long b, long aRescale, long bRescale)
        {
            return a * aRescale + b * bRescale;
        }

        public static Slice addShortShortLong(long a, long b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalAddLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice addLongLongLong(Slice a, Slice b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b);
            return internalAddLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice addShortLongLong(long a, Slice b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b);
            return internalAddLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice addLongShortLong(Slice a, long b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalAddLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        private static Slice internalAddLongLongLong(BigInteger aBigInteger, BigInteger bBigInteger, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aRescaled = aBigInteger.multiply(aRescale);
            BigInteger bRescaled = bBigInteger.multiply(bRescale);
            BigInteger result = aRescaled.add(bRescaled);
            checkOverflow(result);
            return LongDecimalType.unscaledValueToSlice(result);
        }
    }

    public static class DecimalSubtractOperator
            extends BaseAddSubtractDecimalBinaryOperator
    {
        private static final MethodHandle SHORT_SHORT_SHORT_SUBTRACT_METHOD_HANDLE =
                methodHandle(DecimalSubtractOperator.class, "subtractShortShortShort", long.class, long.class, long.class, long.class);
        private static final MethodHandle SHORT_SHORT_LONG_SUBTRACT_METHOD_HANDLE =
                methodHandle(DecimalSubtractOperator.class, "subtractShortShortLong", long.class, long.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle LONG_LONG_LONG_SUBTRACT_METHOD_HANDLE =
                methodHandle(DecimalSubtractOperator.class, "subtractLongLongLong", Slice.class, Slice.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle LONG_SHORT_LONG_SUBTRACT_METHOD_HANDLE =
                methodHandle(DecimalSubtractOperator.class, "subtractLongShortLong", Slice.class, long.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle SHORT_LONG_LONG_SUBTRACT_METHOD_HANDLE =
                methodHandle(DecimalSubtractOperator.class, "subtractShortLongLong", long.class, Slice.class, BigInteger.class, BigInteger.class);

        protected DecimalSubtractOperator()
        {
            super(SUBTRACT);
        }

        @Override
        protected MethodHandle getBaseMethodHandle(DecimalType aType, DecimalType bType, DecimalType resultType)
        {
            if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType && resultType instanceof ShortDecimalType) {
                return SHORT_SHORT_SHORT_SUBTRACT_METHOD_HANDLE;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType && resultType instanceof LongDecimalType) {
                return SHORT_SHORT_LONG_SUBTRACT_METHOD_HANDLE;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof LongDecimalType) {
                return SHORT_LONG_LONG_SUBTRACT_METHOD_HANDLE;
            }
            else if (aType instanceof LongDecimalType && bType instanceof ShortDecimalType) {
                return LONG_SHORT_LONG_SUBTRACT_METHOD_HANDLE;
            }
            else {
                return LONG_LONG_LONG_SUBTRACT_METHOD_HANDLE;
            }
        }

        public static long subtractShortShortShort(long a, long b, long aRescale, long bRescale)
        {
            return a * aRescale - b * bRescale;
        }

        public static Slice subtractShortShortLong(long a, long b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalSubtractLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice subtractLongLongLong(Slice a, Slice b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b);
            return internalSubtractLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice subtractShortLongLong(long a, Slice b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b);
            return internalSubtractLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice subtractLongShortLong(Slice a, long b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalSubtractLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        private static Slice internalSubtractLongLongLong(BigInteger aBigInteger, BigInteger bBigInteger, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aRescaled = aBigInteger.multiply(aRescale);
            BigInteger bRescaled = bBigInteger.multiply(bRescale);
            BigInteger result = aRescaled.subtract(bRescaled);
            checkOverflow(result);
            return LongDecimalType.unscaledValueToSlice(result);
        }
    }

    public static class DecimalMultiplyOperator
            extends BaseDecimalBinaryOperator
    {
        private static final MethodHandle SHORT_SHORT_SHORT_MULTIPLY_METHOD_HANDLE =
                methodHandle(DecimalMultiplyOperator.class, "multiplyShortShortShort", long.class, long.class);
        private static final MethodHandle SHORT_SHORT_LONG_MULTIPLY_METHOD_HANDLE =
                methodHandle(DecimalMultiplyOperator.class, "multiplyShortShortLong", long.class, long.class);
        private static final MethodHandle LONG_LONG_LONG_MULTIPLY_METHOD_HANDLE =
                methodHandle(DecimalMultiplyOperator.class, "multiplyLongLongLong", Slice.class, Slice.class);
        private static final MethodHandle LONG_SHORT_LONG_MULTIPLY_METHOD_HANDLE =
                methodHandle(DecimalMultiplyOperator.class, "multiplyLongShortLong", Slice.class, long.class);
        private static final MethodHandle SHORT_LONG_LONG_MULTIPLY_METHOD_HANDLE =
                methodHandle(DecimalMultiplyOperator.class, "multiplyShortLongLong", long.class, Slice.class);

        protected DecimalMultiplyOperator()
        {
            super(MULTIPLY);
        }

        @Override
        protected MethodHandle getBaseMethodHandle(DecimalType aType, DecimalType bType, DecimalType resultType)
        {
            if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType && resultType instanceof ShortDecimalType) {
                return SHORT_SHORT_SHORT_MULTIPLY_METHOD_HANDLE;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType && resultType instanceof LongDecimalType) {
                return SHORT_SHORT_LONG_MULTIPLY_METHOD_HANDLE;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof LongDecimalType) {
                return SHORT_LONG_LONG_MULTIPLY_METHOD_HANDLE;
            }
            else if (aType instanceof LongDecimalType && bType instanceof ShortDecimalType) {
                return LONG_SHORT_LONG_MULTIPLY_METHOD_HANDLE;
            }
            else {
                return LONG_LONG_LONG_MULTIPLY_METHOD_HANDLE;
            }
        }

        @Override
        protected List<Object> getExtraArguments(int aPrecision, int aScale, int bPrecision, int bScale, int resultPrecision, int resultScale, MethodHandle baseMethodHandle)
        {
            return ImmutableList.of();
        }

        protected int getResultPrecision(int aPrecision, int aScale, int bPrecision, int bScale)
        {
            return min(
                    DecimalType.MAX_PRECISION,
                    aPrecision + bPrecision);
        }

        protected int getResultScale(int aPrecision, int aScale, int bPrecision, int bScale)
        {
            int resultScale = aScale + bScale;
            if (resultScale > DecimalType.MAX_PRECISION) {
                throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "DECIMAL scale would exceed 38 after multiplication");
            }
            return resultScale;
        }

        public static long multiplyShortShortShort(long a, long b)
        {
            return a * b;
        }

        public static Slice multiplyShortShortLong(long a, long b)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalMultiplyLongLongLong(aBigInteger, bBigInteger);
        }

        public static Slice multiplyLongLongLong(Slice a, Slice b)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b);
            return internalMultiplyLongLongLong(aBigInteger, bBigInteger);
        }

        public static Slice multiplyShortLongLong(long a, Slice b)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b);
            return internalMultiplyLongLongLong(aBigInteger, bBigInteger);
        }

        public static Slice multiplyLongShortLong(Slice a, long b)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalMultiplyLongLongLong(aBigInteger, bBigInteger);
        }

        private static Slice internalMultiplyLongLongLong(BigInteger aBigInteger, BigInteger bBigInteger)
        {
            BigInteger result = aBigInteger.multiply(bBigInteger);
            checkOverflow(result);
            return LongDecimalType.unscaledValueToSlice(result);
        }
    }

    public static class DecimalDivideOperator
            extends BaseDecimalBinaryOperator
    {
        private static final MethodHandle SHORT_SHORT_SHORT_DIVIDE_METHOD_HANDLE =
                methodHandle(DecimalDivideOperator.class, "divideShortShortShort", long.class, long.class, long.class);
        private static final MethodHandle SHORT_SHORT_LONG_DIVIDE_METHOD_HANDLE =
                methodHandle(DecimalDivideOperator.class, "divideShortShortLong", long.class, long.class, BigInteger.class);
        private static final MethodHandle LONG_LONG_LONG_DIVIDE_METHOD_HANDLE =
                methodHandle(DecimalDivideOperator.class, "divideLongLongLong", Slice.class, Slice.class, BigInteger.class);
        private static final MethodHandle LONG_SHORT_LONG_DIVIDE_METHOD_HANDLE =
                methodHandle(DecimalDivideOperator.class, "divideLongShortLong", Slice.class, long.class, BigInteger.class);
        private static final MethodHandle SHORT_LONG_LONG_DIVIDE_METHOD_HANDLE =
                methodHandle(DecimalDivideOperator.class, "divideShortLongLong", long.class, Slice.class, BigInteger.class);

        protected DecimalDivideOperator()
        {
            super(DIVIDE);
        }

        @Override
        protected MethodHandle getBaseMethodHandle(DecimalType aType, DecimalType bType, DecimalType resultType)
        {
            if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType && resultType instanceof ShortDecimalType) {
                return SHORT_SHORT_SHORT_DIVIDE_METHOD_HANDLE;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType && resultType instanceof LongDecimalType) {
                return SHORT_SHORT_LONG_DIVIDE_METHOD_HANDLE;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof LongDecimalType) {
                return SHORT_LONG_LONG_DIVIDE_METHOD_HANDLE;
            }
            else if (aType instanceof LongDecimalType && bType instanceof ShortDecimalType) {
                return LONG_SHORT_LONG_DIVIDE_METHOD_HANDLE;
            }
            else {
                return LONG_LONG_LONG_DIVIDE_METHOD_HANDLE;
            }
        }

        @Override
        protected List<Object> getExtraArguments(int aPrecision, int aScale, int bPrecision, int bScale, int resultPrecision, int resultScale, MethodHandle baseMethodHandle)
        {
            // +1 because we want to do computations with one extra decimal field to be able to handle
            // rounding of the result.
            BigInteger aRescale = TEN.pow(resultScale - aScale + bScale + 1);
            if (rescaleParamIsLong(baseMethodHandle)) {
                return ImmutableList.of(aRescale.longValue());
            }
            else {
                return ImmutableList.of(aRescale);
            }
        }

        protected int getResultPrecision(int aPrecision, int aScale, int bPrecision, int bScale)
        {
            // we extend target precision by bScale. This is upper bound on how much division result will grow.
            // pessimistic case is a / 0.0000001            int precision = aPrecision + bScale;
            int precision = aPrecision + bScale;

            // if scale of divisor is greater than scale of dividend we extend scale further as we
            // want result scale to be maximum of scales of divisor and dividend.
            if (bScale > aScale) {
                precision += bScale - aScale;
            }
            return min(precision, MAX_PRECISION);
        }

        protected int getResultScale(int aPrecision, int aScale, int bPrecision, int bScale)
        {
            return max(aScale, bScale);
        }

        public static long divideShortShortShort(long a, long b, long aRescale)
        {
            try {
                long ret = a * aRescale / b;
                if (ret > 0) {
                    if (ret % 10 >= 5) {
                        return ret / 10 + 1;
                    }
                    else {
                        return ret / 10;
                    }
                }
                else {
                    if (ret % 10 <= -5) {
                        return ret / 10 - 1;
                    }
                    else {
                        return ret / 10;
                    }
                }
            }
            catch (ArithmeticException e) {
                throw new PrestoException(DIVISION_BY_ZERO, e);
            }
        }

        public static Slice divideShortShortLong(long a, long b, BigInteger aRescale)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a).multiply(aRescale);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalDivideLongLongLong(aBigInteger, bBigInteger);
        }

        public static Slice divideLongLongLong(Slice a, Slice b, BigInteger aRescale)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a).multiply(aRescale);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b);
            return internalDivideLongLongLong(aBigInteger, bBigInteger);
        }

        public static Slice divideShortLongLong(long a, Slice b, BigInteger aRescale)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a).multiply(aRescale);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b);
            return internalDivideLongLongLong(aBigInteger, bBigInteger);
        }

        public static Slice divideLongShortLong(Slice a, long b, BigInteger aRescale)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a).multiply(aRescale);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalDivideLongLongLong(aBigInteger, bBigInteger);
        }

        private boolean rescaleParamIsLong(MethodHandle baseMethodHandle)
        {
            return baseMethodHandle.type().parameterType(baseMethodHandle.type().parameterCount() - 1).isAssignableFrom(long.class);
        }

        private static Slice internalDivideLongLongLong(BigInteger aBigInteger, BigInteger bBigInteger)
        {
            try {
                BigInteger result = aBigInteger.divide(bBigInteger);
                BigInteger resultModTen = result.mod(TEN);
                if (result.signum() > 0) {
                    if (resultModTen.compareTo(BigInteger.valueOf(5)) >= 0) {
                        result = result.divide(TEN).add(ONE);
                    }
                    else {
                        result = result.divide(TEN);
                    }
                }
                else {
                    if (resultModTen.compareTo(BigInteger.valueOf(5)) < 0 && !resultModTen.equals(ZERO)) {
                        result = result.divide(TEN).subtract(ONE);
                    }
                    else {
                        result = result.divide(TEN);
                    }
                }
                checkOverflow(result);
                return LongDecimalType.unscaledValueToSlice(result);
            }
            catch (ArithmeticException e) {
                throw new PrestoException(DIVISION_BY_ZERO, e);
            }
        }
    }

    public static class DecimalModulusOperator
            extends BaseDecimalBinaryOperator
    {
        private static final MethodHandle SHORT_SHORT_SHORT_MODULUS_METHOD_HANDLE =
                methodHandle(DecimalModulusOperator.class, "modulusShortShortShort", long.class, long.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle SHORT_LONG_SHORT_MODULUS_METHOD_HANDLE =
                methodHandle(DecimalModulusOperator.class, "modulusShortLongShort", long.class, Slice.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle SHORT_LONG_LONG_MODULUS_METHOD_HANDLE =
                methodHandle(DecimalModulusOperator.class, "modulusShortLongLong", long.class, Slice.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle LONG_SHORT_SHORT_MODULUS_METHOD_HANDLE =
                methodHandle(DecimalModulusOperator.class, "modulusLongShortShort", Slice.class, long.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle LONG_SHORT_LONG_MODULUS_METHOD_HANDLE =
                methodHandle(DecimalModulusOperator.class, "modulusLongShortLong", Slice.class, long.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle LONG_LONG_LONG_MODULUS_METHOD_HANDLE =
                methodHandle(DecimalModulusOperator.class, "modulusLongLongLong", Slice.class, Slice.class, BigInteger.class, BigInteger.class);

        protected DecimalModulusOperator()
        {
            super(MODULUS);
        }

        @Override
        protected MethodHandle getBaseMethodHandle(DecimalType aType, DecimalType bType, DecimalType resultType)
        {
            // TODO currently all methods take rescale parameters as BigInteger and all intermediate
            // calculations use BigIntegers. There are some cases when we know that intermediate result would
            // fit in long. Expoiting that is possible optimization

            if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType && resultType instanceof ShortDecimalType) {
                return SHORT_SHORT_SHORT_MODULUS_METHOD_HANDLE;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof LongDecimalType && resultType instanceof ShortDecimalType) {
                return SHORT_LONG_SHORT_MODULUS_METHOD_HANDLE;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof LongDecimalType && resultType instanceof LongDecimalType) {
                return SHORT_LONG_LONG_MODULUS_METHOD_HANDLE;
            }
            else if (aType instanceof LongDecimalType && bType instanceof ShortDecimalType && resultType instanceof ShortDecimalType) {
                return LONG_SHORT_SHORT_MODULUS_METHOD_HANDLE;
            }
            else if (aType instanceof LongDecimalType && bType instanceof ShortDecimalType && resultType instanceof LongDecimalType) {
                return LONG_SHORT_LONG_MODULUS_METHOD_HANDLE;
            }
            else {
                return LONG_LONG_LONG_MODULUS_METHOD_HANDLE;
            }
        }

        @Override
        protected List<Object> getExtraArguments(int aPrecision, int aScale, int bPrecision, int bScale, int resultPrecision, int resultScale, MethodHandle baseMethodHandle)
        {
            BigInteger aRescale = TEN.pow(max(0, bScale - aScale));
            BigInteger bRescale = TEN.pow(max(0, aScale - bScale));
            return ImmutableList.of(aRescale, bRescale);
        }

        protected int getResultPrecision(int aPrecision, int aScale, int bPrecision, int bScale)
        {
            return min(bPrecision - bScale, aPrecision - aScale) + max(aScale, bScale);
        }

        protected int getResultScale(int aPrecision, int aScale, int bPrecision, int bScale)
        {
            return max(aScale, bScale);
        }

        public static long modulusShortShortShort(long a, long b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalModulusShortResult(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice modulusLongLongLong(Slice a, Slice b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b);
            return internalModulusLongResult(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice modulusShortLongLong(long a, Slice b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b);
            return internalModulusLongResult(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static long modulusShortLongShort(long a, Slice b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a).multiply(aRescale);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b).multiply(bRescale);
            return internalModulusShortResult(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static long modulusLongShortShort(Slice a, long b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalModulusShortResult(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice modulusLongShortLong(Slice a, long b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalModulusLongResult(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        private static long internalModulusShortResult(BigInteger aBigInteger, BigInteger bBigInteger, BigInteger aRescale, BigInteger bRescale)
        {
            try {
                return aBigInteger.multiply(aRescale).remainder(bBigInteger.multiply(bRescale)).longValue();
            }
            catch (ArithmeticException e) {
                throw new PrestoException(DIVISION_BY_ZERO, e);
            }
        }

        private static Slice internalModulusLongResult(BigInteger aBigInteger, BigInteger bBigInteger, BigInteger aRescale, BigInteger bRescale)
        {
            try {
                BigInteger result = aBigInteger.multiply(aRescale).remainder(bBigInteger.multiply(bRescale));
                return LongDecimalType.unscaledValueToSlice(result);
            }
            catch (ArithmeticException e) {
                throw new PrestoException(DIVISION_BY_ZERO, e);
            }
        }
    }

    private static void checkOverflow(BigInteger value)
    {
        if (value.compareTo(MAX_DECIMAL_UNSCALED_VALUE) > 0 || value.compareTo(MIN_DECIMAL_UNSCALED_VALUE) < 0) {
            // todo determine correct ErrorCode.
            throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "DECIMAL result exceeds 38 digits");
        }
    }
}
