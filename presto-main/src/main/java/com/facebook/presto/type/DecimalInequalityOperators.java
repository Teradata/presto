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
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.LongDecimalType;
import com.facebook.presto.spi.type.ShortDecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.math.BigInteger;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.BETWEEN;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.OperatorType.NOT_EQUAL;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.LongDecimalType.unscaledValueToBigInteger;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.Integer.max;
import static java.lang.String.format;
import static java.math.BigInteger.TEN;

public class DecimalInequalityOperators
{
    private static final TypeSignature BOOLEAN_SIGNATURE = TypeSignature.parseTypeSignature(BOOLEAN);
    private static final MethodHandle GET_RESULT_EQUAL = methodHandle(DecimalInequalityOperators.class, "getResultEqual", int.class);
    private static final MethodHandle GET_RESULT_NOT_EQUAL = methodHandle(DecimalInequalityOperators.class, "getResultNotEqual", int.class);
    private static final MethodHandle GET_RESULT_LESS_THAN = methodHandle(DecimalInequalityOperators.class, "getResultLessThan", int.class);
    private static final MethodHandle GET_RESULT_LESS_THAN_OR_EQUAL = methodHandle(DecimalInequalityOperators.class, "getResultLessThanOrEqual", int.class);
    private static final MethodHandle GET_RESULT_GREATER_THAN = methodHandle(DecimalInequalityOperators.class, "getResultGreaterThan", int.class);
    private static final MethodHandle GET_RESULT_GREATER_THAN_OR_EQUAL = methodHandle(DecimalInequalityOperators.class, "getResultGreaterThanOrEqual", int.class);
    private static final int MAX_PRECISION_OF_LONG = 18;

    public static final ParametricOperator DECIMAL_EQUAL_OPERATOR = new BinaryOperatorImpl(EQUAL, GET_RESULT_EQUAL);
    public static final ParametricOperator DECIMAL_NOT_EQUAL_OPERATOR = new BinaryOperatorImpl(NOT_EQUAL, GET_RESULT_NOT_EQUAL);
    public static final ParametricOperator DECIMAL_LESS_THAN_OPERATOR = new BinaryOperatorImpl(LESS_THAN, GET_RESULT_LESS_THAN);
    public static final ParametricOperator DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR = new BinaryOperatorImpl(LESS_THAN_OR_EQUAL, GET_RESULT_LESS_THAN_OR_EQUAL);
    public static final ParametricOperator DECIMAL_GREATER_THAN_OPERATOR = new BinaryOperatorImpl(GREATER_THAN, GET_RESULT_GREATER_THAN);
    public static final ParametricOperator DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR = new BinaryOperatorImpl(GREATER_THAN_OR_EQUAL, GET_RESULT_GREATER_THAN_OR_EQUAL);
    public static final ParametricOperator DECIMAL_BETWEEN_OPERATOR = new BetweenOperatorImpl();

    private DecimalInequalityOperators() {}

    public static boolean getResultEqual(int comparisonResult)
    {
        return comparisonResult == 0;
    }

    public static boolean getResultNotEqual(int comparisonResult)
    {
        return comparisonResult != 0;
    }

    public static boolean getResultLessThan(int comparisonResult)
    {
        return comparisonResult < 0;
    }

    public static boolean getResultLessThanOrEqual(int comparisonResult)
    {
        return comparisonResult <= 0;
    }

    public static boolean getResultGreaterThan(int comparisonResult)
    {
        return comparisonResult > 0;
    }

    public static boolean getResultGreaterThanOrEqual(int comparisonResult)
    {
        return comparisonResult >= 0;
    }

    public static class BinaryOperatorImpl
            extends ParametricOperator
    {
        private static final MethodHandle OP_SHORT_SHORT_SHORT_RESCALES = methodHandle(BinaryOperatorImpl.class, "opShortShortShortRescales", long.class, long.class, long.class, long.class, MethodHandle.class);
        private static final MethodHandle OP_SHORT_SHORT_LONG_RESCALES = methodHandle(BinaryOperatorImpl.class, "opShortShortLongRescales", long.class, long.class, BigInteger.class, BigInteger.class, MethodHandle.class);
        private static final MethodHandle OP_SHORT_LONG = methodHandle(BinaryOperatorImpl.class, "opShortLong", long.class, Slice.class, BigInteger.class, BigInteger.class, MethodHandle.class);
        private static final MethodHandle OP_LONG_SHORT = methodHandle(BinaryOperatorImpl.class, "opLongShort", Slice.class, long.class, BigInteger.class, BigInteger.class, MethodHandle.class);
        private static final MethodHandle OP_LONG_LONG = methodHandle(BinaryOperatorImpl.class, "opLongLong", Slice.class, Slice.class, BigInteger.class, BigInteger.class, MethodHandle.class);

        private final OperatorType operatorType;
        private final MethodHandle getResultMethodHandle;

        public BinaryOperatorImpl(OperatorType operatorType, MethodHandle getResultMethodHandle)
        {
            super(operatorType, ImmutableList.of(
                    comparableWithVariadicBound("A", DECIMAL),
                    comparableWithVariadicBound("B", DECIMAL)), BOOLEAN, ImmutableList.of("A", "B"));

            this.operatorType = operatorType;
            this.getResultMethodHandle = getResultMethodHandle;
        }

        public static boolean opShortShortShortRescales(long a, long b, long aRescale, long bRescale, MethodHandle getResultMethodHandle)
        {
            return invokeGetResult(getResultMethodHandle, Long.compare(a * aRescale, b * bRescale));
        }

        public static boolean opShortShortLongRescales(long a, long b, BigInteger aRescale, BigInteger bRescale, MethodHandle getResultMethodHandle)
        {
            BigInteger left = BigInteger.valueOf(a).multiply(aRescale);
            BigInteger right = BigInteger.valueOf(b).multiply(bRescale);
            return invokeGetResult(getResultMethodHandle, left.compareTo(right));
        }

        public static boolean opShortLong(long a, Slice b, BigInteger aRescale, BigInteger bRescale, MethodHandle getResultMethodHandle)
        {
            BigInteger left = BigInteger.valueOf(a).multiply(aRescale);
            BigInteger right = unscaledValueToBigInteger(b).multiply(bRescale);
            return invokeGetResult(getResultMethodHandle, left.compareTo(right));
        }

        public static boolean opLongShort(Slice a, long b, BigInteger aRescale, BigInteger bRescale, MethodHandle getResultMethodHandle)
        {
            BigInteger left = unscaledValueToBigInteger(a).multiply(aRescale);
            BigInteger right = BigInteger.valueOf(b).multiply(bRescale);
            return invokeGetResult(getResultMethodHandle, left.compareTo(right));
        }

        public static boolean opLongLong(Slice a, Slice b, BigInteger aRescale, BigInteger bRescale, MethodHandle getResultMethodHandle)
        {
            BigInteger left = unscaledValueToBigInteger(a).multiply(aRescale);
            BigInteger right = unscaledValueToBigInteger(b).multiply(bRescale);
            return invokeGetResult(getResultMethodHandle, left.compareTo(right));
        }

        private static boolean invokeGetResult(MethodHandle getResultMethodHandle, int comparisonResult)
        {
            try {
                return (boolean) getResultMethodHandle.invokeExact(comparisonResult);
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(INTERNAL_ERROR, t);
            }
        }

        @Override
        public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            DecimalType aType = (DecimalType) types.get("A");
            DecimalType bType = (DecimalType) types.get("B");
            MethodHandle methodHandle = getMethodHandle(aType, bType);
            return operatorInfo(operatorType, BOOLEAN_SIGNATURE, ImmutableList.of(aType.getTypeSignature(), bType.getTypeSignature()), methodHandle, false, ImmutableList.of(false, false));
        }

        private MethodHandle getMethodHandle(DecimalType aType, DecimalType bType)
        {
            MethodHandle baseMethodHandle = getBaseMethodHandle(aType, bType);
            BigInteger aRescale = TEN.pow(max(0, bType.getScale() - aType.getScale()));
            BigInteger bRescale = TEN.pow(max(0, aType.getScale() - bType.getScale()));

            if (baseMethodHandle == OP_SHORT_SHORT_SHORT_RESCALES) {
                return MethodHandles.insertArguments(baseMethodHandle, 2, aRescale.longValue(), bRescale.longValue(), getResultMethodHandle);
            }
            else {
                return MethodHandles.insertArguments(baseMethodHandle, 2, aRescale, bRescale, getResultMethodHandle);
            }
        }

        private MethodHandle getBaseMethodHandle(DecimalType aType, DecimalType bType)
        {
            int aRescaleFactor = max(0, bType.getScale() - aType.getScale());
            int bRescaleFactor = max(0, aType.getScale() - bType.getScale());
            if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType
                    && aType.getPrecision() + aRescaleFactor <= MAX_PRECISION_OF_LONG
                    && bType.getPrecision() + bRescaleFactor <= MAX_PRECISION_OF_LONG) {
                return OP_SHORT_SHORT_SHORT_RESCALES;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType) {
                return OP_SHORT_SHORT_LONG_RESCALES;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof LongDecimalType) {
                return OP_SHORT_LONG;
            }
            else if (aType instanceof LongDecimalType && bType instanceof ShortDecimalType) {
                return OP_LONG_SHORT;
            }
            else if (aType instanceof LongDecimalType && bType instanceof LongDecimalType) {
                return OP_LONG_LONG;
            }
            else {
                throw new IllegalArgumentException("unsupported argument types: " + aType + ", " + bType);
            }
        }
    }

    // todo not a big fan of lots of boilerplate code her
    //      could benefit from bytecode generation.
    public static class BetweenOperatorImpl
            extends ParametricOperator
    {
        private static final MethodHandle OP_SHORT_SHORT_SHORT = methodHandle(BetweenOperatorImpl.class, "betweenShortShortShort", long.class, long.class, long.class, MethodHandle.class, MethodHandle.class);
        private static final MethodHandle OP_SHORT_SHORT_LONG = methodHandle(BetweenOperatorImpl.class, "betweenShortShortLong", long.class, long.class, Slice.class, MethodHandle.class, MethodHandle.class);
        private static final MethodHandle OP_SHORT_LONG_SHORT = methodHandle(BetweenOperatorImpl.class, "betweenShortLongShort", long.class, Slice.class, long.class, MethodHandle.class, MethodHandle.class);
        private static final MethodHandle OP_SHORT_LONG_LONG = methodHandle(BetweenOperatorImpl.class, "betweenShortLongLong", long.class, Slice.class, Slice.class, MethodHandle.class, MethodHandle.class);
        private static final MethodHandle OP_LONG_SHORT_SHORT = methodHandle(BetweenOperatorImpl.class, "betweenLongShortShort", Slice.class, long.class, long.class, MethodHandle.class, MethodHandle.class);
        private static final MethodHandle OP_LONG_SHORT_LONG = methodHandle(BetweenOperatorImpl.class, "betweenLongShortLong", Slice.class, long.class, Slice.class, MethodHandle.class, MethodHandle.class);
        private static final MethodHandle OP_LONG_LONG_SHORT = methodHandle(BetweenOperatorImpl.class, "betweenLongLongShort", Slice.class, Slice.class, long.class, MethodHandle.class, MethodHandle.class);
        private static final MethodHandle OP_LONG_LONG_LONG = methodHandle(BetweenOperatorImpl.class, "betweenLongLongLong", Slice.class, Slice.class, Slice.class, MethodHandle.class, MethodHandle.class);

        protected BetweenOperatorImpl()
        {
            super(BETWEEN, ImmutableList.of(
                    comparableWithVariadicBound("V", DECIMAL),
                    comparableWithVariadicBound("A", DECIMAL),
                    comparableWithVariadicBound("B", DECIMAL)), BOOLEAN, ImmutableList.of("V", "A", "B"));
        }

        @Override
        public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            DecimalType vType = (DecimalType) types.get("V");
            DecimalType aType = (DecimalType) types.get("A");
            DecimalType bType = (DecimalType) types.get("B");

            MethodHandle baseMethodHandle;
            if (vType instanceof ShortDecimalType && aType instanceof ShortDecimalType && bType instanceof ShortDecimalType) {
                baseMethodHandle = OP_SHORT_SHORT_SHORT;
            }
            else if (vType instanceof ShortDecimalType && aType instanceof ShortDecimalType && bType instanceof LongDecimalType) {
                baseMethodHandle = OP_SHORT_SHORT_LONG;
            }
            else if (vType instanceof ShortDecimalType && aType instanceof LongDecimalType && bType instanceof ShortDecimalType) {
                baseMethodHandle = OP_SHORT_LONG_SHORT;
            }
            else if (vType instanceof ShortDecimalType && aType instanceof LongDecimalType && bType instanceof LongDecimalType) {
                baseMethodHandle = OP_SHORT_LONG_LONG;
            }
            else if (vType instanceof LongDecimalType && aType instanceof ShortDecimalType && bType instanceof ShortDecimalType) {
                baseMethodHandle = OP_LONG_SHORT_SHORT;
            }
            else if (vType instanceof LongDecimalType && aType instanceof ShortDecimalType && bType instanceof LongDecimalType) {
                baseMethodHandle = OP_LONG_SHORT_LONG;
            }
            else if (vType instanceof LongDecimalType && aType instanceof LongDecimalType && bType instanceof ShortDecimalType) {
                baseMethodHandle = OP_LONG_LONG_SHORT;
            }
            else if (vType instanceof LongDecimalType && aType instanceof LongDecimalType && bType instanceof LongDecimalType) {
                baseMethodHandle = OP_LONG_LONG_LONG;
            }
            else {
                throw new IllegalArgumentException(format("unsuported argument types (%s, %s, %s)", vType, aType, bType));
            }

            MethodHandle lowerBoundTestMethodHandle = DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR.specialize(ImmutableMap.<String, Type>of("A", aType, "B", vType), 2, typeManager, functionRegistry).getMethodHandle();
            MethodHandle upperBoundTestMethodHandle = DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR.specialize(ImmutableMap.<String, Type>of("A", bType, "B", vType), 2, typeManager, functionRegistry).getMethodHandle();

            MethodHandle methodHandle = MethodHandles.insertArguments(baseMethodHandle, 3, lowerBoundTestMethodHandle, upperBoundTestMethodHandle);
            return operatorInfo(BETWEEN, BOOLEAN_SIGNATURE, ImmutableList.of(vType.getTypeSignature(), aType.getTypeSignature(), bType.getTypeSignature()),
                    methodHandle, false, ImmutableList.of(false, false, false));
        }

        public static boolean betweenShortShortShort(long v, long a, long b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
                throws Throwable
        {
            return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
        }

        public static boolean betweenShortShortLong(long v, long a, Slice b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
                throws Throwable
        {
            return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
        }

        public static boolean betweenShortLongShort(long v, Slice a, long b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
                throws Throwable
        {
            return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
        }

        public static boolean betweenShortLongLong(long v, Slice a, Slice b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
                throws Throwable
        {
            return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
        }

        public static boolean betweenLongShortShort(Slice v, long a, long b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
                throws Throwable
        {
            return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
        }

        public static boolean betweenLongShortLong(Slice v, long a, Slice b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
                throws Throwable
        {
            return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
        }

        public static boolean betweenLongLongShort(Slice v, Slice a, long b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
                throws Throwable
        {
            return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
        }

        public static boolean betweenLongLongLong(Slice v, Slice a, Slice b, MethodHandle lowerBoundTestMethodHandle, MethodHandle upperBoundTestMethodHandle)
                throws Throwable
        {
            return (boolean) lowerBoundTestMethodHandle.invokeExact(a, v) && (boolean) upperBoundTestMethodHandle.invokeExact(b, v);
        }
    }
}
