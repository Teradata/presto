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
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.math.BigInteger;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
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
import static java.math.BigInteger.TEN;

public class DecimalInequalityOperators

{
    public static final int MAX_PRECISION_OF_LONG = 18;

    private DecimalInequalityOperators() {}

    private static final MethodHandle GET_RESULT_EQUAL = methodHandle(DecimalInequalityOperators.class, "getResultEqual", int.class);
    private static final MethodHandle GET_RESULT_NOT_EQUAL = methodHandle(DecimalInequalityOperators.class, "getResultNotEqual", int.class);
    private static final MethodHandle GET_RESULT_LESS_THAN = methodHandle(DecimalInequalityOperators.class, "getResultLessThan", int.class);
    private static final MethodHandle GET_RESULT_LESS_THAN_OR_EQUAL = methodHandle(DecimalInequalityOperators.class, "getResultLessThanOrEqual", int.class);
    private static final MethodHandle GET_RESULT_GREATER_THAN = methodHandle(DecimalInequalityOperators.class, "getResultGreaterThan", int.class);
    private static final MethodHandle GET_RESULT_GREATER_THAN_OR_EQUAL = methodHandle(DecimalInequalityOperators.class, "getResultGreaterThanOrEqual", int.class);

    public static final ParametricOperator DECIMAL_EQUAL_OPERATOR = new OperatorImpl(EQUAL, GET_RESULT_EQUAL);
    public static final ParametricOperator DECIMAL_NOT_EQUAL_OPERATOR = new OperatorImpl(NOT_EQUAL, GET_RESULT_NOT_EQUAL);
    public static final ParametricOperator DECIMAL_LESS_THAN_OPERATOR = new OperatorImpl(LESS_THAN, GET_RESULT_LESS_THAN);
    public static final ParametricOperator DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR = new OperatorImpl(LESS_THAN_OR_EQUAL, GET_RESULT_LESS_THAN_OR_EQUAL);
    public static final ParametricOperator DECIMAL_GREATER_THAN_OPERATOR = new OperatorImpl(GREATER_THAN, GET_RESULT_GREATER_THAN);
    public static final ParametricOperator DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR = new OperatorImpl(GREATER_THAN_OR_EQUAL, GET_RESULT_GREATER_THAN_OR_EQUAL);

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

    public static class OperatorImpl
            extends ParametricOperator
    {
        private static final TypeSignature BOOLEAN_SIGNATURE = TypeSignature.parseTypeSignature(BOOLEAN);
        private static final MethodHandle OP_SHORT_SHORT_SHORT_RESCALES = methodHandle(OperatorImpl.class, "opShortShortShortRescales", long.class, long.class, long.class, long.class, MethodHandle.class);
        private static final MethodHandle OP_SHORT_SHORT_LONG_RESCALES = methodHandle(OperatorImpl.class, "opShortShortLongRescales", long.class, long.class, BigInteger.class, BigInteger.class, MethodHandle.class);
        private static final MethodHandle OP_SHORT_LONG = methodHandle(OperatorImpl.class, "opShortLong", long.class, Slice.class, BigInteger.class, BigInteger.class, MethodHandle.class);
        private static final MethodHandle OP_LONG_SHORT = methodHandle(OperatorImpl.class, "opLongShort", Slice.class, long.class, BigInteger.class, BigInteger.class, MethodHandle.class);
        private static final MethodHandle OP_LONG_LONG = methodHandle(OperatorImpl.class, "opLongLong", Slice.class, Slice.class, BigInteger.class, BigInteger.class, MethodHandle.class);

        private final OperatorType operatorType;
        private final MethodHandle getResultMethodHandle;

        public OperatorImpl(OperatorType operatorType, MethodHandle getResultMethodHandle)
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
}
