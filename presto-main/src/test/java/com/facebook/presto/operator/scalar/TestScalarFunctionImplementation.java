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

package com.facebook.presto.operator.scalar;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Optional;

import static java.lang.invoke.MethodType.methodType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestScalarFunctionImplementation
{
    private static final MethodHandle DUMMY_METHOD_HANDLE = MethodHandles.identity(TestMethodHandles.class);

    public static class TestMethodHandles
    {
        private long c;

        public TestMethodHandles(long c)
        {
            this.c = c;
        }

        @UsedByGeneratedCode
        public static Slice returnAsReturnNoInstance(long a, long b)
        {
            return Slices.copiedBuffer(String.format("(%d, %d)", a, b), UTF_8);
        }

        @UsedByGeneratedCode
        public static void returnAsParameterNoInstance(Slice ret, long a, long b)
        {
            ret.setBytes(0, String.format("(%d, %d)", a, b).getBytes(UTF_8));
        }

        @UsedByGeneratedCode
        public Slice returnAsReturnWithInstance(long a, long b)
        {
            return Slices.copiedBuffer(String.format("(%d, %d, %d)", a, b, c), UTF_8);
        }

        @UsedByGeneratedCode
        public void returnAsParameterWithInstance(Slice ret, long a, long b)
        {
            ret.clear();
            ret.setBytes(0, String.format("(%d, %d, %d)", a, b, c).getBytes(UTF_8));
        }
    }

    @Test
    public void testReturnAsReturnNoInstanceEnsure()
            throws Throwable
    {
        MethodHandle methodHandle = MethodHandles.lookup().findStatic(TestMethodHandles.class, "returnAsReturnNoInstance", methodType(Slice.class, long.class, long.class));
        ScalarFunctionImplementation scalarFunctionImplementation = new ScalarFunctionImplementation(false, ImmutableList.of(false, false), methodHandle, Optional.empty(), true, false, Optional.empty());

        Slice ret = (Slice) scalarFunctionImplementation.ensureReturnValueAsReturn().getMethodHandle().invoke(1, 2);
        assertEquals(ret.toStringUtf8(), "(1, 2)");
    }

    @Test
    public void testReturnAsParameterNoInstanceEnsure()
            throws Throwable
    {
        MethodHandle methodHandle = MethodHandles.lookup().findStatic(TestMethodHandles.class, "returnAsParameterNoInstance", methodType(void.class, Slice.class, long.class, long.class));
        ScalarFunctionImplementation scalarFunctionImplementation = new ScalarFunctionImplementation(false, ImmutableList.of(false, false), methodHandle, Optional.empty(), true, true, Optional.of(6));

        Slice ret = (Slice) scalarFunctionImplementation.ensureReturnValueAsReturn().getMethodHandle().invoke(1, 2);
        assertEquals(ret.toStringUtf8(), "(1, 2)");
    }

    @Test
    public void testReturnAsReturnWithEnsure()
            throws Throwable
    {
        MethodHandle methodHandle = MethodHandles.lookup().findVirtual(TestMethodHandles.class, "returnAsReturnWithInstance", methodType(Slice.class, long.class, long.class));
        ScalarFunctionImplementation scalarFunctionImplementation = new ScalarFunctionImplementation(false, ImmutableList.of(false, false), methodHandle, Optional.of(DUMMY_METHOD_HANDLE), true, false, Optional.empty());

        Slice ret = (Slice) scalarFunctionImplementation.ensureReturnValueAsReturn().getMethodHandle().invoke(new TestMethodHandles(3), 1, 2);
        assertEquals(ret.toStringUtf8(), "(1, 2, 3)");
    }

    @Test
    public void testReturnAsParameterWithInstanceEnsure()
            throws Throwable
    {
        MethodHandle methodHandle = MethodHandles.lookup().findVirtual(TestMethodHandles.class, "returnAsParameterWithInstance", methodType(void.class, Slice.class, long.class, long.class));
        ScalarFunctionImplementation scalarFunctionImplementation = new ScalarFunctionImplementation(false, ImmutableList.of(false, false), methodHandle, Optional.of(DUMMY_METHOD_HANDLE), true, true, Optional.of(9));

        Slice ret = (Slice) scalarFunctionImplementation.ensureReturnValueAsReturn().getMethodHandle().invoke(new TestMethodHandles(3), 1, 2);
        assertEquals(ret.toStringUtf8(), "(1, 2, 3)");
    }
}
