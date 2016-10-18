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
package com.facebook.presto.spiller;

import com.facebook.presto.ExceededSpillLimitException;
import com.facebook.presto.spi.QueryId;
import io.airlift.units.DataSize;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;

public class TestSpillSpaceTracker
{
    private static final DataSize MAX_DATA_SIZE = new DataSize(10, MEGABYTE);
    private SpillSpaceTracker spillSpaceTracker;

    @BeforeTest
    public void setUp()
    {
        spillSpaceTracker = new SpillSpaceTracker(MAX_DATA_SIZE);
    }

    @Test
    public void testSpillSpaceTracker()
    {
        assertEquals(spillSpaceTracker.getCurrentBytes(), 0);
        assertEquals(spillSpaceTracker.getMaxBytes(), MAX_DATA_SIZE.toBytes());
        QueryId mockQueryId = new QueryId("test");
        long reservedBytes = new DataSize(5, MEGABYTE).toBytes();
        spillSpaceTracker.reserve(mockQueryId, reservedBytes);
        assertEquals(spillSpaceTracker.getCurrentBytes(), reservedBytes);

        QueryId mockQueryId2 = new QueryId("test2");
        long otherReservedBytes = new DataSize(2, MEGABYTE).toBytes();
        spillSpaceTracker.reserve(mockQueryId2, otherReservedBytes);
        assertEquals(spillSpaceTracker.getCurrentBytes(), (reservedBytes + otherReservedBytes));

        spillSpaceTracker.reserve(mockQueryId, otherReservedBytes);
        assertEquals(spillSpaceTracker.getCurrentBytes(), (reservedBytes + 2 * otherReservedBytes));

        spillSpaceTracker.free(mockQueryId, otherReservedBytes);
        spillSpaceTracker.free(mockQueryId2, otherReservedBytes);
        assertEquals(spillSpaceTracker.getCurrentBytes(), reservedBytes);

        spillSpaceTracker.free(mockQueryId, reservedBytes);
        assertEquals(spillSpaceTracker.getCurrentBytes(), 0);
    }

    @Test (expectedExceptions = ExceededSpillLimitException.class)
    public void testSpillOutOfSpace()
    {
        assertEquals(spillSpaceTracker.getCurrentBytes(), 0);
        spillSpaceTracker.reserve(new QueryId("test"), MAX_DATA_SIZE.toBytes() + 1);
    }
}
