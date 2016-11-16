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

import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;

import java.util.BitSet;
import java.util.List;

import static com.facebook.presto.spi.type.StandardTypes.BIGINT;

public final class GroupingOperationFunction
{
    private GroupingOperationFunction() {}

    /**
     * The grouping function is used in conjunction with GROUPING SETS, ROLLUP and CUBE to
     * indicate which columns are present in that grouping.
     *
     * <p>The grouping function must be invoked with arguments that exactly match the columns
     * referenced in the corresponding GROUPING SET, ROLLUP or CUBE clause at the associated
     * query level. Those column arguments are not evaluated and instead the function is
     * re-written with the arguments below.
     *
     * <p>To compute the resulting bit set for a particular row, bits are assigned to the
     * argument columns with the rightmost column being the most significant bit. For a
     * given grouping, a bit is set to 0 if the corresponding column is included in the
     * grouping and 1 otherwise. For an example, see the SQL documentation for the
     * function.
     *
     * @param groupId An ordinal indicating which grouping is currently being processed.
     *        Each grouping is assigned a unique monotonically increasing integer.
     * @param groupingOrdinals The column arguments with which the function was
     *        invoked converted to ordinals with respect to the base table column
     *        ordering.
     * @param groupingSetOrdinals A collection of ordinal lists where the index of
     *        the list is the groupId and the list itself contains the ordinals of the
     *        columns present in the grouping. For example: [[0, 2], [2], [0, 1, 2]]
     *        means the the 0th list contains the set of columns that are present in
     *        the 0th grouping.
     * @return A bit set converted to decimal indicating which columns are present in
     *         the grouping.
     *
     */
    @ScalarFunction(deterministic = false)
    @SqlType(StandardTypes.INTEGER)
    public static long grouping(
            @SqlType(BIGINT) long groupId,
            @SqlType("ListLiteral") List groupingOrdinals,
            @SqlType("ListLiteral") List groupingSetOrdinals)
    {
        BitSet groupingBinary = new BitSet(groupingOrdinals.size());
        int startIndex = 0;
        groupingBinary.set(startIndex, groupingOrdinals.size(), true);

        List<Integer> groupingSet = (List<Integer>) groupingSetOrdinals.get((int) groupId);
        for (Integer groupingColumn : groupingSet) {
            if (groupingOrdinals.contains(groupingColumn)) {
                groupingBinary.clear(groupingOrdinals.indexOf(groupingColumn));
            }
        }

        long grouping = 0;
        // Rightmost argument to grouping() is represented by the least significant bit
        // so we start the conversion from binary to decimal from the left.
        for (int i = groupingOrdinals.size() - 1, j = 0; i >= 0; i--, j++) {
            if (groupingBinary.get(i)) {
                grouping += 1 << j;
            }
        }

        return grouping;
    }
}
