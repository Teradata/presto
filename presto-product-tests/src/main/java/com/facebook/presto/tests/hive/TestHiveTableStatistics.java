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
package com.facebook.presto.tests.hive;

import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.Requires;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_REGIONKEY;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.context.ThreadLocalTestContextHolder.testContext;
import static com.teradata.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static com.teradata.tempto.fulfillment.table.TableRequirements.mutableTable;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static com.teradata.tempto.query.QueryExecutor.query;

public class TestHiveTableStatistics
        extends ProductTest
{
    private static class UnpartitionedNationTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return mutableTable(NATION);
        }
    }

    private static class PartitionedNationTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return mutableTable(NATION_PARTITIONED_BY_REGIONKEY);
        }
    }

    @Test(groups = {HIVE_CONNECTOR})
    @Requires(UnpartitionedNationTable.class)
    public void testStatisticsForUnpartitionedTable()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;

        // table not analyzed

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("n_nationkey", null, null, null, null),
                row("n_name", null, null, null, null),
                row("n_regionkey", null, null, null, null),
                row("n_comment", null, null, null, null),
                row(null, null, null, null, null));

        // basic analysis

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("n_nationkey", null, null, null, null),
                row("n_name", null, null, null, null),
                row("n_regionkey", null, null, null, null),
                row("n_comment", null, null, null, null),
                row(null, null, null, null, 25.0));

        // column analysis

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " COMPUTE STATISTICS FOR COLUMNS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("n_nationkey", null, 19.0, 0.0, null),
                row("n_name", null, 24.0, 0.0, null),
                row("n_regionkey", null, 5.0, 0.0, null),
                row("n_comment", null, 31.0, 0.0, null),
                row(null, null, null, null, 25.0));
    }

    @Test(groups = {HIVE_CONNECTOR})
    @Requires(PartitionedNationTable.class)
    public void testStatisticsForPartitionedTable()
    {
        String tableNameInDatabase = mutableTablesState().get(NATION_PARTITIONED_BY_REGIONKEY.getName()).getNameInDatabase();

        String showStatsWholeTable = "SHOW STATS FOR " + tableNameInDatabase;
        String showStatsPartitionOne = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 1)";
        String showStatsPartitionTwo = "SHOW STATS FOR (SELECT * FROM " + tableNameInDatabase + " WHERE p_regionkey = 2)";

        // table not analyzed

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null),
                row("p_name", null, null, null, null),
                row("p_regionkey", null, 3.0, null, null),
                row("p_comment", null, null, null, null),
                row(null, null, null, null, null));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null),
                row("p_name", null, null, null, null),
                row("p_regionkey", null, 1.0, null, null),
                row("p_comment", null, null, null, null),
                row(null, null, null, null, null));

        // basic analysis for single partition

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey = \"1\") COMPUTE STATISTICS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null),
                row("p_name", null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null),
                row("p_comment", null, null, null, null),
                row(null, null, null, null, 15.0));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null),
                row("p_name", null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null),
                row("p_comment", null, null, null, null),
                row(null, null, null, null, 5.0));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null),
                row("p_name", null, null, null, null),
                row("p_regionkey", null, 1.0, null, null),
                row("p_comment", null, null, null, null),
                row(null, null, null, null, null));

        // basic analysis for all partitions

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey) COMPUTE STATISTICS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, null, null, null),
                row("p_name", null, null, null, null),
                row("p_regionkey", null, 3.0, 0.0, null),
                row("p_comment", null, null, null, null),
                row(null, null, null, null, 15.0));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, null, null, null),
                row("p_name", null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null),
                row("p_comment", null, null, null, null),
                row(null, null, null, null, 5.0));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null),
                row("p_name", null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null),
                row("p_comment", null, null, null, null),
                row(null, null, null, null, 5.0));

        // column analysis for single partition

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey = \"1\") COMPUTE STATISTICS FOR COLUMNS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null),
                row("p_name", null, 6.0, 0.0, null),
                row("p_regionkey", null, 3.0, 0.0, null),
                row("p_comment", null, 1.0, 0.0, null),
                row(null, null, null, null, 15.0));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null),
                row("p_name", null, 6.0, 0.0, null),
                row("p_regionkey", null, 1.0, 0.0, null),
                row("p_comment", null, 1.0, 0.0, null),
                row(null, null, null, null, 5.0));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, null, null, null),
                row("p_name", null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null),
                row("p_comment", null, null, null, null),
                row(null, null, null, null, 5.0));

        // column analysis for all partitions

        onHive().executeQuery("ANALYZE TABLE " + tableNameInDatabase + " PARTITION (p_regionkey) COMPUTE STATISTICS FOR COLUMNS");

        assertThat(query(showStatsWholeTable)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null),
                row("p_name", null, 6.0, 0.0, null),
                row("p_regionkey", null, 3.0, 0.0, null),
                row("p_comment", null, 1.0, 0.0, null),
                row(null, null, null, null, 15.0));

        assertThat(query(showStatsPartitionOne)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null),
                row("p_name", null, 6.0, 0.0, null),
                row("p_regionkey", null, 1.0, 0.0, null),
                row("p_comment", null, 1.0, 0.0, null),
                row(null, null, null, null, 5.0));

        assertThat(query(showStatsPartitionTwo)).containsOnly(
                row("p_nationkey", null, 4.0, 0.0, null),
                row("p_name", null, 6.0, 0.0, null),
                row("p_regionkey", null, 1.0, 0.0, null),
                row("p_comment", null, 1.0, 0.0, null),
                row(null, null, null, null, 5.0));
    }

    private static QueryExecutor onHive()
    {
        return testContext().getDependency(QueryExecutor.class, "hive");
    }
}
