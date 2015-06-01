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
package com.facebook.presto.tests;

import com.facebook.presto.tests.ImmutableTpchTablesRequirements.ImmutableNationTable;
import com.facebook.presto.tests.utils.PrestoDDLUtils.Table;
import com.google.common.collect.ImmutableMap;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requires;
import com.teradata.tempto.query.QueryResult;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.tests.TestGroups.CREATE_TABLE;
import static com.facebook.presto.tests.TestGroups.QUARANTINE;
import static com.facebook.presto.tests.TestGroups.SMOKE;
import static com.facebook.presto.tests.utils.PrestoDDLUtils.createPrestoTable;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.collect.Iterables.getFirst;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.query.QueryExecutor.query;
import static com.teradata.tempto.query.QueryType.SELECT;
import static java.lang.String.format;
import static org.assertj.core.data.MapEntry.entry;

@Requires(ImmutableNationTable.class)
public class CreateTableTests
        extends ProductTest
{
    private static final String TABLE_PARAMETERS = "Table Parameters:";
    private static final String SERDE_PARAMETERS = "Storage Desc Params:";

    @Test(groups = CREATE_TABLE)
    public void shouldCreateTableAsSelect()
            throws Exception
    {
        String tableName = "create_table_as_select";
        try (Table table = createPrestoTable(tableName, "CREATE TABLE %s AS SELECT * FROM nation")) {
            assertThat(query(format("SELECT * FROM %s", table.getNameInDatabase()))).hasRowsCount(25);
        }
    }

    @Test(groups = CREATE_TABLE)
    public void shouldCreateTableAsEmptySelect()
            throws Exception
    {
        String tableName = "create_table_as_empty_select";
        try (Table table = createPrestoTable(tableName, "CREATE TABLE %s AS SELECT * FROM nation WHERE 0 is NULL")) {
            assertThat(query(format("SELECT * FROM %s", table.getNameInDatabase()))).hasRowsCount(0);
        }
    }

    @Test(groups = {CREATE_TABLE, QUARANTINE})
    // quarantined. unquarantine after addin table parameters mechanism compatible with Dain's insert patch.
    public void shouldCreateTableWithParameters()
            throws Exception
    {
        testCreateTableWithParameters("dummy_table_with_parameters", "CREATE TABLE %s (a VARCHAR)");
    }

    @Test(groups = {CREATE_TABLE, SMOKE, QUARANTINE})
    // quarantined. unquarantine after addin table parameters mechanism compatible with Dain's insert patch.
    public void shouldCreateTableAsSelectWithParameters()
            throws Exception
    {
        testCreateTableWithParameters("create_table_with_table_properties", "CREATE TABLE %s AS SELECT * FROM nation");
    }

    private void testCreateTableWithParameters(String tableName, String tableDDL)
            throws IOException
    {
        Map<String, String> sessionProperties = createSessionPropertiesWithTableParameters();
        try (Table table = createPrestoTable(tableName, tableDDL, sessionProperties)) {
            QueryResult describeResult = onHive().executeQuery(format("DESCRIBE FORMATTED %s", table.getNameInDatabase()), SELECT);
            assertTableParameters(describeResult);
        }
    }

    private Map<String, String> createSessionPropertiesWithTableParameters()
    {
        return ImmutableMap.<String, String>builder()
                .put("hive.table_parameters.table_property_1", "table_property_1_value")
                .put("hive.table_parameters.table_property_2", "table_property_2_value")
                .put("hive.serde_parameters.serde_property_1", "serde_property_1_value")
                .put("hive.serde_parameters.serde_property_2", "serde_property_2_value")
                .build();
    }

    private void assertTableParameters(QueryResult describeResult)
    {
        Assertions.assertThat(extractParameters(describeResult, TABLE_PARAMETERS)).contains(
                entry("table_property_1", "table_property_1_value"),
                entry("table_property_2", "table_property_2_value")
        );

        Assertions.assertThat(extractParameters(describeResult, SERDE_PARAMETERS)).contains(
                entry("serde_property_1", "serde_property_1_value"),
                entry("serde_property_2", "serde_property_2_value")
        );
    }

    private Map<String, String> extractParameters(QueryResult describeResult, String header)
    {
        ImmutableMap.Builder<String, String> result = ImmutableMap.builder();

        boolean parametersFound = false;
        for (List<Object> row : describeResult.rows()) {
            if (parametersFound) {
                checkPositionIndexes(0, 2, row.size());
                if (!Objects.toString(getFirst(row, "")).equalsIgnoreCase("")) {
                    break;
                }
                result.put(Objects.toString(row.get(1)).trim(), Objects.toString(row.get(2)).trim());
            }

            if (Objects.toString(getFirst(row, "")).equalsIgnoreCase(header)) {
                parametersFound = true;
            }
        }

        Assertions.assertThat(parametersFound).isTrue();
        return result.build();
    }
}
