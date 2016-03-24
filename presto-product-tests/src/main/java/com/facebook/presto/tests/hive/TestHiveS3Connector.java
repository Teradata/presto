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
import com.teradata.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static com.facebook.presto.tests.TestGroups.QUARANTINE;
import static com.facebook.presto.tests.TestGroups.S3_CONNECTOR;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.query.QueryExecutor.query;

public class TestHiveS3Connector
        extends ProductTest
{
    @Test(groups = {S3_CONNECTOR, QUARANTINE})
    public void testSelectFromTextFile()
            throws SQLException
    {
        QueryResult queryResult = query("SELECT n_name FROM hive.default.nation where n_nationkey = 7");
        assertThat(queryResult).containsOnly(row("GERMANY"));
    }
}
