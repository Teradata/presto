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
import com.teradata.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.QUARANTINE;
import static com.facebook.presto.tests.utils.QueryExecutors.connectToPresto;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static java.lang.String.format;

public class TestGrantRevoke
    extends ProductTest
{
    /* This test has been quarantined since hive.security property is NOT set by default and
     * doing so may affect other tests.
     *
     * Pre-requisites for the test:
     * (1) In hive.properties file, set this property: hive.security=sql-standard;
     * (2) tempto-configuration.yaml file should have two connections to Presto server:
     * "prestoConfigUserAlice" that has "jdbc_user: alice" and
     * "prestoConfigUserBob" that has "jdbc_user: bob"
     * (all other values of the connection are same as the default "presto" connection).
     * You can get these configurations from presto-product-tests/conf/tempto/tempto-configuration.yaml file.
     *
    */
    @Test(groups = {HIVE_CONNECTOR, QUARANTINE})
    public void testGrantRevoke()
    {
        String tableName = "alice_owned_table";
        QueryExecutor queryExecutorForAlice = connectToPresto("prestoConfigUserAlice");
        QueryExecutor queryExecutorForBob = connectToPresto("prestoConfigUserBob");

        queryExecutorForAlice.executeQuery(format("DROP TABLE IF EXISTS %s", tableName));
        queryExecutorForAlice.executeQuery(format("CREATE TABLE %s(month varchar, day bigint)", tableName));
        assertThat(queryExecutorForAlice.executeQuery(format("SELECT * from %s", tableName))).hasNoRows();
        assertThat(() -> queryExecutorForBob.executeQuery(format("SELECT * from %s", tableName))).
                failsWithMessage(format("Access Denied: Cannot select from table default.%s", tableName));

        //test GRANT
        queryExecutorForAlice.executeQuery(format("GRANT INSERT, SELECT ON %s to bob", tableName));
        assertThat(queryExecutorForBob.executeQuery(format("INSERT INTO %s values ('t', 3)", tableName))).hasRowsCount(1);
        assertThat(queryExecutorForBob.executeQuery(format("SELECT * from %s", tableName))).hasRowsCount(1);
        assertThat(() -> queryExecutorForBob.executeQuery(format("DELETE from %s WHERE day=3", tableName))).
                failsWithMessage(format("Access Denied: Cannot delete from table default.%s", tableName));

        //test REVOKE
        queryExecutorForAlice.executeQuery(format("REVOKE INSERT on %s from bob", tableName));
        assertThat(() -> queryExecutorForBob.executeQuery(format("INSERT into %s values ('y', 5)", tableName))).
                failsWithMessage(format("Access Denied: Cannot insert into table default.%s", tableName));
        assertThat(queryExecutorForBob.executeQuery(format("SELECT * from %s", tableName))).hasRowsCount(1);
    }
}
