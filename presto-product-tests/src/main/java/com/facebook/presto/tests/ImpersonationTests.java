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
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requires;
import com.teradata.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.*;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

@Requires(ImmutableNationTable.class)
public class ImpersonationTests
        extends ProductTest {
    @Inject(optional = true)
    private HdfsClient hdfsClient;

    @Inject(optional = true)
    @Named("databases.presto.jdbc_user")
    private String prestoJdbcUser;

    @Inject(optional = true)
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectoryPath;

    private String getTableLocation(String tablename) {
        requireNonNull(warehouseDirectoryPath, "databases.hive.warehouse_directory_path is null");
        requireNonNull(tablename, "Input tablename is null");
        return warehouseDirectoryPath + tablename;
    }

    private void checkTableOwner(String tablename, String tablepath, String expectedOwner) {
        query(format("DROP TABLE IF EXISTS %s", tablename));
        query(format("CREATE TABLE %s AS SELECT * FROM NATION", tablename));
        String owner = hdfsClient.getOwner(tablepath);
        assertEquals(owner, expectedOwner);
        query(format("DROP TABLE IF EXISTS %s", tablename));
    }

    // Without impersonation, the table is created with user specified for
    // -DHADOOP_USER_NAME in jvm.config
    @Test(groups = {SINGLENODE, PROFILE_SPECIFIC_TESTS})
    public void testOwnerSingleNode()
            throws Exception {
        String tableName = "check_owner_singlenode";
        String tableLocation = getTableLocation(tableName);
        checkTableOwner(tableName, tableLocation, "hive");
    }

    @Test(groups = {SINGLENODE_HDFS_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testOwnerSingleNodeHdfsImpersonation()
            throws Exception {
        String tableName = "check_owner_singlenode_hdfs_impersonation";
        requireNonNull(prestoJdbcUser, "databases.presto.jdbc_user is null");
        String tableLocation = getTableLocation(tableName);
        checkTableOwner(tableName, tableLocation, prestoJdbcUser);
    }

    @Test(groups = {SINGLENODE_KERBEROS_HDFS_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testOwnerSingleNodeKerberosHdfsImpersonation()
            throws Exception {
        String tableName = "check_owner_singlenode_kerberos_hdfs_impersonation";
        requireNonNull(prestoJdbcUser, "databases.presto.jdbc_user is null");
        String tableLocation = getTableLocation(tableName);
        checkTableOwner(tableName, tableLocation, prestoJdbcUser);
    }

    // Without hdfs impersonation, the table should be created with user specified for the
    // principal hive.hdfs.presto.principal in hive.properties
    @Test(groups = {SINGLENODE_KERBEROS_HDFS_NO_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testOwnerSingleNodeKerberosHdfsNoImpersonation()
            throws Exception {
        String tableName = "check_owner_singlenode_kerberos_hdfs_no_impersonation";
        String tableLocation = getTableLocation(tableName);
        checkTableOwner(tableName, tableLocation, "hdfs");
    }
}
