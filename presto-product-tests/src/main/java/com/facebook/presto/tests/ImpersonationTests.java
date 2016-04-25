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

import static com.facebook.presto.tests.TestGroups.PROFILE_SPECIFIC_TESTS;
import static com.facebook.presto.tests.TestGroups.SINGLENODE;
import static com.facebook.presto.tests.TestGroups.SINGLENODE_HDFS_IMPERSONATION;
import static com.facebook.presto.tests.TestGroups.SINGLENODE_KERBEROS_HDFS_IMPERSONATION;
import static com.facebook.presto.tests.TestGroups.SINGLENODE_KERBEROS_HDFS_NO_IMPERSONATION;
import static com.teradata.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

@Requires(ImmutableNationTable.class)
public class ImpersonationTests
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;
    private static final String tableName = "checkTableOwner";
    private static final String tablepath = "/user/hive/warehouse/checktableowner";

    @Inject
    @Named("databases.presto.jdbc_user")
    private String jdbcuserkerberos;

    @Inject
    @Named("databases.presto.jdbc_user")
    private String jdbchdfsimpersonation;

    // Without impersonation, the table is created with user specified for
    // -DHADOOP_USER_NAME in jvm.config
    @Test(groups = {SINGLENODE, PROFILE_SPECIFIC_TESTS})
    public void testOwnerSingleNode()
            throws Exception
    {
        checkTableOwner("hive");
        query(format("DROP TABLE IF EXISTS %s", tableName));
    }

    @Test(groups = {SINGLENODE_HDFS_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testOwnerSingleNodeHdfsImpersonation()
            throws Exception
    {
        requireNonNull(jdbchdfsimpersonation, "databases.presto.jdbc_user is null");
        checkTableOwner(jdbchdfsimpersonation);
        query(format("DROP TABLE IF EXISTS %s", tableName));
    }

    @Test(groups = {SINGLENODE_KERBEROS_HDFS_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testOwnerSingleNodeKerberosHdfsImpersonation()
            throws Exception
    {
        requireNonNull(jdbcuserkerberos, "databases.presto.jdbc_user is null");
        checkTableOwner(jdbcuserkerberos);
        query(format("DROP TABLE IF EXISTS %s", tableName));
    }

    // Without hdfs impersonation, the table should be created with user specified for the
    // principal hive.hdfs.presto.principal in hive.properties
    @Test(groups = {SINGLENODE_KERBEROS_HDFS_NO_IMPERSONATION, PROFILE_SPECIFIC_TESTS})
    public void testOwnerSingleNodeKerberosHdfsNoImpersonation()
            throws Exception
    {
        checkTableOwner("hdfs");
        query(format("DROP TABLE IF EXISTS %s", tableName));
    }

    private void checkTableOwner(String expectedOwner)
    {
        query(format("DROP TABLE IF EXISTS %s", tableName));
        query(format("create table %s as select * from nation", tableName));
        String owner = hdfsClient.getOwner(tablepath);
        assertEquals(owner, expectedOwner);
    }
}
