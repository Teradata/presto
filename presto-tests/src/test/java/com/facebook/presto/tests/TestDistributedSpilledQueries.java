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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;

import java.nio.file.Paths;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestDistributedSpilledQueries
        extends AbstractTestQueries
{
    public TestDistributedSpilledQueries()
            throws Exception
    {
        super(TestDistributedSpilledQueries::createQueryRunner);
    }

    private static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        // TODO this test doesn't force any spill. :/

        Session defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(SystemSessionProperties.TASK_CONCURRENCY, "2")
                .setSystemProperty(SystemSessionProperties.SPILL_ENABLED, "true")
                .setSystemProperty(SystemSessionProperties.AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT, "128kB")
                .build();

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(defaultSession, 2, ImmutableMap.of(
                "experimental.spiller-spill-path", Paths.get(System.getProperty("java.io.tmpdir"), "presto", "spills").toString(),
                "experimental.spiller-max-used-space-threshold", "1.0"
        ), ImmutableMap.of(), new SqlParserOptions());

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }
}
