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

import org.testng.annotations.Test;

import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;

public class TestTpchDistributedQueries
        extends AbstractTestQueries
{
    public TestTpchDistributedQueries()
            throws Exception
    {
        super(createQueryRunner());
    }

    @Test
    public void testFoo()
    {
        assertQuery("SELECT null IN (SELECT cast(NULL AS BIGINT))");
        assertQuery("SELECT null IN (SELECT cast(null AS BIGINT) where false)");
        assertQuery("SELECT null IN (SELECT 1)");
        assertQuery("SELECT null IN (SELECT 1 where false)");
        assertQuery("select null in ((select 1) union all (select null))");
        assertQuery("select x in (select true) from (select * from (values cast(null as boolean)) t(x) where (x or null) is null)", "select null");
        assertQuery("select x in (select 1) from (select * from (values cast(null as integer)) t(x) where (x + 10 is null) or x = 2)", "select null");
        assertQuery("select x in (select 1) from (select * from (values cast(null as integer)) t(x) where (x + 10 is null) or x = 1)", "select null");
        assertQuery("select x in (select true) from (select * from (values cast(null as boolean)) t(x) where x is null)", "select null");
    }
}
