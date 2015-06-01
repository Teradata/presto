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

package com.facebook.presto.tests.queryinfo;

import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.server.BasicQueryInfo;

import java.util.List;
import java.util.Optional;

/**
 * Simple client for accessing Presto's /query REST interface. Implemented for testing purposes.
 */
public interface QueryInfoClient
{
    /**
     * Obtains information about all queries exposed by the Presto /query REST interface as
     * BasicQueryInfo objects.
     */
    List<BasicQueryInfo> getBasicInfoForAllQueries();

    /**
     * Obtains QueryStats for query with given id. If query does not exist or
     * does not contain queryStats information Optional.empty() is returned.
     */
    Optional<QueryStats> getQueryStats(String queryId);
}
