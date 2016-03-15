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
package com.facebook.presto.tests.utils;

import static com.teradata.tempto.query.QueryExecutor.defaultQueryExecutor;

public class JdbcDriverUtils
{
    public static boolean usingFacebookJdbcDriver()
    {
        return getClassNameForJdbcDriver().equals("com.facebook.presto.jdbc.PrestoConnection");
    }

    public static boolean usingSimbaJdbcDriver()
    {
        String className = getClassNameForJdbcDriver();
        return className.equals("com.simba.jdbc.jdbc41.S41Connection") ||
                className.equals("com.simba.jdbc.jdbc40.S40Connection") ||
                className.equals("com.teradata.jdbc.jdbc41.S41Connection") ||
                className.equals("com.teradata.jdbc.jdbc40.S40Connection");
    }

    private static String getClassNameForJdbcDriver()
    {
        return defaultQueryExecutor().getConnection().getClass().getCanonicalName();
    }

    private JdbcDriverUtils() {}
}
