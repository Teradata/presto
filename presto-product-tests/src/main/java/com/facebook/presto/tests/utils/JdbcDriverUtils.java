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

import com.facebook.presto.jdbc.PrestoConnection;
import io.airlift.log.Logger;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcDriverUtils
{
    private static final Logger LOGGER = Logger.get(JdbcDriverUtils.class);

    public static String getSessionProperty(Connection connection, String key) throws SQLException
    {
        return getSessionProperty(connection, key, "Value");
    }

    public static String getSessionPropertyDefault(Connection connection, String key) throws SQLException
    {
        return getSessionProperty(connection, key, "Default");
    }

    private static String getSessionProperty(Connection connection, String key, String valueType) throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SHOW SESSION");
            while (rs.next()) {
                if (rs.getString("Name").equals(key)) {
                    return rs.getString(valueType);
                }
            }
        }
        return null;
    }

    public static boolean shouldValueBeQuoted(String value)
    {
        // If value represents a number, no quotes are needed
        // If value is 'true' or 'false', no quotes are needed
        // Everything else should be enclosed in single quotes.

        if (value.equalsIgnoreCase("true") ||
                value.equalsIgnoreCase("false")) {
            return false;
        }

        if (StringUtils.isNumeric(value)) {
            return false;
        }

        if (StringUtils.isAlphanumericSpace(value)) {
            return true;
        }

        // Not particularly efficient, but it should be rare that we get here.
        // For occasional use in setting session properties for tests, this should be OK.
        try {
            new BigDecimal(value);
            return false;
        }
        catch (NumberFormatException e) {
            LOGGER.info("'%s' is not a number", value, e);
        }

        return true;
    }

    public static void setSessionProperty(Connection connection, String key, String value) throws SQLException
    {
        if (usingPrestoJdbcDriver(connection)) {
            PrestoConnection prestoConnection = connection.unwrap(PrestoConnection.class);
            prestoConnection.setSessionProperty(key, value);
        }
        else if (usingTeradataJdbcDriver(connection)) {
            try (Statement statement = connection.createStatement()) {
                if (shouldValueBeQuoted(value)) {
                    value = "'" + value + "'";
                }
                statement.execute(String.format("set session %s=%s", key, value));
            }
        }
        else {
            throw new IllegalStateException();
        }
    }

    public static void resetSessionProperty(Connection connection, String key) throws SQLException
    {
        if (usingPrestoJdbcDriver(connection)) {
            setSessionProperty(connection, key, getSessionPropertyDefault(connection, key));
        }
        else if (usingTeradataJdbcDriver(connection)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(String.format("RESET SESSION %s", key));
            }
        }
        else {
            throw new IllegalStateException();
        }
    }

    public static boolean usingPrestoJdbcDriver(Connection connection)
    {
        return connection instanceof PrestoConnection;
    }

    public static boolean usingTeradataJdbcDriver(Connection connection)
    {
        String className = getClassNameForJdbcDriver(connection);
        return className != null && className.startsWith("com.teradata.jdbc.");
    }

    public static boolean usingTeradataJdbc4Driver(Connection connection)
    {
        String className = getClassNameForJdbcDriver(connection);
        return className != null && className.startsWith("com.teradata.jdbc.jdbc4.");
    }

    private static String getClassNameForJdbcDriver(Connection connection)
    {
        return connection.getClass().getCanonicalName();
    }

    private JdbcDriverUtils() {}
}
