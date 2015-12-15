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
package com.facebook.presto.plugin.teradata;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.teradata.jdbc.TeraDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by charles on 6/12/15.
 */

public class TeradataClient
        extends BaseJdbcClient
{
    @Inject
    public TeradataClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
            throws SQLException
    {
        super(connectorId, config, "\"", new TeraDriver());
    }

    @Override
    public Statement getStatement(Connection connection)
            throws SQLException
    {
        connection.setAutoCommit(false);
        Statement statement = connection.createStatement();
        statement.setFetchSize(1000);
        return statement;
    }
}
