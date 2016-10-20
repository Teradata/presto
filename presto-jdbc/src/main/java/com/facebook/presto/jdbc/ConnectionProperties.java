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
package com.facebook.presto.jdbc;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;

public class ConnectionProperties
{
    public static final ConnectionProperty<String> USER = new User();
    public static final ConnectionProperty<String> PASSWORD = new Password();
    public static final ConnectionProperty<Integer> SSL = new Ssl();
    public static final ConnectionProperty<String> SSL_TRUST_STORE_PATH = new SslTrustStorePath();
    public static final ConnectionProperty<String> SSL_TRUST_STORE_PASSWORD = new SslTrustStorePassword();

    private static final Set<ConnectionProperty> ALL_OF = ImmutableSet.of(
            USER,
            PASSWORD,
            SSL,
            SSL_TRUST_STORE_PATH,
            SSL_TRUST_STORE_PASSWORD);

    private static final Map<String, ConnectionProperty> keyLookup = unmodifiableMap(allOf()
                .stream()
                .collect(Collectors.toMap(ConnectionProperty::getKey, identity())));

    private static final Map<String, String> DEFAULTS;

    static {
        ImmutableMap.Builder<String, String> defaults = ImmutableMap.builder();
        for (ConnectionProperty property : ALL_OF) {
            Optional<String> value = property.getDefault();
            if (value.isPresent()) {
                defaults.put(property.getKey(), value.get());
            }
        }
        DEFAULTS = defaults.build();
    }

    private ConnectionProperties()
    {
    }

    public static ConnectionProperty forKey(String propertiesKey)
    {
        return keyLookup.get(propertiesKey);
    }

    public static Set<ConnectionProperty> allOf()
    {
        return ALL_OF;
    }

    public static Map<String, String> getDefaults()
    {
        return DEFAULTS;
    }

    private static class User
            extends BaseConnectionProperty<String>
    {
        private User()
        {
            super("user", REQUIRED, STRING_CONVERTER);
        }
    }

    private static class Password
            extends BaseConnectionProperty<String>
    {
        private Password()
        {
            super("password", NOT_REQUIRED, STRING_CONVERTER);
        }
    }

    public static final int SSL_DISABLED = 0;
    public static final int SSL_ENABLED = 1;
    private static class Ssl
            extends BaseConnectionProperty<Integer>
    {
        private static final ImmutableSet<Integer> allowed = ImmutableSet.of(SSL_DISABLED, SSL_ENABLED);

        private Ssl()
        {
            super("SSL", Optional.of(Integer.toString(SSL_DISABLED)), NOT_REQUIRED, INTEGER_CONVERTER);
        }

        @Override
        public void validate(Properties properties)
                throws SQLException
        {
            Optional<Integer> flag = getValue(properties);
            if (!flag.isPresent()) {
                return;
            }

            if (!allowed.contains(flag.get())) {
                throw new SQLException(format("The value of %s must be one of %s", getKey(), Joiner.on(", ").join(allowed)));
            }
        }
    }

    public static class SslTrustStorePath
            extends BaseConnectionProperty<String>
    {
        private SslTrustStorePath()
        {
            super("SSLTrustStorePath", dependsKeyValue(SSL, SSL_ENABLED), STRING_CONVERTER);
        }
    }

    public static class SslTrustStorePassword
            extends BaseConnectionProperty<String>
    {
        private SslTrustStorePassword()
        {
            super("SSLTrustStorePassword", dependsKeyValue(SSL, SSL_ENABLED), STRING_CONVERTER);
        }
    }
}
