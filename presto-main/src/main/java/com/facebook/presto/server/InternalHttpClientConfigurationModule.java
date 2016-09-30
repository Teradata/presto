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
package com.facebook.presto.server;

import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.matcher.AbstractMatcher;
import com.google.inject.spi.ProvisionListener;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.client.HttpClientConfig;

import java.nio.file.Paths;

import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.Files.isReadable;

public class InternalHttpClientConfigurationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        InternalHttpClientConfiguration configuration = buildConfigObject(InternalHttpClientConfiguration.class);
        binder.bindListener(new HttpClientConfigMatcher(), new ConfigurationListener(configuration));
    }

    private static class HttpClientConfigMatcher
            extends AbstractMatcher<Binding<?>>
    {
        @Override
        public boolean matches(Binding<?> binding)
        {
            return binding.getKey().getTypeLiteral().getRawType().equals(HttpClientConfig.class);
        }
    }

    private static class ConfigurationListener
            implements ProvisionListener
    {
        private final InternalHttpClientConfiguration configuration;

        private ConfigurationListener(InternalHttpClientConfiguration configuration)
        {
            this.configuration = configuration;
        }

        @Override
        public <T> void onProvision(ProvisionInvocation<T> provision)
        {
            HttpClientConfig config = checkType(provision.provision(), HttpClientConfig.class, "config");
            checkState(config.getKeyStorePath() == null);
            checkState(config.getKeyStorePassword() == null);
            String keyStorePath = configuration.getKeyStorePath();
            if (keyStorePath != null) {
                checkState(isReadable(Paths.get(keyStorePath)), "Keystore is not readable: %s", keyStorePath);
                config.setKeyStorePath(keyStorePath);
            }
            config.setKeyStorePassword(configuration.getKeyStorePassword());
        }
    }
}
