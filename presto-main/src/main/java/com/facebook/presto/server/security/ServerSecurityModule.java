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
package com.facebook.presto.server.security;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.TheServlet;

import javax.servlet.Filter;

import static com.facebook.presto.server.security.LdapServerConfig.ServerType.ACTIVE_DIRECTORY;
import static com.facebook.presto.server.security.LdapServerConfig.ServerType.OPENLDAP;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.String.format;

public class ServerSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(LdapServerConfig.class);
        LdapServerConfig ldapServerConfig = buildConfigObject(LdapServerConfig.class);

        configBinder(binder).bindConfig(SecurityConfig.class);
        SecurityConfig config = buildConfigObject(SecurityConfig.class);

        checkState(!(ldapServerConfig.getAuthenticationEnabled() && config.getAuthenticationEnabled()), "Enabling both Kerberos and LDAP authentication not supported");

        if (ldapServerConfig.getAuthenticationEnabled()) {
            Multibinder.newSetBinder(binder, Filter.class, TheServlet.class)
                    .addBinding()
                    .to(LdapFilter.class)
                    .in(Scopes.SINGLETON);
            setupLdap(ldapServerConfig, binder);
        }

        if (config.getAuthenticationEnabled()) {
            Multibinder.newSetBinder(binder, Filter.class, TheServlet.class)
                    .addBinding()
                    .to(SpnegoFilter.class)
                    .in(Scopes.SINGLETON);
        }
    }

    private void setupLdap(LdapServerConfig ldapServerConfig, Binder binder)
    {
        checkState(ldapServerConfig.getServerType() != null, "Missing property 'authentication.ldap.server-type'");

        if (ldapServerConfig.getGroupDistinguishedName() != null) {
            checkState(ldapServerConfig.getBaseDistinguishedName() != null, "Missing property 'authentication.ldap.base-dn'");
            checkState(ldapServerConfig.getUserObjectClass() != null, "Missing property 'authentication.ldap.user-object-class'");
        }

        if (ldapServerConfig.getServerType().equalsIgnoreCase(OPENLDAP.name())) {
            checkState(ldapServerConfig.getBaseDistinguishedName() != null, "Missing property 'authentication.ldap.base-dn'");

            binder.bind(LdapBinder.class).to(OpenLdapBinder.class).in(Scopes.SINGLETON);
        }
        else if (ldapServerConfig.getServerType().equalsIgnoreCase(ACTIVE_DIRECTORY.name())) {
            checkState(ldapServerConfig.getActiveDirectoryDomain() != null, "Missing property 'authentication.ldap.ad-domain'");

            binder.bind(LdapBinder.class).to(ActiveDirectoryBinder.class).in(Scopes.SINGLETON);
        }
        else {
            throw new IllegalStateException(format("Invalid value '%s' for the property 'authentication.ldap.server-type'", ldapServerConfig.getServerType()));
        }
    }
}
