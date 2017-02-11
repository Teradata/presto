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

import io.airlift.configuration.Config;

public class LdapServerConfig
{
    private String ldapUrl;
    private String baseDistinguishedName;
    private boolean authenticationEnabled;
    private String activeDirectoryDomain;

    public String getLdapUrl()
    {
        return ldapUrl;
    }

    public String getBaseDistinguishedName()
    {
        return baseDistinguishedName;
    }

    public String getActiveDirectoryDomain()
    {
        return activeDirectoryDomain;
    }

    public boolean getAuthenticationEnabled()
    {
        return authenticationEnabled;
    }

    @Config("ldap.url")
    public LdapServerConfig setLdapUrl(String url)
    {
        this.ldapUrl = url;
        return this;
    }

    @Config("ldap.base.dn")
    public LdapServerConfig setBaseDistinguishedName(String baseDistinguishedName)
    {
        this.baseDistinguishedName = baseDistinguishedName;
        return this;
    }

    @Config("ldap.ad.domain")
    public LdapServerConfig setActiveDirectoryDomain(String activeDirectoryDomain)
    {
        this.activeDirectoryDomain = activeDirectoryDomain;
        return this;
    }

    @Config("ldap.authentication.enabled")
    public LdapServerConfig setAuthenticationEnabled(boolean enabled)
    {
        this.authenticationEnabled = enabled;
        return this;
    }
}
