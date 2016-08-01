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
    private String serverType;
    private String activeDirectoryDomain;
    private String groupDistinguishedName;
    private String userObjectClass;

    public static enum ServerType
    {
        ACTIVE_DIRECTORY,
        OPENLDAP
    }

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

    @Config("authentication.ldap.url")
    public LdapServerConfig setLdapUrl(String url)
    {
        this.ldapUrl = url;
        return this;
    }

    @Config("authentication.ldap.base-dn")
    public LdapServerConfig setBaseDistinguishedName(String baseDistinguishedName)
    {
        this.baseDistinguishedName = baseDistinguishedName;
        return this;
    }

    @Config("authentication.ldap.ad-domain")
    public LdapServerConfig setActiveDirectoryDomain(String activeDirectoryDomain)
    {
        this.activeDirectoryDomain = activeDirectoryDomain;
        return this;
    }

    @Config("authentication.ldap.enabled")
    public LdapServerConfig setAuthenticationEnabled(boolean enabled)
    {
        this.authenticationEnabled = enabled;
        return this;
    }

    public String getGroupDistinguishedName()
    {
        return groupDistinguishedName;
    }

    @Config("authentication.ldap.group-dn")
    public LdapServerConfig setGroupDistinguishedName(String groupDistinguishedName)
    {
        this.groupDistinguishedName = groupDistinguishedName;
        return this;
    }

    public String getUserObjectClass()
    {
        return userObjectClass;
    }

    @Config("authentication.ldap.user-object-class")
    public LdapServerConfig setUserObjectClass(String userObjectClass)
    {
        this.userObjectClass = userObjectClass;
        return this;
    }

    public String getServerType()
    {
        return serverType;
    }

    @Config("authentication.ldap.server-type")
    public LdapServerConfig setServerType(String serverType)
    {
        this.serverType = serverType;
        return this;
    }
}
