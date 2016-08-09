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

import javax.inject.Inject;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ActiveDirectoryBinder
        implements LdapBinder
{
    private final String activeDirectoryDomain;

    @Inject
    public ActiveDirectoryBinder(LdapServerConfig config)
    {
        activeDirectoryDomain = requireNonNull(config.getActiveDirectoryDomain(), "activeDirectoryDomain is null");
    }

    @Override
    public String getBindDistinguishedName(String user)
    {
        return format("%s@%s", user, activeDirectoryDomain);
    }

    @Override
    public String getUserSearchInput()
    {
        return "sAMAccountName";
    }
}
