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
package com.facebook.presto.tests;

import com.google.common.collect.ImmutableMap;
import com.teradata.tempto.fulfillment.ldap.LdapObjectDefinition;

import java.util.Arrays;
import java.util.Optional;

import static java.lang.String.format;

public final class ImmutableLdapObjectDefinitions
{
    private static final String domain = "dc=presto,dc=testldap,dc=com";
    private static final String americaOrgDistinguishedName = format("ou=America,%s", domain);
    private static final String asiaOrgDistinguishedName = format("ou=Asia,%s", domain);
    private static final String password = "LDAPPass123";

    private ImmutableLdapObjectDefinitions()
    {}

    public static final LdapObjectDefinition AMERICA_ORG = buildLdapOrganizationObject("America", americaOrgDistinguishedName, "America");

    public static final LdapObjectDefinition ASIA_ORG = buildLdapOrganizationObject("Asia", asiaOrgDistinguishedName, "Asia");

    public static final LdapObjectDefinition DEFAULT_GROUP = buildLdapGroupObject("DefaultGroup", "DefaultGroupUser");

    public static final LdapObjectDefinition PARENT_GROUP = buildLdapGroupObject("ParentGroup", "ParentGroupUser");

    public static final LdapObjectDefinition CHILD_GROUP = buildLdapGroupObject("ChildGroup", "ChildGroupUser");

    public static final LdapObjectDefinition DEFAULT_GROUP_USER = buildLdapUserObject("DefaultGroupUser", Optional.of("DefaultGroup"));

    public static final LdapObjectDefinition PARENT_GROUP_USER = buildLdapUserObject("ParentGroupUser", Optional.of("ParentGroup"));

    public static final LdapObjectDefinition CHILD_GROUP_USER = buildLdapUserObject("ChildGroupUser", Optional.of("ChildGroup"));

    public static final LdapObjectDefinition ORPHAN_USER = buildLdapUserObject("OrphanUser", Optional.empty());

    private static LdapObjectDefinition buildLdapOrganizationObject(String id, String distinguishedName, String unit)
    {
        return LdapObjectDefinition.builder(id)
                .setDistinguishedName(distinguishedName)
                .setAttributes(ImmutableMap.of("ou", unit))
                .setObjectClasses(Arrays.asList("top", "organizationalUnit"))
                .build();
    }

    private static LdapObjectDefinition buildLdapGroupObject(String groupName, String userName)
    {
        return buildLdapGroupObject(groupName, americaOrgDistinguishedName, userName, asiaOrgDistinguishedName);
    }

    private static LdapObjectDefinition buildLdapGroupObject(String groupName, String groupOrganizationName,
                                                             String userName, String userOrganizationName)
    {
        return LdapObjectDefinition.builder(groupName)
                .setDistinguishedName(format("cn=%s,%s", groupName, groupOrganizationName))
                .setAttributes(ImmutableMap.of(
                        "cn", groupName,
                        "member", format("uid=%s,%s", userName, userOrganizationName)))
                .setObjectClasses(Arrays.asList("groupOfNames"))
                .build();
    }

    private static LdapObjectDefinition buildLdapUserObject(String userName, Optional<String> groupName)
    {
        if (groupName.isPresent()) {
            return buildLdapUserObject(userName, asiaOrgDistinguishedName,
                    groupName, Optional.of(americaOrgDistinguishedName));
        }
        else {
            return buildLdapUserObject(userName, asiaOrgDistinguishedName,
                    Optional.empty(), Optional.empty());
        }
    }

    private static LdapObjectDefinition buildLdapUserObject(String userName, String userOrganizationName,
                                                            Optional<String> groupName, Optional<String> groupOrganizationName)
    {
        if (groupName.isPresent() && groupOrganizationName.isPresent()) {
            return LdapObjectDefinition.builder(userName)
                    .setDistinguishedName(format("uid=%s,%s", userName, userOrganizationName))
                    .setAttributes(ImmutableMap.of(
                            "cn", userName,
                            "sn", userName,
                            "userPassword", password,
                            "memberOf", format("cn=%s,%s", groupName.get(), groupOrganizationName.get())
                    ))
                    .setObjectClasses(Arrays.asList("person", "inetOrgPerson"))
                    .build();
        }
        else {
            return LdapObjectDefinition.builder(userName)
                    .setDistinguishedName(format("uid=%s,%s", userName, userOrganizationName))
                    .setAttributes(ImmutableMap.of(
                            "cn", userName,
                            "sn", userName,
                            "userPassword", password
                    ))
                    .setObjectClasses(Arrays.asList("person", "inetOrgPerson"))
                    .build();
        }
    }
}
