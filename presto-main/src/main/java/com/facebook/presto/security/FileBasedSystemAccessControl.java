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
package com.facebook.presto.security;

import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.security.GrantInfo;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.facebook.presto.spi.security.AccessDeniedException.denyCatalogAccess;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetUser;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public class FileBasedSystemAccessControl
        implements SystemAccessControl
{
    public static final String NAME = "file";

    private final List<CatalogAccessControlRule> catalogRules;
    private final KerberosPrincipalAccessControlRule kerberosPrincipalRule;

    private FileBasedSystemAccessControl(List<CatalogAccessControlRule> catalogRules, KerberosPrincipalAccessControlRule kerberosPrincipalRule)
    {
        this.catalogRules = catalogRules;
        this.kerberosPrincipalRule = kerberosPrincipalRule;
    }

    public static class Factory
            implements SystemAccessControlFactory
    {
        private static final String CONFIG_FILE_NAME = "security.config-file";

        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public SystemAccessControl create(Map<String, String> config)
        {
            requireNonNull(config, "config is null");

            String configFileName = config.get(CONFIG_FILE_NAME);
            checkState(
                    configFileName != null,
                    "Configuration option %s not found while loading file based system access control", CONFIG_FILE_NAME);

            try {
                Path path = Paths.get(configFileName);
                if (!path.isAbsolute()) {
                    path = path.toAbsolutePath();
                }
                FileBasedSystemAccessControlRules rules = jsonCodec(FileBasedSystemAccessControlRules.class).fromJson(Files.readAllBytes(path));
                ImmutableList.Builder<CatalogAccessControlRule> catalogRulesBuilder = ImmutableList.builder();
                catalogRulesBuilder.addAll(rules.getCatalogRules());

                // Allow all users to access the "system" catalog to facilitate debugging via
                // "select * from system.runtime.nodes", etc.
                // TODO: change userRegex from ".*" to admin, maybe
                catalogRulesBuilder.add(new CatalogAccessControlRule(
                        true,
                        Optional.of(Pattern.compile(".*")),
                        Optional.of(Pattern.compile("system"))));

                return new FileBasedSystemAccessControl(catalogRulesBuilder.build(), rules.getKerberosPrincipalRule());
            }
            catch (IOException | InvalidPathException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void checkCanSetUser(Principal principal, String userName)
    {
        // Allow unauthenticated access.
        if (principal == null) {
            return;
        }

        if (!(principal instanceof KerberosPrincipal)) {
            return;
        }

        String kerberosUserName = getKerberosUserName((KerberosPrincipal) principal);
        if (!kerberosPrincipalRule.match(userName, kerberosUserName)) {
            denySetUser(principal, userName);
        }
    }

    private String getKerberosUserName(KerberosPrincipal principal)
    {
        String realmName = principal.getRealm();
        String kerberosUserName = principal.getName();

        if (!isNullOrEmpty(realmName)) {
            kerberosUserName = kerberosUserName.substring(0, kerberosUserName.length() - realmName.length() - 1);
        }
        return kerberosUserName;
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        ImmutableSet.Builder<String> filteredCatalogs = ImmutableSet.builder();
        for (String catalog : catalogs) {
            if (canAccessCatalog(identity, catalog)) {
                filteredCatalogs.add(catalog);
            }
        }
        return filteredCatalogs.build();
    }

    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
        if (!canAccessCatalog(identity, schema.getCatalogName())) {
            denyCatalogAccess(schema.getCatalogName());
        }
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
        if (!canAccessCatalog(identity, schema.getCatalogName())) {
            denyCatalogAccess(schema.getCatalogName());
        }
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
        if (!canAccessCatalog(identity, schema.getCatalogName())) {
            denyCatalogAccess(schema.getCatalogName());
        }
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            return ImmutableSet.of();
        }
        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName())) {
            denyCatalogAccess(table.getCatalogName());
        }
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName())) {
            denyCatalogAccess(table.getCatalogName());
        }
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        if (!canAccessCatalog(identity, table.getCatalogName())) {
            denyCatalogAccess(table.getCatalogName());
        }
    }

    @Override
    public void checkCanShowTables(Identity identity, CatalogSchemaName schema)
    {
        if (!canAccessCatalog(identity, schema.getCatalogName())) {
            denyCatalogAccess(schema.getCatalogName());
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            return ImmutableSet.of();
        }
        return tableNames;
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName())) {
            denyCatalogAccess(table.getCatalogName());
        }
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName())) {
            denyCatalogAccess(table.getCatalogName());
        }
    }

    @Override
    public void checkCanSelectFromTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName())) {
            denyCatalogAccess(table.getCatalogName());
        }
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName())) {
            denyCatalogAccess(table.getCatalogName());
        }
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName())) {
            denyCatalogAccess(table.getCatalogName());
        }
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
        if (!canAccessCatalog(identity, view.getCatalogName())) {
            denyCatalogAccess(view.getCatalogName());
        }
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
        if (!canAccessCatalog(identity, view.getCatalogName())) {
            denyCatalogAccess(view.getCatalogName());
        }
    }

    @Override
    public void checkCanSelectFromView(Identity identity, CatalogSchemaTableName view)
    {
        if (!canAccessCatalog(identity, view.getCatalogName())) {
            denyCatalogAccess(view.getCatalogName());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName())) {
            denyCatalogAccess(table.getCatalogName());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(Identity identity, CatalogSchemaTableName view)
    {
        if (!canAccessCatalog(identity, view.getCatalogName())) {
            denyCatalogAccess(view.getCatalogName());
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName())) {
            denyCatalogAccess(table.getCatalogName());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table)
    {
        if (!canAccessCatalog(identity, table.getCatalogName())) {
            denyCatalogAccess(table.getCatalogName());
        }
    }

    @Override
    public void checkCanShowGrants(Identity identity, String catalogName, SchemaTablePrefix schemaTablePrefix)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public Set<GrantInfo> filterGrants(Identity identity, String catalogName, SchemaTablePrefix schemaTablePrefix, Set<GrantInfo> grantInfos)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            return ImmutableSet.of();
        }
        return grantInfos;
    }

    @Override
    public void checkCanShowRoles(Identity identity, String catalogName)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public void checkCanShowCurrentRoles(Identity identity, String catalogName)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public void checkCanShowRoleGrants(Identity identity, String catalogName)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public Set<String> filterRoles(Identity identity, String catalogName, Set<String> roles)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            return ImmutableSet.of();
        }
        return roles;
    }

    @Override
    public void checkCanCreateRole(Identity identity, String role, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public void checkCanDropRole(Identity identity, String role, String catalogName)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public void checkCanGrantRoles(Identity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public void checkCanRevokeRoles(Identity identity, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor, String catalogName)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public void checkCanSetRole(Identity identity, String role, String catalogName)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    private boolean canAccessCatalog(Identity identity, String catalogName)
    {
        for (CatalogAccessControlRule rule : catalogRules) {
            Optional<Boolean> allowed = rule.match(identity.getUser(), catalogName);
            if (allowed.isPresent()) {
                return allowed.get();
            }
        }
        return false;
    }
}
