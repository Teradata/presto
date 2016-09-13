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
package com.facebook.presto.sql.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShowGrants
    extends Statement
{
    public enum IdentityType
    {
        USER, ROLE;
    }

    private final Optional<IdentityType> identityType;
    private final Optional<String> identity;
    private final boolean table;
    private final Optional<QualifiedName> tableName;
    private final boolean all;

    public ShowGrants(Optional<IdentityType> identityType, Optional<String> identity, boolean table, Optional<QualifiedName> tableName, boolean all)
    {
        this(Optional.empty(), identityType, identity, table, tableName, all);
    }

    public ShowGrants(NodeLocation location, Optional<IdentityType> identityType, Optional<String> identity, boolean table, Optional<QualifiedName> tableName, boolean all)
    {
        this(Optional.of(location), identityType, identity, table, tableName, all);
    }

    public ShowGrants(Optional<NodeLocation> location, Optional<IdentityType> identityType, Optional<String> identity, boolean table, Optional<QualifiedName> tableName, boolean all)
    {
        super(location);
        requireNonNull(identity, "identity is null");
        requireNonNull(tableName, "tableName is null");

        this.identityType = identityType;
        this.table = table;
        this.tableName = tableName;
        this.identity = identity;
        this.all = all;
    }

    public Optional<IdentityType> getIdentityType()
    {
        return identityType;
    }

    public Optional<String> getIdentity()
    {
        return identity;
    }

    public boolean getAll()
    {
        return all;
    }

    public boolean getTable()
    {
        return table;
    }

    public Optional<QualifiedName> getTableName()
    {
        return tableName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowGrants(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(identityType, identity, table, tableName, all);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ShowGrants o = (ShowGrants) obj;
        return Objects.equals(identityType, o.identityType) &&
                Objects.equals(identity, o.identity) &&
                Objects.equals(table, o.table) &&
                Objects.equals(tableName, o.tableName) &&
                Objects.equals(all, o.all);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("identityType", identityType)
                .add("identity", identity)
                .add("table", table)
                .add("tableName", tableName)
                .add("all", all)
                .toString();
    }
}
