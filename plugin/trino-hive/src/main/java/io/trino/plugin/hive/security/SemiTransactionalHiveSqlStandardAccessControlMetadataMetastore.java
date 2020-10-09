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
package io.trino.plugin.hive.security;

import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.spi.security.RoleGrant;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class SemiTransactionalHiveSqlStandardAccessControlMetadataMetastore
        implements SqlStandardAccessControlMetadataMetastore
{
    private final SemiTransactionalHiveMetastore hiveMetastore;

    public SemiTransactionalHiveSqlStandardAccessControlMetadataMetastore(SemiTransactionalHiveMetastore hiveMetastore)
    {
        this.hiveMetastore = requireNonNull(hiveMetastore, "hiveMetastore is null");
    }

    @Override
    public Set<String> listRoles()
    {
        return hiveMetastore.listRoles();
    }

    @Override
    public void createRole(String role, String grantor)
    {
        hiveMetastore.createRole(role, grantor);
    }

    @Override
    public void dropRole(String role)
    {
        hiveMetastore.dropRole(role);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return hiveMetastore.listRoleGrants(principal);
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        hiveMetastore.grantRoles(roles, grantees, adminOption, grantor);
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        hiveMetastore.revokeRoles(roles, grantees, adminOption, grantor);
    }

    @Override
    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        return hiveMetastore.listGrantedPrincipals(role);
    }

    @Override
    public void revokeTablePrivileges(HiveIdentity identity, String databaseName, String tableName, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        hiveMetastore.revokeTablePrivileges(identity, databaseName, tableName, grantee, privileges);
    }

    @Override
    public void grantTablePrivileges(HiveIdentity identity, String databaseName, String tableName, HivePrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        hiveMetastore.grantTablePrivileges(identity, databaseName, tableName, grantee, privileges);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(HiveIdentity identity, String databaseName, String tableName, Optional<HivePrincipal> principal)
    {
        return hiveMetastore.listTablePrivileges(identity, databaseName, tableName, principal);
    }
}
