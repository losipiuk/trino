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
package io.prestosql.tests.hive;

import com.google.common.net.HostAndPort;
import io.prestosql.plugin.hive.metastore.thrift.NoHiveMetastoreAuthentication;
import io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient;
import io.prestosql.plugin.hive.metastore.thrift.Transport;
import io.prestosql.tests.utils.QueryExecutors;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.thrift.TException;

import javax.inject.Inject;
import javax.inject.Named;

import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;

public final class ACIDTestHelper
{
    private static final String LOCALHOST = "localhost";

    @Inject
    @Named("databases.hive.metastore.host")
    private String metastoreHost;

    @Inject
    @Named("databases.hive.metastore.port")
    private int metastorePort;

    // Simulates an aborted transaction which leaves behind a file in a table partition with some data
    public void simulateAbortedHiveTransaction(String database, String tableName, String partitionSpec)
            throws TException
    {
        ThriftHiveMetastoreClient client = createMetastoreClient();
        try {
            long transaction = client.openTransaction("test");

            long writeId = client.allocateTableWriteIds(database, tableName, Collections.singletonList(transaction)).get(0).getWriteId();

            // Rollback transaction which leaves behind a delta directory
            client.abortTransaction(transaction);

            // Create a new external table with location as the given partition location + delta directory for the rolled back transaction
            Table table = client.getTableWithCapabilities(database, tableName);
            String tableLocation = table.getSd().getLocation();
            String partitionLocation = tableLocation.endsWith("/") ? tableLocation + partitionSpec : tableLocation + "/" + partitionSpec;
            String deltaLocation = partitionLocation + "/" + AcidUtils.deltaSubdir(writeId, writeId, 0);

            String tmpTableName = getNewTableName();
            QueryExecutors.onHive().executeQuery(String.format("CREATE EXTERNAL TABLE %s (col string) STORED AS ORC LOCATION '%s'", tmpTableName, deltaLocation));

            // Insert data to the external table
            QueryExecutors.onHive().executeQuery(String.format("INSERT INTO TABLE %s SELECT 'a'", tmpTableName));
            assertThat(QueryExecutors.onHive().executeQuery(String.format("SELECT * FROM %s", tmpTableName))).containsOnly(row("a"));
            QueryExecutors.onHive().executeQuery(String.format("DROP TABLE %s", tmpTableName));
        }
        finally {
            client.close();
        }
    }

    private String getNewTableName()
    {
        return "table_" + UUID.randomUUID().toString().replace('-', '_');
    }

    private ThriftHiveMetastoreClient createMetastoreClient()
            throws TException
    {
        URI metastore = URI.create("thrift://" + metastoreHost + ":" + metastorePort);
        System.out.println("XXXXXXXXXXXXXXXXXXXXXx " + metastore);
        return new ThriftHiveMetastoreClient(
                Transport.create(
                        HostAndPort.fromParts(metastore.getHost(), metastore.getPort()),
                        Optional.empty(),
                        Optional.empty(),
                        10000,
                        new NoHiveMetastoreAuthentication(),
                        Optional.empty()), LOCALHOST);
    }
}
