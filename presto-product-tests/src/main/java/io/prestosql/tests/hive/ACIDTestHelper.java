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
import io.prestosql.tempto.context.ThreadLocalTestContextHolder;
import io.prestosql.tempto.query.QueryExecutor;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.thrift.TException;

import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;

public final class ACIDTestHelper
{
    private static final String LOCALHOST = "localhost";

    private ACIDTestHelper()
    {
    }

    // This simulates a aborted transaction which leaves behind a file in a partition of a table as follows:
    // 1. Open Txn
    // 2. Rollback Txn
    // 3. Create a new table with location as the given partition location + delta directory for the rolled back transaction
    // 4. Insert something to it which will create a file for this table
    public static void simulateAbortedHiveTransaction(String database, String tableName, String partitionSpec)
            throws TException
    {
        ThriftHiveMetastoreClient client = createMetastoreClient();
        try {
            // Step 1
            long transaction = client.openTransaction("test");

            AllocateTableWriteIdsRequest allocateTableWriteIdsRequest = new AllocateTableWriteIdsRequest(database, tableName);
            allocateTableWriteIdsRequest.setTxnIds(Collections.singletonList(transaction));
            long writeId = client.allocateTableWriteIdsBatchIntr(allocateTableWriteIdsRequest).get(0).getWriteId();

            // Step 2
            client.rollbackTransaction(transaction);

            // Step 3
            Table table = client.getTableWithCapabilities(database, tableName);
            String tableLocation = table.getSd().getLocation();
            String partitionLocation = tableLocation.endsWith("/") ? tableLocation + partitionSpec : tableLocation + "/" + partitionSpec;
            String deltaLocation = partitionLocation + "/" + AcidUtils.deltaSubdir(writeId, writeId, 0);
            QueryExecutor hiveQueryExecutor = ThreadLocalTestContextHolder.testContext().getDependency(QueryExecutor.class, "hive");

            String tmpTableName = getNewTableName();
            hiveQueryExecutor.executeQuery(String.format("CREATE EXTERNAL TABLE %s (col string) stored as ORC location '%s'", tmpTableName, deltaLocation));

            // Step 4
            hiveQueryExecutor.executeQuery(String.format("INSERT INTO TABLE %s SELECT 'a'", tmpTableName));
            assertThat(hiveQueryExecutor.executeQuery(String.format("SELECT * FROM %s", tmpTableName))).containsOnly(row("a"));
            hiveQueryExecutor.executeQuery(String.format("DROP TABLE %s", tmpTableName));
        }
        finally {
            client.close();
        }
    }

    private static String getNewTableName()
    {
        return "table_" + UUID.randomUUID().toString().replace('-', '_');
    }

    private static ThriftHiveMetastoreClient createMetastoreClient()
            throws TException
    {
        URI metastore = URI.create("thrift://hadoop-master:9083");
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
