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

import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.query.QueryResult;
import org.apache.thrift.TException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_TRANSACTIONAL;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.hive.ACIDTestHelper.simulateAbortedHiveTransaction;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

public class TestHiveTransactionalTableInsert
        extends ProductTest
{
    @Test(dataProvider = "transactionalTableType", groups = HIVE_TRANSACTIONAL)
    public void testInsertIntoTransactionalTable(TransactionalTableType type)
    {
        String tableName = "test_insert_into_transactional_table_" + type.name().toLowerCase(ENGLISH);
        onHive().executeQuery("" +
                "CREATE TABLE " + tableName + "(a bigint)" +
                "CLUSTERED BY(a) INTO 4 BUCKETS STORED AS ORC " +
                hiveTableProperties(type));

        try {
            assertThat(() -> query("INSERT INTO " + tableName + " (a) VALUES (42)"))
                    .failsWithMessage("Hive transactional tables are not supported: default." + tableName);
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(dataProvider = "isTablePartitioned", groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL})
    public void testFilesForAbortedTransactionsIgnored(boolean isPartitioned)
            throws TException
    {
        String tableName = "test_insert_only_acid_table_partitioned" + (isPartitioned ? "_partitioned" : "");
        onHive().executeQuery("" +
                "CREATE TABLE " + tableName + " (col INT) " +
                (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                "STORED AS ORC " + hiveTableProperties(TransactionalTableType.INSERT_ONLY));

        try {
            String hivePartition = isPartitioned ? " PARTITION (part_col=2) " : "";
            String predicate = isPartitioned ? " WHERE part_col = 2 " : "";

            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartition + " select 1");

            String selectFromOnePartitionsSql = "SELECT col FROM " + tableName + predicate + " ORDER BY COL";
            QueryResult onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(1));

            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartition + " select 2");
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsExactly(row(1), row(2));

            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartition + " select 3");
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(3));

            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartition + " select 4");
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsExactly(row(3), row(4));

            // Simulate aborted transaction in Hive which has left behind a write directory and file
            simulateAbortedHiveTransaction("default", tableName, "part_col=2");

            // Above simulation would have written to the part_col a new delta directory that corresponds to a
            // aborted txn but it should not be read
            onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsExactly(row(3), row(4));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @DataProvider
    public Object[][] isTablePartitioned()
    {
        return new Object[][] {
                {true},
                {false}
        };
    }

    @DataProvider
    public Object[][] transactionalTableType()
    {
        return new Object[][] {
                {TransactionalTableType.ACID},
                {TransactionalTableType.INSERT_ONLY},
        };
    }

    private static String hiveTableProperties(TransactionalTableType transactionalTableType)
    {
        return transactionalTableType.getHiveTableProperties().stream()
                .collect(joining(",", "TBLPROPERTIES (", ")"));
    }
}
