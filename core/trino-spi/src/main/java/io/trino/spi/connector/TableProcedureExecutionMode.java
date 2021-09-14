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
package io.trino.spi.connector;

public class TableProcedureExecutionMode
{
    private final boolean processData;
    private final boolean filter;
    private final boolean repartition;
    private final boolean sort;

    public TableProcedureExecutionMode(boolean processData, boolean repartition, boolean filter, boolean sort)
    {
        this.processData = processData;
        if (!processData) {
            if (repartition) {
                throw new IllegalArgumentException("repartitioning not supported if table data is not processed");
            }
            if (filter) {
                throw new IllegalArgumentException("filtering not supported if table data is not processed");
            }
            if (sort) {
                throw new IllegalArgumentException("sorting not supported if table data is not processed");
            }
        }

        this.repartition = repartition;
        this.filter = filter;
        this.sort = sort;
    }

    public boolean isProcessData()
    {
        return processData;
    }

    public boolean isFilter()
    {
        return filter;
    }

    public boolean isRepartition()
    {
        return repartition;
    }

    public boolean isSort()
    {
        return sort;
    }

    public static TableProcedureExecutionMode coordinatorOnly()
    {
        return new TableProcedureExecutionMode(false, false, false, false);
    }

    public static TableProcedureExecutionMode distributedWithFilteringAndRepartitioning()
    {
        return new TableProcedureExecutionMode(true, true, true, false);
    }
}
