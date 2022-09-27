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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.ImmutableIntArray;
import io.trino.metadata.Split;
import io.trino.sql.planner.plan.PlanNodeId;

import java.io.Closeable;
import java.util.List;

import static java.util.Objects.requireNonNull;

public interface EventDrivenTaskSource
        extends Closeable
{
    void start();

    @Override
    void close();

    interface Callback
    {
        void partitionsAdded(List<Partition> partitions);

        void noMorePartitions();

        void partitionsUpdated(List<PartitionUpdate> partitionUpdates);

        void partitionsSealed(ImmutableIntArray partitionIds);

        void failed(Throwable t);
    }

    record Partition(int partitionId, NodeRequirements nodeRequirements)
    {
        public Partition
        {
            requireNonNull(nodeRequirements, "nodeRequirements is null");
        }
    }

    record PartitionUpdate(int partitionId, PlanNodeId planNodeId, List<Split> splits, boolean noMoreSplits)
    {
        public PartitionUpdate
        {
            requireNonNull(planNodeId, "planNodeId is null");
            splits = ImmutableList.copyOf(requireNonNull(splits, "splits is null"));
        }
    }
}
