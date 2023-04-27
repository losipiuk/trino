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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.airlift.log.Logger;
import io.trino.metadata.Split;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_HASH_DISTRIBUTION;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

class StaticHashDistributionSplitAssigner
        implements SplitAssigner
{
    private static final Logger log = Logger.get(StaticHashDistributionSplitAssigner.class);

    private final Set<PlanNodeId> replicatedSources;
    private final Map<PlanFragmentId, Integer> outputPartitionsCounts;
    private final Set<PlanNodeId> allSources;

    private final PlanFragmentId fragmentId;
    private final int hashOutputPartitionsCount;
    private final boolean adjustToInputPartitionsCount;
    private final double adjustToInputPartitionsCountFactor;
    private final Set<PlanNodeId> completedSources = new HashSet<>();
    private final ListMultimap<PlanNodeId, Split> replicatedSplits = ArrayListMultimap.create();
    private boolean partitionsAdded;

    public static StaticHashDistributionSplitAssigner create(
            int hashOutputPartitionsCount,
            boolean adjustToInputPartitionsCount,
            double adjustToInputPartitionsCountFactor,
            Set<PlanNodeId> partitionedSources,
            Set<PlanNodeId> replicatedSources,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            PlanFragment fragment,
            Map<PlanFragmentId, Integer> outputPartitionsCounts)
    {
        if (fragment.getPartitioning().equals(SCALED_WRITER_HASH_DISTRIBUTION)) {
            verify(
                    fragment.getPartitionedSources().isEmpty() && fragment.getRemoteSourceNodes().size() == 1,
                    "SCALED_WRITER_HASH_DISTRIBUTION fragments are expected to have exactly one remote source and no table scans");
        }
        return new StaticHashDistributionSplitAssigner(
                fragment.getId(),
                hashOutputPartitionsCount,
                adjustToInputPartitionsCount,
                adjustToInputPartitionsCountFactor,
                partitionedSources,
                replicatedSources,
                sourcePartitioningScheme,
                outputPartitionsCounts);
    }

    @VisibleForTesting
    StaticHashDistributionSplitAssigner(
            PlanFragmentId fragmentId,
            int hashOutputPartitionsCount,
            boolean adjustToInputPartitionsCount,
            double adjustToInputPartitionsCountFactor,
            Set<PlanNodeId> partitionedSources,
            Set<PlanNodeId> replicatedSources,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            Map<PlanFragmentId, Integer> outputPartitionsCounts)
    {
        this.fragmentId = requireNonNull(fragmentId, "fragmentId is null");
        this.hashOutputPartitionsCount = hashOutputPartitionsCount;
        this.adjustToInputPartitionsCount = adjustToInputPartitionsCount;
        this.adjustToInputPartitionsCountFactor = adjustToInputPartitionsCountFactor;
        this.replicatedSources = ImmutableSet.copyOf(requireNonNull(replicatedSources, "replicatedSources is null"));
        this.outputPartitionsCounts = ImmutableMap.copyOf(outputPartitionsCounts);
        allSources = ImmutableSet.<PlanNodeId>builder()
                .addAll(partitionedSources)
                .addAll(replicatedSources)
                .build();
        checkArgument(!sourcePartitioningScheme.isExplicitPartitionToNodeMappingPresent(), "sourcePartitioningScheme.isExplicitPartitionToNodeMappingPresent() not supported");
        checkArgument(IntStream.range(0, sourcePartitioningScheme.getPartitionCount()).noneMatch(outputPartition -> sourcePartitioningScheme.getNodeRequirement(outputPartition).isPresent()), "host requirements not supported");
    }

    @Override
    public AssignmentResult assign(PlanNodeId planNodeId, ListMultimap<Integer, Split> splits, boolean noMoreSplits)
    {
        AssignmentResult.Builder assignment = AssignmentResult.builder();

        int selectedHashOutputPartitionsCount;
        if (adjustToInputPartitionsCount) {
            selectedHashOutputPartitionsCount = min((int) (outputPartitionsCounts.values().stream().mapToInt(i -> i).sum() * adjustToInputPartitionsCountFactor), hashOutputPartitionsCount);
            log.info("selected %s partitions for %s based on children %s", selectedHashOutputPartitionsCount, fragmentId, outputPartitionsCounts);
        }
        else {
            selectedHashOutputPartitionsCount = this.hashOutputPartitionsCount;
            log.info("selected %s partitions for %s statically ", selectedHashOutputPartitionsCount, fragmentId);
        }

        if (!partitionsAdded) {
            for (int taskPartition = 0; taskPartition < selectedHashOutputPartitionsCount; ++taskPartition) {
                assignment.addPartition(new Partition(taskPartition, new NodeRequirements(Optional.empty(), ImmutableSet.of())));
            }
            assignment.setNoMorePartitions();
            partitionsAdded = true;
        }

        if (replicatedSources.contains(planNodeId)) {
            replicatedSplits.putAll(planNodeId, splits.values());
            for (int taskPartition = 0; taskPartition < selectedHashOutputPartitionsCount; ++taskPartition) {
                assignment.updatePartition(new PartitionUpdate(taskPartition, planNodeId, ImmutableList.copyOf(splits.values()), noMoreSplits));
            }
        }
        else {
            splits.forEach((outputPartitionId, split) -> {
                int taskPartition = outputPartitionId % selectedHashOutputPartitionsCount;
                assignment.updatePartition(new PartitionUpdate(taskPartition, planNodeId, ImmutableList.of(split), false));
            });
        }

        if (noMoreSplits) {
            completedSources.add(planNodeId);
            for (int taskPartition = 0; taskPartition < selectedHashOutputPartitionsCount; ++taskPartition) {
                assignment.updatePartition(new PartitionUpdate(taskPartition, planNodeId, ImmutableList.of(), true));
            }
            if (completedSources.containsAll(allSources)) {
                for (int taskPartition = 0; taskPartition < selectedHashOutputPartitionsCount; ++taskPartition) {
                    assignment.sealPartition(taskPartition);
                }
                replicatedSplits.clear();
            }
        }

        return assignment.build();
    }

    @Override
    public AssignmentResult finish()
    {
        return AssignmentResult.builder().build();
    }
}
