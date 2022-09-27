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
import com.google.common.collect.SetMultimap;
import com.google.common.io.Closer;
import com.google.common.primitives.ImmutableIntArray;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.execution.ForQueryExecution;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.scheduler.EventDrivenTaskSource.Partition;
import io.trino.execution.scheduler.EventDrivenTaskSource.PartitionUpdate;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.SplitWeight;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceHandleSource;
import io.trino.spi.exchange.ExchangeSourceHandleSource.ExchangeSourceHandleBatch;
import io.trino.split.RemoteSplit;
import io.trino.split.SplitSource;
import io.trino.split.SplitSource.SplitBatch;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SplitSourceFactory;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableWriterNode;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.ImmutableSetMultimap.toImmutableSetMultimap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTargetTaskInputSize;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionTargetTaskSplitCount;
import static io.trino.SystemSessionProperties.getFaultTolerantPreserveInputPartitionsInWriteStage;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static java.lang.Math.round;
import static java.util.Objects.requireNonNull;

public class StageEventDrivenTaskSourceFactory
        implements EventDrivenTaskSourceFactory
{
    private final SplitSourceFactory splitSourceFactory;
    private final Executor executor;
    private final InternalNodeManager nodeManager;
    private final int splitBatchSize;

    @Inject
    public StageEventDrivenTaskSourceFactory(
            SplitSourceFactory splitSourceFactory,
            @ForQueryExecution ExecutorService executor,
            InternalNodeManager nodeManager,
            QueryManagerConfig queryManagerConfig)
    {
        this(
                splitSourceFactory,
                executor,
                nodeManager,
                requireNonNull(queryManagerConfig, "queryManagerConfig is null").getScheduleSplitBatchSize());
    }

    public StageEventDrivenTaskSourceFactory(
            SplitSourceFactory splitSourceFactory,
            Executor executor,
            InternalNodeManager nodeManager,
            int splitBatchSize)
    {
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.splitBatchSize = splitBatchSize;
    }

    @Override
    public EventDrivenTaskSource create(
            EventDrivenTaskSource.Callback callback,
            Session session,
            PlanFragment fragment,
            Map<PlanFragmentId, Exchange> sourceExchanges,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            LongConsumer getSplitTimeRecorder,
            Map<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates)
    {
        ImmutableMap.Builder<PlanFragmentId, PlanNodeId> remoteSources = ImmutableMap.builder();
        for (RemoteSourceNode remoteSource : fragment.getRemoteSourceNodes()) {
            for (PlanFragmentId sourceFragment : remoteSource.getSourceFragmentIds()) {
                remoteSources.put(sourceFragment, remoteSource.getId());
            }
        }
        long targetPartitionSizeInBytes = getFaultTolerantExecutionTargetTaskInputSize(session).toBytes();
        return new StageTaskSource(
                sourceExchanges,
                remoteSources.buildOrThrow(),
                () -> splitSourceFactory.createSplitSources(session, fragment),
                createSplitAssigner(
                        session,
                        fragment,
                        outputDataSizeEstimates,
                        sourcePartitioningScheme,
                        targetPartitionSizeInBytes),
                callback,
                executor,
                splitBatchSize,
                targetPartitionSizeInBytes,
                sourcePartitioningScheme,
                getSplitTimeRecorder);
    }

    private SplitAssigner createSplitAssigner(
            Session session,
            PlanFragment fragment,
            Map<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates,
            FaultTolerantPartitioningScheme sourcePartitioningScheme,
            long targetPartitionSizeInBytes)
    {
        PartitioningHandle partitioning = fragment.getPartitioning();

        Set<PlanNodeId> partitionedRemoteSources = fragment.getRemoteSourceNodes().stream()
                .filter(node -> node.getExchangeType() != REPLICATE)
                .map(PlanNode::getId)
                .collect(toImmutableSet());
        Set<PlanNodeId> partitionedSources = ImmutableSet.<PlanNodeId>builder()
                .addAll(partitionedRemoteSources)
                .addAll(fragment.getPartitionedSources())
                .build();
        Set<PlanNodeId> replicatedSources = fragment.getRemoteSourceNodes().stream()
                .filter(node -> node.getExchangeType() == REPLICATE)
                .map(PlanNode::getId)
                .collect(toImmutableSet());

        boolean coordinatorOnly = partitioning.equals(COORDINATOR_DISTRIBUTION);
        if (partitioning.equals(SINGLE_DISTRIBUTION) || coordinatorOnly) {
            ImmutableSet<HostAddress> hostRequirement = ImmutableSet.of();
            if (coordinatorOnly) {
                Node currentNode = nodeManager.getCurrentNode();
                verify(currentNode.isCoordinator(), "current node is expected to be a coordinator");
                hostRequirement = ImmutableSet.of(currentNode.getHostAndPort());
            }
            return new SingleDistributionSplitAssigner(
                    hostRequirement,
                    ImmutableSet.<PlanNodeId>builder()
                            .addAll(partitionedSources)
                            .addAll(replicatedSources)
                            .build());
        }
        if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION) || partitioning.equals(SCALED_WRITER_DISTRIBUTION) || partitioning.equals(SOURCE_DISTRIBUTION)) {
            // TODO: refactor to define explicitly
            long standardSplitSizeInBytes = targetPartitionSizeInBytes / getFaultTolerantExecutionTargetTaskSplitCount(session);
            return new ArbitraryDistributionSplitAssigner(
                    partitioning.getCatalogHandle(),
                    partitionedSources,
                    replicatedSources,
                    targetPartitionSizeInBytes,
                    standardSplitSizeInBytes);
        }
        if (partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.getCatalogHandle().isPresent()) {
            return new HashDistributionSplitAssigner(
                    partitioning.getCatalogHandle(),
                    partitionedSources,
                    replicatedSources,
                    getFaultTolerantExecutionTargetTaskInputSize(session).toBytes(),
                    outputDataSizeEstimates,
                    sourcePartitioningScheme,
                    getFaultTolerantPreserveInputPartitionsInWriteStage(session) && isWriteFragment(fragment));
        }

        // other partitioning handles are not expected to be set as a fragment partitioning
        throw new IllegalArgumentException("Unexpected partitioning: " + partitioning);
    }

    private static boolean isWriteFragment(PlanFragment fragment)
    {
        PlanVisitor<Boolean, Void> visitor = new PlanVisitor<>()
        {
            @Override
            protected Boolean visitPlan(PlanNode node, Void context)
            {
                for (PlanNode child : node.getSources()) {
                    if (child.accept(this, context)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Boolean visitTableWriter(TableWriterNode node, Void context)
            {
                return true;
            }
        };

        return fragment.getRoot().accept(visitor, null);
    }

    @VisibleForTesting
    static class StageTaskSource
            implements EventDrivenTaskSource
    {
        private final Map<PlanFragmentId, Exchange> sourceExchanges;
        private final Map<PlanFragmentId, PlanNodeId> remoteSources;
        private final Supplier<Map<PlanNodeId, SplitSource>> splitSourceSupplier;
        private final SplitAssigner assigner;
        private final EventDrivenTaskSource.Callback callback;
        private final Executor executor;
        private final int splitBatchSize;
        private final long targetPartitionSizeInBytes;
        private final FaultTolerantPartitioningScheme sourcePartitioningScheme;
        private final LongConsumer getSplitTimeRecorder;
        private final SetMultimap<PlanNodeId, PlanFragmentId> remoteSourceFragments;

        @GuardedBy("this")
        private boolean started;
        @GuardedBy("this")
        private boolean closed;
        @GuardedBy("this")
        private final Closer closer = Closer.create();

        private final Object assignerLock = new Object();

        @VisibleForTesting
        StageTaskSource(
                Map<PlanFragmentId, Exchange> sourceExchanges,
                Map<PlanFragmentId, PlanNodeId> remoteSources,
                Supplier<Map<PlanNodeId, SplitSource>> splitSourceSupplier,
                SplitAssigner assigner,
                Callback callback,
                Executor executor,
                int splitBatchSize,
                long targetPartitionSizeInBytes,
                FaultTolerantPartitioningScheme sourcePartitioningScheme,
                LongConsumer getSplitTimeRecorder)
        {
            this.sourceExchanges = ImmutableMap.copyOf(requireNonNull(sourceExchanges, "sourceExchanges is null"));
            this.remoteSources = ImmutableMap.copyOf(requireNonNull(remoteSources, "remoteSources is null"));
            checkArgument(
                    sourceExchanges.keySet().equals(remoteSources.keySet()),
                    "sourceExchanges and remoteSources are expected to contain the same set of keys: %s != %s",
                    sourceExchanges.keySet(),
                    remoteSources.keySet());
            this.splitSourceSupplier = requireNonNull(splitSourceSupplier, "splitSourceSupplier is null");
            this.assigner = requireNonNull(assigner, "assigner is null");
            this.callback = requireNonNull(callback, "callback is null");
            this.executor = requireNonNull(executor, "executor is null");
            this.splitBatchSize = splitBatchSize;
            this.targetPartitionSizeInBytes = targetPartitionSizeInBytes;
            this.sourcePartitioningScheme = requireNonNull(sourcePartitioningScheme, "sourcePartitioningScheme is null");
            this.getSplitTimeRecorder = requireNonNull(getSplitTimeRecorder, "getSplitTimeRecorder is null");
            remoteSourceFragments = remoteSources.entrySet().stream()
                    .collect(toImmutableSetMultimap(Map.Entry::getValue, Map.Entry::getKey));
        }

        @Override
        public synchronized void start()
        {
            if (started || closed) {
                return;
            }
            started = true;
            try {
                List<SplitLoader> splitLoaders = new ArrayList<>();
                Set<PlanFragmentId> finishedFragments = new HashSet<>();
                Set<PlanNodeId> allSources = new HashSet<>();
                Set<PlanNodeId> finishedSources = new HashSet<>();
                for (Map.Entry<PlanFragmentId, Exchange> entry : sourceExchanges.entrySet()) {
                    PlanFragmentId fragmentId = entry.getKey();
                    PlanNodeId remoteSourceNodeId = getRemoteSourceNode(fragmentId);
                    allSources.add(remoteSourceNodeId);
                    ExchangeSourceHandleSource handleSource = closer.register(entry.getValue().getSourceHandles());
                    ExchangeSplitSource splitSource = closer.register(new ExchangeSplitSource(handleSource, targetPartitionSizeInBytes));
                    SplitLoader splitLoader = closer.register(new SplitLoader(
                            splitSource,
                            executor,
                            ExchangeSplitSource::getSplitPartition,
                            new SplitLoader.Callback()
                            {
                                @Override
                                public void update(ListMultimap<Integer, Split> splits, boolean noMoreSplitsForFragment)
                                {
                                    try {
                                        synchronized (assignerLock) {
                                            if (noMoreSplitsForFragment) {
                                                finishedFragments.add(fragmentId);
                                            }
                                            boolean noMoreSplitsForRemoteSource = finishedFragments.containsAll(remoteSourceFragments.get(remoteSourceNodeId));
                                            assigner.assign(remoteSourceNodeId, splits, noMoreSplitsForRemoteSource).update(callback);
                                            if (noMoreSplitsForRemoteSource) {
                                                finishedSources.add(remoteSourceNodeId);
                                            }
                                            if (finishedSources.containsAll(allSources)) {
                                                assigner.finish().update(callback);
                                            }
                                        }
                                    }
                                    catch (Throwable t) {
                                        fail(t);
                                    }
                                }

                                @Override
                                public void failed(Throwable t)
                                {
                                    fail(t);
                                }
                            },
                            splitBatchSize,
                            getSplitTimeRecorder));
                    splitLoaders.add(splitLoader);
                }
                for (Map.Entry<PlanNodeId, SplitSource> entry : splitSourceSupplier.get().entrySet()) {
                    PlanNodeId planNodeId = entry.getKey();
                    allSources.add(planNodeId);
                    SplitLoader splitLoader = closer.register(new SplitLoader(
                            entry.getValue(),
                            executor,
                            this::getSplitPartition,
                            new SplitLoader.Callback()
                            {
                                @Override
                                public void update(ListMultimap<Integer, Split> splits, boolean noMoreSplits)
                                {
                                    try {
                                        synchronized (assignerLock) {
                                            assigner.assign(planNodeId, splits, noMoreSplits).update(callback);
                                            if (noMoreSplits) {
                                                finishedSources.add(planNodeId);
                                            }
                                            if (finishedSources.containsAll(allSources)) {
                                                assigner.finish().update(callback);
                                            }
                                        }
                                    }
                                    catch (Throwable t) {
                                        fail(t);
                                    }
                                }

                                @Override
                                public void failed(Throwable t)
                                {
                                    fail(t);
                                }
                            },
                            splitBatchSize,
                            getSplitTimeRecorder));
                    splitLoaders.add(splitLoader);
                }
                if (splitLoaders.isEmpty()) {
                    executor.execute(() -> {
                        try {
                            synchronized (assignerLock) {
                                assigner.finish().update(callback);
                            }
                        }
                        catch (Throwable t) {
                            fail(t);
                        }
                    });
                }
                else {
                    splitLoaders.forEach(SplitLoader::start);
                }
            }
            catch (Throwable t) {
                try {
                    closer.close();
                }
                catch (Throwable closerFailure) {
                    if (closerFailure != t) {
                        t.addSuppressed(closerFailure);
                    }
                }
                throw t;
            }
        }

        private PlanNodeId getRemoteSourceNode(PlanFragmentId fragmentId)
        {
            PlanNodeId planNodeId = remoteSources.get(fragmentId);
            verify(planNodeId != null, "remote source not found for fragment: %s", fragmentId);
            return planNodeId;
        }

        private int getSplitPartition(Split split)
        {
            return sourcePartitioningScheme.getPartition(split);
        }

        private void fail(Throwable failure)
        {
            callback.failed(failure);
            close();
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            try {
                closer.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @VisibleForTesting
    interface SplitAssigner
    {
        AssignmentResult assign(PlanNodeId planNodeId, ListMultimap<Integer, Split> splits, boolean noMoreSplits);

        AssignmentResult finish();
    }

    @VisibleForTesting
    record AssignmentResult(
            List<Partition> partitionsAdded,
            boolean noMorePartitions,
            List<PartitionUpdate> partitionUpdates,
            ImmutableIntArray sealedPartitions)
    {
        AssignmentResult
        {
            partitionsAdded = ImmutableList.copyOf(requireNonNull(partitionsAdded, "partitionsAdded is null"));
            partitionUpdates = ImmutableList.copyOf(requireNonNull(partitionUpdates, "partitionUpdates is null"));
        }

        void update(EventDrivenTaskSource.Callback callback)
        {
            if (!partitionsAdded.isEmpty()) {
                callback.partitionsAdded(partitionsAdded);
            }
            if (noMorePartitions) {
                callback.noMorePartitions();
            }
            if (!partitionUpdates.isEmpty()) {
                callback.partitionsUpdated(partitionUpdates);
            }
            if (!sealedPartitions.isEmpty()) {
                callback.partitionsSealed(sealedPartitions);
            }
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static class Builder
        {
            private final ImmutableList.Builder<Partition> partitionsAdded = ImmutableList.builder();
            private boolean noMorePartitions;
            private final ImmutableList.Builder<PartitionUpdate> partitionUpdates = ImmutableList.builder();
            private final ImmutableIntArray.Builder sealedPartitions = ImmutableIntArray.builder();

            public Builder addPartition(Partition partition)
            {
                partitionsAdded.add(partition);
                return this;
            }

            public Builder setNoMorePartitions()
            {
                this.noMorePartitions = true;
                return this;
            }

            public Builder updatePartition(PartitionUpdate partitionUpdate)
            {
                partitionUpdates.add(partitionUpdate);
                return this;
            }

            public Builder sealPartition(int partitionId)
            {
                sealedPartitions.add(partitionId);
                return this;
            }

            public AssignmentResult build()
            {
                return new AssignmentResult(
                        partitionsAdded.build(),
                        noMorePartitions,
                        partitionUpdates.build(),
                        sealedPartitions.build());
            }
        }
    }

    @VisibleForTesting
    static class SingleDistributionSplitAssigner
            implements SplitAssigner
    {
        private final Set<HostAddress> hostRequirement;
        private final Set<PlanNodeId> allSources;

        private boolean partitionAdded;
        private final Set<PlanNodeId> completedSources = new HashSet<>();

        SingleDistributionSplitAssigner(Set<HostAddress> hostRequirement, Set<PlanNodeId> allSources)
        {
            this.hostRequirement = ImmutableSet.copyOf(requireNonNull(hostRequirement, "hostRequirement is null"));
            this.allSources = ImmutableSet.copyOf(requireNonNull(allSources, "allSources is null"));
        }

        @Override
        public AssignmentResult assign(PlanNodeId planNodeId, ListMultimap<Integer, Split> splits, boolean noMoreSplits)
        {
            AssignmentResult.Builder assignment = AssignmentResult.builder();
            if (!partitionAdded) {
                partitionAdded = true;
                assignment.addPartition(new Partition(0, new NodeRequirements(Optional.empty(), hostRequirement)));
                assignment.setNoMorePartitions();
            }
            if (!splits.isEmpty()) {
                checkState(!completedSources.contains(planNodeId), "source is finished: %s", planNodeId);
                assignment.updatePartition(new PartitionUpdate(
                        0,
                        planNodeId,
                        ImmutableList.copyOf(splits.values()),
                        false));
            }
            if (noMoreSplits) {
                assignment.updatePartition(new PartitionUpdate(
                        0,
                        planNodeId,
                        ImmutableList.of(),
                        true));
                completedSources.add(planNodeId);
            }
            if (completedSources.containsAll(allSources)) {
                assignment.sealPartition(0);
            }
            return assignment.build();
        }

        @Override
        public AssignmentResult finish()
        {
            AssignmentResult.Builder result = AssignmentResult.builder();
            if (!partitionAdded) {
                partitionAdded = true;
                result
                        .addPartition(new Partition(0, new NodeRequirements(Optional.empty(), hostRequirement)))
                        .sealPartition(0)
                        .setNoMorePartitions();
            }
            return result.build();
        }
    }

    @VisibleForTesting
    static class ArbitraryDistributionSplitAssigner
            implements SplitAssigner
    {
        private final Optional<CatalogHandle> catalogRequirement;
        private final Set<PlanNodeId> partitionedSources;
        private final Set<PlanNodeId> replicatedSources;
        private final Set<PlanNodeId> allSources;
        private final long targetPartitionSizeInBytes;
        private final long standardSplitSizeInBytes;

        private int nextPartitionId;
        private final List<PartitionAssignment> allAssignments = new ArrayList<>();
        private final Map<Optional<HostAddress>, PartitionAssignment> openAssignments = new HashMap<>();

        private final Set<PlanNodeId> completedSources = new HashSet<>();

        private final ListMultimap<PlanNodeId, Split> replicatedSplits = ArrayListMultimap.create();
        private boolean noMoreReplicatedSplits;

        ArbitraryDistributionSplitAssigner(
                Optional<CatalogHandle> catalogRequirement,
                Set<PlanNodeId> partitionedSources,
                Set<PlanNodeId> replicatedSources,
                long targetPartitionSizeInBytes,
                long standardSplitSizeInBytes)
        {
            this.catalogRequirement = requireNonNull(catalogRequirement, "catalogRequirement is null");
            this.partitionedSources = ImmutableSet.copyOf(requireNonNull(partitionedSources, "partitionedSources is null"));
            this.replicatedSources = ImmutableSet.copyOf(requireNonNull(replicatedSources, "replicatedSources is null"));
            allSources = ImmutableSet.<PlanNodeId>builder()
                    .addAll(partitionedSources)
                    .addAll(replicatedSources)
                    .build();
            this.targetPartitionSizeInBytes = targetPartitionSizeInBytes;
            this.standardSplitSizeInBytes = standardSplitSizeInBytes;
        }

        @Override
        public AssignmentResult assign(PlanNodeId planNodeId, ListMultimap<Integer, Split> splits, boolean noMoreSplits)
        {
            for (Split split : splits.values()) {
                Optional<CatalogHandle> splitCatalogRequirement = Optional.of(split.getCatalogHandle())
                        .filter(catalog -> !catalog.getType().isInternal() && !catalog.equals(REMOTE_CATALOG_HANDLE));
                checkArgument(
                        catalogRequirement.isEmpty() || catalogRequirement.equals(splitCatalogRequirement),
                        "unexpected split catalog requirement: %s",
                        splitCatalogRequirement);
            }
            if (replicatedSources.contains(planNodeId)) {
                return assignReplicatedSplits(planNodeId, ImmutableList.copyOf(splits.values()), noMoreSplits);
            }
            return assignPartitionedSplits(planNodeId, ImmutableList.copyOf(splits.values()), noMoreSplits);
        }

        @Override
        public AssignmentResult finish()
        {
            checkState(!allAssignments.isEmpty(), "allAssignments is not expected to be empty");
            return AssignmentResult.builder().build();
        }

        private AssignmentResult assignReplicatedSplits(PlanNodeId planNodeId, List<Split> splits, boolean noMoreSplits)
        {
            AssignmentResult.Builder assignment = AssignmentResult.builder();
            replicatedSplits.putAll(planNodeId, splits);
            for (PartitionAssignment partitionAssignment : allAssignments) {
                assignment.updatePartition(new PartitionUpdate(
                        partitionAssignment.getPartitionId(),
                        planNodeId,
                        splits,
                        noMoreSplits));
            }
            if (noMoreSplits) {
                completedSources.add(planNodeId);
                if (completedSources.containsAll(replicatedSources)) {
                    noMoreReplicatedSplits = true;
                }
            }
            if (noMoreReplicatedSplits) {
                for (PartitionAssignment partitionAssignment : allAssignments) {
                    if (partitionAssignment.isFull()) {
                        assignment.sealPartition(partitionAssignment.getPartitionId());
                    }
                }
            }
            if (completedSources.containsAll(allSources)) {
                if (allAssignments.isEmpty()) {
                    // at least a single partition is expected to be created
                    allAssignments.add(new PartitionAssignment(0));
                    assignment.addPartition(new Partition(0, new NodeRequirements(catalogRequirement, ImmutableSet.of())));
                    for (PlanNodeId replicatedSourceId : replicatedSources) {
                        assignment.updatePartition(new PartitionUpdate(
                                0,
                                replicatedSourceId,
                                replicatedSplits.get(replicatedSourceId),
                                completedSources.contains(replicatedSourceId)));
                    }
                    assignment.sealPartition(0);
                }
                else {
                    for (PartitionAssignment partitionAssignment : allAssignments) {
                        // set noMoreSplits for partitioned sources
                        if (!partitionAssignment.isFull()) {
                            for (PlanNodeId partitionedSourceNodeId : partitionedSources) {
                                assignment.updatePartition(new PartitionUpdate(
                                        partitionAssignment.getPartitionId(),
                                        partitionedSourceNodeId,
                                        ImmutableList.of(),
                                        true));
                            }
                            // seal partition
                            assignment.sealPartition(partitionAssignment.getPartitionId());
                        }
                    }
                }
                replicatedSplits.clear();
                // no more partitions will be created
                assignment.setNoMorePartitions();
            }
            return assignment.build();
        }

        private AssignmentResult assignPartitionedSplits(PlanNodeId planNodeId, List<Split> splits, boolean noMoreSplits)
        {
            AssignmentResult.Builder assignment = AssignmentResult.builder();

            for (Split split : splits) {
                Optional<HostAddress> hostRequirement = getHostRequirement(split);
                PartitionAssignment partitionAssignment = openAssignments.get(hostRequirement);
                long splitSizeInBytes = getSplitSizeInBytes(split);
                if (partitionAssignment != null && partitionAssignment.getAssignedDataSizeInBytes() + splitSizeInBytes > targetPartitionSizeInBytes) {
                    partitionAssignment.setFull(true);
                    for (PlanNodeId partitionedSourceNodeId : partitionedSources) {
                        assignment.updatePartition(new PartitionUpdate(
                                partitionAssignment.getPartitionId(),
                                partitionedSourceNodeId,
                                ImmutableList.of(),
                                true));
                    }
                    if (completedSources.containsAll(replicatedSources)) {
                        assignment.sealPartition(partitionAssignment.getPartitionId());
                    }
                    partitionAssignment = null;
                    openAssignments.remove(hostRequirement);
                }
                if (partitionAssignment == null) {
                    partitionAssignment = new PartitionAssignment(nextPartitionId++);
                    allAssignments.add(partitionAssignment);
                    openAssignments.put(hostRequirement, partitionAssignment);
                    assignment.addPartition(new Partition(
                            partitionAssignment.getPartitionId(),
                            new NodeRequirements(catalogRequirement, hostRequirement.map(ImmutableSet::of).orElseGet(ImmutableSet::of))));

                    for (PlanNodeId replicatedSourceId : replicatedSources) {
                        assignment.updatePartition(new PartitionUpdate(
                                partitionAssignment.getPartitionId(),
                                replicatedSourceId,
                                replicatedSplits.get(replicatedSourceId),
                                completedSources.contains(replicatedSourceId)));
                    }
                }
                assignment.updatePartition(new PartitionUpdate(
                        partitionAssignment.getPartitionId(),
                        planNodeId,
                        ImmutableList.of(split),
                        false));
                partitionAssignment.assignPartitionedData(splitSizeInBytes);
            }

            if (noMoreSplits) {
                completedSources.add(planNodeId);
            }

            if (completedSources.containsAll(allSources)) {
                if (allAssignments.isEmpty()) {
                    // at least a single partition is expected to be created
                    allAssignments.add(new PartitionAssignment(0));
                    assignment.addPartition(new Partition(0, new NodeRequirements(catalogRequirement, ImmutableSet.of())));
                    for (PlanNodeId replicatedSourceId : replicatedSources) {
                        assignment.updatePartition(new PartitionUpdate(
                                0,
                                replicatedSourceId,
                                replicatedSplits.get(replicatedSourceId),
                                completedSources.contains(replicatedSourceId)));
                    }
                    assignment.sealPartition(0);
                }
                else {
                    for (PartitionAssignment partitionAssignment : openAssignments.values()) {
                        // set noMoreSplits for partitioned sources
                        for (PlanNodeId partitionedSourceNodeId : partitionedSources) {
                            assignment.updatePartition(new PartitionUpdate(
                                    partitionAssignment.getPartitionId(),
                                    partitionedSourceNodeId,
                                    ImmutableList.of(),
                                    true));
                        }
                        // seal partition
                        assignment.sealPartition(partitionAssignment.getPartitionId());
                    }
                    openAssignments.clear();
                }
                replicatedSplits.clear();
                // no more partitions will be created
                assignment.setNoMorePartitions();
            }

            return assignment.build();
        }

        private Optional<HostAddress> getHostRequirement(Split split)
        {
            if (split.getConnectorSplit().isRemotelyAccessible()) {
                return Optional.empty();
            }
            List<HostAddress> addresses = split.getAddresses();
            checkArgument(!addresses.isEmpty(), "split is not remotely accessible but the list of hosts is empty: %s", split);
            HostAddress selectedAddress = null;
            long selectedAssignmentDataSize = Long.MAX_VALUE;
            for (HostAddress address : addresses) {
                PartitionAssignment assignment = openAssignments.get(Optional.of(address));
                if (assignment == null) {
                    // prioritize unused addresses
                    selectedAddress = address;
                    break;
                }
                else if (assignment.getAssignedDataSizeInBytes() < selectedAssignmentDataSize) {
                    // otherwise prioritize the smallest assignment
                    selectedAddress = address;
                    selectedAssignmentDataSize = assignment.getAssignedDataSizeInBytes();
                }
            }
            verify(selectedAddress != null, "selectedAddress is null");
            return Optional.of(selectedAddress);
        }

        private long getSplitSizeInBytes(Split split)
        {
            if (split.getCatalogHandle().equals(REMOTE_CATALOG_HANDLE)) {
                RemoteSplit remoteSplit = (RemoteSplit) split.getConnectorSplit();
                SpoolingExchangeInput exchangeInput = (SpoolingExchangeInput) remoteSplit.getExchangeInput();
                long size = 0;
                for (ExchangeSourceHandle handle : exchangeInput.getExchangeSourceHandles()) {
                    size += handle.getDataSizeInBytes();
                }
                return size;
            }
            return round(((split.getSplitWeight().getRawValue() * 1.0) / SplitWeight.standard().getRawValue()) * standardSplitSizeInBytes);
        }

        private static class PartitionAssignment
        {
            private final int partitionId;
            private long assignedDataSizeInBytes;
            private boolean full;

            private PartitionAssignment(int partitionId)
            {
                this.partitionId = partitionId;
            }

            public int getPartitionId()
            {
                return partitionId;
            }

            public void assignPartitionedData(long sizeInBytes)
            {
                assignedDataSizeInBytes += sizeInBytes;
            }

            public long getAssignedDataSizeInBytes()
            {
                return assignedDataSizeInBytes;
            }

            public boolean isFull()
            {
                return full;
            }

            public void setFull(boolean full)
            {
                this.full = full;
            }
        }
    }

    static class HashDistributionSplitAssigner
            implements SplitAssigner
    {
        private final Optional<CatalogHandle> catalogRequirement;
        private final Set<PlanNodeId> partitionedSources;
        private final Set<PlanNodeId> replicatedSources;
        private final Set<PlanNodeId> allSources;
        private final long targetPartitionSizeInBytes;
        private final Map<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates;
        private final FaultTolerantPartitioningScheme sourcePartitioningScheme;
        private final boolean preserveOutputPartitioning;

        private Map<Integer, TaskPartition> outputPartitionToTaskPartition;
        private final Set<Integer> createdTaskPartitions = new HashSet<>();
        private final Set<PlanNodeId> completedSources = new HashSet<>();
        private final ListMultimap<PlanNodeId, Split> replicatedSplits = ArrayListMultimap.create();

        private int nextTaskPartitionId;

        HashDistributionSplitAssigner(
                Optional<CatalogHandle> catalogRequirement,
                Set<PlanNodeId> partitionedSources,
                Set<PlanNodeId> replicatedSources,
                long targetPartitionSizeInBytes,
                Map<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates,
                FaultTolerantPartitioningScheme sourcePartitioningScheme,
                boolean preserveOutputPartitioning)
        {
            this.catalogRequirement = requireNonNull(catalogRequirement, "catalogRequirement is null");
            this.partitionedSources = ImmutableSet.copyOf(requireNonNull(partitionedSources, "partitionedSources is null"));
            this.replicatedSources = ImmutableSet.copyOf(requireNonNull(replicatedSources, "replicatedSources is null"));
            allSources = ImmutableSet.<PlanNodeId>builder()
                    .addAll(partitionedSources)
                    .addAll(replicatedSources)
                    .build();
            this.targetPartitionSizeInBytes = targetPartitionSizeInBytes;
            this.outputDataSizeEstimates = ImmutableMap.copyOf(requireNonNull(outputDataSizeEstimates, "outputDataSizeEstimates is null"));
            this.sourcePartitioningScheme = requireNonNull(sourcePartitioningScheme, "sourcePartitioningScheme is null");
            this.preserveOutputPartitioning = preserveOutputPartitioning;
        }

        @Override
        public AssignmentResult assign(PlanNodeId planNodeId, ListMultimap<Integer, Split> splits, boolean noMoreSplits)
        {
            AssignmentResult.Builder assignment = AssignmentResult.builder();
            if (outputPartitionToTaskPartition == null) {
                outputPartitionToTaskPartition = createOutputPartitionToTaskPartition();
            }

            if (replicatedSources.contains(planNodeId)) {
                replicatedSplits.putAll(planNodeId, splits.values());
                for (Integer partitionId : createdTaskPartitions) {
                    assignment.updatePartition(new PartitionUpdate(partitionId, planNodeId, ImmutableList.copyOf(splits.values()), noMoreSplits));
                }
            }
            else {
                for (Integer outputPartitionId : splits.keySet()) {
                    TaskPartition taskPartition = outputPartitionToTaskPartition.get(outputPartitionId);
                    verify(taskPartition != null, "taskPartition not found for outputPartitionId: %s", outputPartitionId);
                    if (!taskPartition.isIdAssigned()) {
                        taskPartition.assignId(nextTaskPartitionId++);
                    }
                    int taskPartitionId = taskPartition.getId();
                    if (!createdTaskPartitions.contains(taskPartitionId)) {
                        Set<HostAddress> hostRequirement = sourcePartitioningScheme.getNodeRequirement(outputPartitionId)
                                .map(InternalNode::getHostAndPort)
                                .map(ImmutableSet::of)
                                .orElse(ImmutableSet.of());
                        assignment.addPartition(new Partition(
                                taskPartitionId,
                                new NodeRequirements(catalogRequirement, hostRequirement)));
                        for (PlanNodeId replicatedSource : replicatedSplits.keySet()) {
                            assignment.updatePartition(new PartitionUpdate(taskPartitionId, replicatedSource, replicatedSplits.get(replicatedSource), completedSources.contains(replicatedSource)));
                        }
                        for (PlanNodeId completedSource : completedSources) {
                            assignment.updatePartition(new PartitionUpdate(taskPartitionId, completedSource, ImmutableList.of(), true));
                        }
                        createdTaskPartitions.add(taskPartitionId);
                    }
                    assignment.updatePartition(new PartitionUpdate(taskPartitionId, planNodeId, splits.get(outputPartitionId), false));
                }
            }

            if (noMoreSplits) {
                completedSources.add(planNodeId);
                for (Integer taskPartition : createdTaskPartitions) {
                    assignment.updatePartition(new PartitionUpdate(taskPartition, planNodeId, ImmutableList.of(), true));
                }
                if (completedSources.containsAll(allSources)) {
                    if (createdTaskPartitions.isEmpty()) {
                        assignment.addPartition(new Partition(
                                0,
                                new NodeRequirements(catalogRequirement, ImmutableSet.of())));
                        for (PlanNodeId replicatedSource : replicatedSplits.keySet()) {
                            assignment.updatePartition(new PartitionUpdate(0, replicatedSource, replicatedSplits.get(replicatedSource), completedSources.contains(replicatedSource)));
                        }
                        for (PlanNodeId completedSource : completedSources) {
                            assignment.updatePartition(new PartitionUpdate(0, completedSource, ImmutableList.of(), true));
                        }
                        createdTaskPartitions.add(0);
                    }
                    for (Integer taskPartition : createdTaskPartitions) {
                        assignment.sealPartition(taskPartition);
                    }
                    assignment.setNoMorePartitions();
                    replicatedSplits.clear();
                }
            }

            return assignment.build();
        }

        @Override
        public AssignmentResult finish()
        {
            checkState(!createdTaskPartitions.isEmpty(), "createdTaskPartitions is not expected to be empty");
            return AssignmentResult.builder().build();
        }

        private Map<Integer, TaskPartition> createOutputPartitionToTaskPartition()
        {
            int partitionCount = sourcePartitioningScheme.getPartitionCount();
            if (sourcePartitioningScheme.isExplicitPartitionToNodeMappingPresent() ||
                    partitionedSources.isEmpty() ||
                    !outputDataSizeEstimates.keySet().containsAll(partitionedSources) ||
                    preserveOutputPartitioning) {
                // if bucket scheme is set explicitly or if estimates are missing create one task partition per output partition
                return IntStream.range(0, partitionCount)
                        .boxed()
                        .collect(toImmutableMap(Function.identity(), (key) -> new TaskPartition()));
            }

            List<OutputDataSizeEstimate> partitionedSourcesEstimates = outputDataSizeEstimates.entrySet().stream()
                    .filter(entry -> partitionedSources.contains(entry.getKey()))
                    .map(Map.Entry::getValue)
                    .collect(toImmutableList());
            OutputDataSizeEstimate mergedEstimate = OutputDataSizeEstimate.merge(partitionedSourcesEstimates);
            ImmutableMap.Builder<Integer, TaskPartition> result = ImmutableMap.builder();
            PriorityQueue<PartitionAssignment> assignments = new PriorityQueue<>();
            assignments.add(new PartitionAssignment(new TaskPartition(), 0));
            for (int outputPartitionId = 0; outputPartitionId < partitionCount; outputPartitionId++) {
                long outputPartitionSize = mergedEstimate.getPartitionSizeInBytes(outputPartitionId);
                if (assignments.peek().getAssignedDataSizeInBytes() + outputPartitionSize > targetPartitionSizeInBytes
                        && assignments.size() < partitionCount) {
                    assignments.add(new PartitionAssignment(new TaskPartition(), 0));
                }
                PartitionAssignment assignment = assignments.poll();
                result.put(outputPartitionId, assignment.getTaskPartition());
                assignments.add(new PartitionAssignment(assignment.getTaskPartition(), assignment.getAssignedDataSizeInBytes() + outputPartitionSize));
            }
            return result.buildOrThrow();
        }

        private static class PartitionAssignment
                implements Comparable<PartitionAssignment>
        {
            private final TaskPartition taskPartition;
            private final long assignedDataSizeInBytes;

            private PartitionAssignment(TaskPartition taskPartition, long assignedDataSizeInBytes)
            {
                this.taskPartition = requireNonNull(taskPartition, "taskPartition is null");
                this.assignedDataSizeInBytes = assignedDataSizeInBytes;
            }

            public TaskPartition getTaskPartition()
            {
                return taskPartition;
            }

            public long getAssignedDataSizeInBytes()
            {
                return assignedDataSizeInBytes;
            }

            @Override
            public int compareTo(PartitionAssignment other)
            {
                return Long.compare(assignedDataSizeInBytes, other.assignedDataSizeInBytes);
            }
        }

        private static class TaskPartition
        {
            private OptionalInt id = OptionalInt.empty();

            public void assignId(int id)
            {
                this.id = OptionalInt.of(id);
            }

            public boolean isIdAssigned()
            {
                return id.isPresent();
            }

            public int getId()
            {
                checkState(id.isPresent(), "id is expected to be assigned");
                return id.getAsInt();
            }
        }
    }

    private static class ExchangeSplitSource
            implements SplitSource
    {
        private final ExchangeSourceHandleSource handleSource;
        private final long targetPartitionSizeInBytes;
        private final AtomicBoolean finished = new AtomicBoolean();

        private ExchangeSplitSource(ExchangeSourceHandleSource handleSource, long targetPartitionSizeInBytes)
        {
            this.handleSource = requireNonNull(handleSource, "handleSource is null");
            this.targetPartitionSizeInBytes = targetPartitionSizeInBytes;
        }

        @Override
        public CatalogHandle getCatalogHandle()
        {
            return REMOTE_CATALOG_HANDLE;
        }

        @Override
        public ListenableFuture<SplitBatch> getNextBatch(int maxSize)
        {
            ListenableFuture<ExchangeSourceHandleBatch> sourceHandlesFuture = toListenableFuture(handleSource.getNextBatch());
            return Futures.transform(
                    sourceHandlesFuture,
                    batch -> {
                        List<ExchangeSourceHandle> handles = batch.handles();
                        ListMultimap<Integer, ExchangeSourceHandle> partitionToHandles = handles.stream()
                                .collect(toImmutableListMultimap(ExchangeSourceHandle::getPartitionId, Function.identity()));
                        ImmutableList.Builder<Split> splits = ImmutableList.builder();
                        for (int partition : partitionToHandles.keySet()) {
                            splits.addAll(createRemoteSplits(partitionToHandles.get(partition)));
                        }
                        if (batch.lastBatch()) {
                            finished.set(true);
                        }
                        return new SplitBatch(splits.build(), batch.lastBatch());
                    }, directExecutor());
        }

        private List<Split> createRemoteSplits(List<ExchangeSourceHandle> handles)
        {
            ImmutableList.Builder<Split> result = ImmutableList.builder();
            ImmutableList.Builder<ExchangeSourceHandle> currentSplitHandles = ImmutableList.builder();
            long currentSplitHandlesSize = 0;
            long currentSplitHandlesCount = 0;
            for (ExchangeSourceHandle handle : handles) {
                if (currentSplitHandlesCount > 0 && currentSplitHandlesSize + handle.getDataSizeInBytes() > targetPartitionSizeInBytes) {
                    result.add(createRemoteSplit(currentSplitHandles.build()));
                    currentSplitHandles = ImmutableList.builder();
                    currentSplitHandlesSize = 0;
                    currentSplitHandlesCount = 0;
                }
                currentSplitHandles.add(handle);
                currentSplitHandlesSize += handle.getDataSizeInBytes();
                currentSplitHandlesCount++;
            }
            if (currentSplitHandlesCount > 0) {
                result.add(createRemoteSplit(currentSplitHandles.build()));
            }
            return result.build();
        }

        private static Split createRemoteSplit(List<ExchangeSourceHandle> handles)
        {
            return new Split(REMOTE_CATALOG_HANDLE, new RemoteSplit(new SpoolingExchangeInput(handles, Optional.empty())));
        }

        private static int getSplitPartition(Split split)
        {
            RemoteSplit remoteSplit = (RemoteSplit) split.getConnectorSplit();
            SpoolingExchangeInput exchangeInput = (SpoolingExchangeInput) remoteSplit.getExchangeInput();
            List<ExchangeSourceHandle> handles = exchangeInput.getExchangeSourceHandles();
            return handles.get(0).getPartitionId();
        }

        @Override
        public void close()
        {
            handleSource.close();
        }

        @Override
        public boolean isFinished()
        {
            return finished.get();
        }

        @Override
        public Optional<List<Object>> getTableExecuteSplitsInfo()
        {
            return Optional.empty();
        }
    }

    private static class SplitLoader
            implements Closeable
    {
        private final SplitSource splitSource;
        private final Executor executor;
        private final ToIntFunction<Split> splitToPartition;
        private final Callback callback;
        private final int splitBatchSize;
        private final LongConsumer getSplitTimeRecorder;

        @GuardedBy("this")
        private boolean started;
        @GuardedBy("this")
        private boolean closed;
        @GuardedBy("this")
        private ListenableFuture<SplitBatch> splitLoadingFuture;

        public SplitLoader(
                SplitSource splitSource,
                Executor executor,
                ToIntFunction<Split> splitToPartition,
                Callback callback,
                int splitBatchSize,
                LongConsumer getSplitTimeRecorder)
        {
            this.splitSource = requireNonNull(splitSource, "splitSource is null");
            this.executor = requireNonNull(executor, "executor is null");
            this.splitToPartition = requireNonNull(splitToPartition, "splitToPartition is null");
            this.callback = requireNonNull(callback, "callback is null");
            this.splitBatchSize = splitBatchSize;
            this.getSplitTimeRecorder = requireNonNull(getSplitTimeRecorder, "getSplitTimeRecorder is null");
        }

        public synchronized void start()
        {
            if (started || closed) {
                return;
            }
            started = true;
            processNext();
        }

        private synchronized void processNext()
        {
            if (closed) {
                return;
            }
            verify(splitLoadingFuture == null || splitLoadingFuture.isDone(), "splitLoadingFuture is still running");
            long start = System.currentTimeMillis();
            splitLoadingFuture = splitSource.getNextBatch(splitBatchSize);
            Futures.addCallback(splitLoadingFuture, new FutureCallback<>()
            {
                @Override
                public void onSuccess(SplitBatch result)
                {
                    try {
                        getSplitTimeRecorder.accept(System.currentTimeMillis() - start);
                        ListMultimap<Integer, Split> splits = result.getSplits().stream()
                                .collect(toImmutableListMultimap(splitToPartition::applyAsInt, Function.identity()));
                        callback.update(splits, result.isLastBatch());
                        if (!result.isLastBatch()) {
                            processNext();
                        }
                    }
                    catch (Throwable t) {
                        callback.failed(t);
                    }
                }

                @Override
                public void onFailure(Throwable t)
                {
                    callback.failed(t);
                }
            }, executor);
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            if (splitLoadingFuture != null) {
                splitLoadingFuture.cancel(true);
                splitLoadingFuture = null;
            }
            splitSource.close();
        }

        public interface Callback
        {
            void update(ListMultimap<Integer, Split> splits, boolean noMoreSplits);

            void failed(Throwable t);
        }
    }
}
