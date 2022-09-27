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

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.graph.Traverser;
import com.google.common.io.Closer;
import com.google.common.primitives.ImmutableIntArray;
import com.google.common.primitives.ImmutableLongArray;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.execution.BasicStageStats;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.RemoteTask;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.SqlStage;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.StageState;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.TableInfo;
import io.trino.execution.TaskId;
import io.trino.execution.TaskState;
import io.trino.execution.TaskStatus;
import io.trino.execution.buffer.OutputBufferStatus;
import io.trino.execution.buffer.SpoolingOutputBuffers;
import io.trino.execution.buffer.SpoolingOutputStats;
import io.trino.execution.resourcegroups.IndexedPriorityQueue;
import io.trino.execution.scheduler.EventDrivenTaskSource.Partition;
import io.trino.execution.scheduler.EventDrivenTaskSource.PartitionUpdate;
import io.trino.execution.scheduler.NodeAllocator.NodeLease;
import io.trino.execution.scheduler.PartitionMemoryEstimator.MemoryRequirements;
import io.trino.failuredetector.FailureDetector;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Metadata;
import io.trino.metadata.Split;
import io.trino.operator.RetryPolicy;
import io.trino.server.DynamicFilterService;
import io.trino.spi.ErrorCode;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;
import io.trino.split.RemoteSplit;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.getDone;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionDefaultCoordinatorTaskMemory;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionDefaultTaskMemory;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionPartitionCount;
import static io.trino.SystemSessionProperties.getMaxTasksWaitingForNodePerStage;
import static io.trino.SystemSessionProperties.getRetryDelayScaleFactor;
import static io.trino.SystemSessionProperties.getRetryInitialDelay;
import static io.trino.SystemSessionProperties.getRetryMaxDelay;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.getTaskRetryAttemptsPerTask;
import static io.trino.execution.BasicStageStats.aggregateBasicStageStats;
import static io.trino.execution.StageState.ABORTED;
import static io.trino.execution.StageState.PLANNED;
import static io.trino.execution.scheduler.ErrorCodes.isOutOfMemoryError;
import static io.trino.execution.scheduler.Exchanges.getAllSourceHandles;
import static io.trino.failuredetector.FailureDetector.State.GONE;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.spi.ErrorType.USER_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.REMOTE_HOST_GONE;
import static io.trino.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.trino.util.Failures.toFailure;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class EventDrivenFaultTolerantQueryScheduler
        implements QueryScheduler
{
    private static final Logger log = Logger.get(EventDrivenFaultTolerantQueryScheduler.class);

    private final QueryStateMachine queryStateMachine;
    private final Metadata metadata;
    private final RemoteTaskFactory remoteTaskFactory;
    private final TaskDescriptorStorage taskDescriptorStorage;
    private final EventDrivenTaskSourceFactory taskSourceFactory;
    private final boolean summarizeTaskInfo;
    private final NodeTaskMap nodeTaskMap;
    private final ExecutorService queryExecutor;
    private final ScheduledExecutorService scheduledExecutorService;
    private final SplitSchedulerStats schedulerStats;
    private final PartitionMemoryEstimatorFactory memoryEstimatorFactory;
    private final NodePartitioningManager nodePartitioningManager;
    private final ExchangeManager exchangeManager;
    private final NodeAllocatorService nodeAllocatorService;
    private final FailureDetector failureDetector;
    private final DynamicFilterService dynamicFilterService;
    private final TaskExecutionStats taskExecutionStats;
    private final SubPlan originalPlan;

    private final StageRegistry stageRegistry;

    @GuardedBy("this")
    private boolean started;
    @GuardedBy("this")
    private Scheduler scheduler;

    public EventDrivenFaultTolerantQueryScheduler(
            QueryStateMachine queryStateMachine,
            Metadata metadata,
            RemoteTaskFactory remoteTaskFactory,
            TaskDescriptorStorage taskDescriptorStorage,
            EventDrivenTaskSourceFactory taskSourceFactory,
            boolean summarizeTaskInfo,
            NodeTaskMap nodeTaskMap,
            ExecutorService queryExecutor,
            ScheduledExecutorService scheduledExecutorService,
            SplitSchedulerStats schedulerStats,
            PartitionMemoryEstimatorFactory memoryEstimatorFactory,
            NodePartitioningManager nodePartitioningManager,
            ExchangeManager exchangeManager,
            NodeAllocatorService nodeAllocatorService,
            FailureDetector failureDetector,
            DynamicFilterService dynamicFilterService,
            TaskExecutionStats taskExecutionStats,
            SubPlan originalPlan)
    {
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        RetryPolicy retryPolicy = getRetryPolicy(queryStateMachine.getSession());
        verify(retryPolicy == TASK, "unexpected retry policy: %s", retryPolicy);
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
        this.taskSourceFactory = requireNonNull(taskSourceFactory, "taskSourceFactory is null");
        this.summarizeTaskInfo = summarizeTaskInfo;
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
        this.scheduledExecutorService = requireNonNull(scheduledExecutorService, "scheduledExecutorService is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.memoryEstimatorFactory = requireNonNull(memoryEstimatorFactory, "memoryEstimatorFactory is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "partitioningSchemeFactory is null");
        this.exchangeManager = requireNonNull(exchangeManager, "exchangeManager is null");
        this.nodeAllocatorService = requireNonNull(nodeAllocatorService, "nodeAllocatorService is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        this.taskExecutionStats = requireNonNull(taskExecutionStats, "taskExecutionStats is null");
        this.originalPlan = requireNonNull(originalPlan, "originalPlan is null");

        stageRegistry = new StageRegistry(queryStateMachine, originalPlan);
    }

    @Override
    public synchronized void start()
    {
        checkState(!started, "already started");
        started = true;

        if (queryStateMachine.isDone()) {
            return;
        }

        taskDescriptorStorage.initialize(queryStateMachine.getQueryId());
        queryStateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                taskDescriptorStorage.destroy(queryStateMachine.getQueryId());
            }
        });

        // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
        queryStateMachine.addStateChangeListener(state -> {
            if (!state.isDone()) {
                return;
            }
            Scheduler scheduler;
            synchronized (this) {
                scheduler = this.scheduler;
                this.scheduler = null;
            }
            if (scheduler != null) {
                scheduler.abort();
            }
            queryStateMachine.updateQueryInfo(Optional.ofNullable(stageRegistry.getStageInfo()));
        });

        Session session = queryStateMachine.getSession();
        FaultTolerantPartitioningSchemeFactory partitioningSchemeFactory = new FaultTolerantPartitioningSchemeFactory(
                nodePartitioningManager,
                session,
                getFaultTolerantExecutionPartitionCount(session));
        Closer closer = Closer.create();
        NodeAllocator nodeAllocator = closer.register(nodeAllocatorService.getNodeAllocator(session));
        try {
            scheduler = new Scheduler(
                    queryStateMachine,
                    metadata,
                    remoteTaskFactory,
                    taskDescriptorStorage,
                    taskSourceFactory,
                    summarizeTaskInfo,
                    nodeTaskMap,
                    queryExecutor,
                    scheduledExecutorService, schedulerStats,
                    memoryEstimatorFactory,
                    partitioningSchemeFactory,
                    exchangeManager,
                    getTaskRetryAttemptsPerTask(session) + 1,
                    getMaxTasksWaitingForNodePerStage(session),
                    nodeAllocator,
                    failureDetector,
                    stageRegistry,
                    taskExecutionStats,
                    dynamicFilterService,
                    getRetryInitialDelay(session).toMillis(),
                    getRetryMaxDelay(session).toMillis(),
                    getRetryDelayScaleFactor(session),
                    originalPlan,
                    systemTicker());
            queryExecutor.submit(scheduler::run);
        }
        catch (Throwable t) {
            try {
                closer.close();
            }
            catch (Throwable closerFailure) {
                if (t != closerFailure) {
                    t.addSuppressed(closerFailure);
                }
            }
            throw t;
        }
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        throw new UnsupportedOperationException("partial cancel is not supported in fault tolerant mode");
    }

    @Override
    public void failTask(TaskId taskId, Throwable failureCause)
    {
        stageRegistry.failTaskRemotely(taskId, failureCause);
    }

    @Override
    public BasicStageStats getBasicStageStats()
    {
        return stageRegistry.getBasicStageStats();
    }

    @Override
    public StageInfo getStageInfo()
    {
        return stageRegistry.getStageInfo();
    }

    @Override
    public long getUserMemoryReservation()
    {
        return stageRegistry.getUserMemoryReservation();
    }

    @Override
    public long getTotalMemoryReservation()
    {
        return stageRegistry.getTotalMemoryReservation();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return stageRegistry.getTotalCpuTime();
    }

    @ThreadSafe
    private static class StageRegistry
    {
        private final QueryStateMachine queryStateMachine;
        private final AtomicReference<SubPlan> plan;
        private final Map<StageId, SqlStage> stages = new ConcurrentHashMap<>();

        public StageRegistry(QueryStateMachine queryStateMachine, SubPlan plan)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.plan = new AtomicReference<>(requireNonNull(plan, "plan is null"));
        }

        public void add(SqlStage stage)
        {
            stages.put(stage.getStageId(), stage);
        }

        public void updatePlan(SubPlan plan)
        {
            this.plan.set(requireNonNull(plan, "plan is null"));
        }

        public StageInfo getStageInfo()
        {
            SubPlan plan = requireNonNull(this.plan.get(), "plan is null");
            Map<PlanFragmentId, StageInfo> stageInfos = stages.values().stream()
                    .collect(toImmutableMap(stage -> stage.getFragment().getId(), SqlStage::getStageInfo));
            Set<PlanFragmentId> reportedFragments = new HashSet<>();
            StageInfo stageInfo = getStageInfo(plan, stageInfos, reportedFragments);
            // TODO Some stages may no longer be present in the plan when adaptive re-planning is implemented
            // TODO Figure out how to report statistics for such stages
            verify(reportedFragments.containsAll(stageInfos.keySet()), "some stages are left unreported");
            return stageInfo;
        }

        private StageInfo getStageInfo(SubPlan plan, Map<PlanFragmentId, StageInfo> infos, Set<PlanFragmentId> reportedFragments)
        {
            PlanFragmentId fragmentId = plan.getFragment().getId();
            reportedFragments.add(fragmentId);
            StageInfo info = infos.get(fragmentId);
            if (info == null) {
                info = StageInfo.createInitial(
                        queryStateMachine.getQueryId(),
                        queryStateMachine.getQueryState().isDone() ? ABORTED : PLANNED,
                        plan.getFragment());
            }
            List<StageInfo> children = plan.getChildren().stream()
                    .map(child -> getStageInfo(child, infos, reportedFragments))
                    .collect(toImmutableList());
            return info.withSubStages(children);
        }

        public BasicStageStats getBasicStageStats()
        {
            List<BasicStageStats> stageStats = stages.values().stream()
                    .map(SqlStage::getBasicStageStats)
                    .collect(toImmutableList());
            return aggregateBasicStageStats(stageStats);
        }

        public long getUserMemoryReservation()
        {
            return stages.values().stream()
                    .mapToLong(SqlStage::getUserMemoryReservation)
                    .sum();
        }

        public long getTotalMemoryReservation()
        {
            return stages.values().stream()
                    .mapToLong(SqlStage::getTotalMemoryReservation)
                    .sum();
        }

        public Duration getTotalCpuTime()
        {
            long millis = stages.values().stream()
                    .mapToLong(stage -> stage.getTotalCpuTime().toMillis())
                    .sum();
            return new Duration(millis, MILLISECONDS);
        }

        public void failTaskRemotely(TaskId taskId, Throwable failureCause)
        {
            SqlStage sqlStage = requireNonNull(stages.get(taskId.getStageId()), () -> "stage not found: %s" + taskId.getStageId());
            sqlStage.failTaskRemotely(taskId, failureCause);
        }
    }

    private static class Scheduler
            implements EventListener
    {
        private static final int EVENT_BUFFER_CAPACITY = 100;

        private final QueryStateMachine queryStateMachine;
        private final Metadata metadata;
        private final RemoteTaskFactory remoteTaskFactory;
        private final TaskDescriptorStorage taskDescriptorStorage;
        private final EventDrivenTaskSourceFactory taskSourceFactory;
        private final boolean summarizeTaskInfo;
        private final NodeTaskMap nodeTaskMap;
        private final ExecutorService queryExecutor;
        private final ScheduledExecutorService scheduledExecutorService;
        private final SplitSchedulerStats schedulerStats;
        private final PartitionMemoryEstimatorFactory memoryEstimatorFactory;
        private final FaultTolerantPartitioningSchemeFactory partitioningSchemeFactory;
        private final ExchangeManager exchangeManager;
        private final int maxTaskExecutionAttempts;
        private final int maxTasksWaitingForNode;
        private final NodeAllocator nodeAllocator;
        private final FailureDetector failureDetector;
        private final StageRegistry stageRegistry;
        private final TaskExecutionStats taskExecutionStats;
        private final DynamicFilterService dynamicFilterService;
        private final long minRetryDelayInMillis;
        private final long maxRetryDelayInMillis;
        private final double retryDelayScaleFactor;

        private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();
        private final List<Event> eventBuffer = new ArrayList<>(EVENT_BUFFER_CAPACITY);

        private boolean started;

        private SubPlan plan;
        private List<SubPlan> planInTopologicalOrder;
        private final Map<StageId, StageExecution> stageExecutions = new HashMap<>();
        private final SetMultimap<StageId, StageId> stageConsumers = HashMultimap.create();

        private final IndexedPriorityQueue<ScheduledTask> schedulingQueue = new IndexedPriorityQueue<>();
        private int nextSchedulingPriority;

        private final Map<ScheduledTask, NodeLease> nodeAcquisitions = new HashMap<>();

        private final Stopwatch delayStopwatch;
        private OptionalLong delaySchedulingDurationInMillis = OptionalLong.empty();

        private boolean queryOutputSet;

        public Scheduler(
                QueryStateMachine queryStateMachine,
                Metadata metadata,
                RemoteTaskFactory remoteTaskFactory,
                TaskDescriptorStorage taskDescriptorStorage,
                EventDrivenTaskSourceFactory taskSourceFactory,
                boolean summarizeTaskInfo,
                NodeTaskMap nodeTaskMap,
                ExecutorService queryExecutor,
                ScheduledExecutorService scheduledExecutorService,
                SplitSchedulerStats schedulerStats,
                PartitionMemoryEstimatorFactory memoryEstimatorFactory,
                FaultTolerantPartitioningSchemeFactory partitioningSchemeFactory,
                ExchangeManager exchangeManager,
                int maxTaskExecutionAttempts,
                int maxTasksWaitingForNode,
                NodeAllocator nodeAllocator,
                FailureDetector failureDetector,
                StageRegistry stageRegistry,
                TaskExecutionStats taskExecutionStats,
                DynamicFilterService dynamicFilterService,
                long minRetryDelayInMillis,
                long maxRetryDelayInMillis,
                double retryDelayScaleFactor,
                SubPlan plan,
                Ticker ticker)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
            this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
            this.taskSourceFactory = requireNonNull(taskSourceFactory, "taskSourceFactory is null");
            this.summarizeTaskInfo = summarizeTaskInfo;
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.scheduledExecutorService = requireNonNull(scheduledExecutorService, "scheduledExecutorService is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.memoryEstimatorFactory = requireNonNull(memoryEstimatorFactory, "memoryEstimatorFactory is null");
            this.partitioningSchemeFactory = requireNonNull(partitioningSchemeFactory, "partitioningSchemeFactory is null");
            this.exchangeManager = requireNonNull(exchangeManager, "exchangeManager is null");
            checkArgument(maxTaskExecutionAttempts > 0, "maxTaskExecutionAttempts must be greater than zero: %s", maxTaskExecutionAttempts);
            this.maxTaskExecutionAttempts = maxTaskExecutionAttempts;
            this.maxTasksWaitingForNode = maxTasksWaitingForNode;
            this.nodeAllocator = requireNonNull(nodeAllocator, "nodeAllocator is null");
            this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
            this.stageRegistry = requireNonNull(stageRegistry, "stageRegistry is null");
            this.taskExecutionStats = requireNonNull(taskExecutionStats, "taskExecutionStats is null");
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
            this.minRetryDelayInMillis = minRetryDelayInMillis;
            this.maxRetryDelayInMillis = maxRetryDelayInMillis;
            this.retryDelayScaleFactor = retryDelayScaleFactor;
            this.delayStopwatch = Stopwatch.createUnstarted(requireNonNull(ticker, "ticker is null"));
            this.plan = requireNonNull(plan, "plan is null");

            planInTopologicalOrder = sortPlanInTopologicalOrder(plan);
        }

        public void run()
        {
            checkState(!started, "already started");
            started = true;

            queryStateMachine.addStateChangeListener(state -> {
                if (state.isDone()) {
                    eventQueue.add(Event.WAKE_UP);
                }
            });

            Optional<Throwable> failure = Optional.empty();
            try {
                if (schedule()) {
                    while (processEvents()) {
                        if (delayStopwatch.isRunning()) {
                            verify(delaySchedulingDurationInMillis.isPresent(), "delaySchedulingDurationInMillis must be present");
                            long remainingDelay = delaySchedulingDurationInMillis.getAsLong() - delayStopwatch.elapsed(MILLISECONDS);
                            if (remainingDelay > 0) {
                                continue;
                            }
                        }
                        if (!schedule()) {
                            break;
                        }
                    }
                }
            }
            catch (Throwable t) {
                failure = Optional.of(t);
            }

            for (StageExecution execution : stageExecutions.values()) {
                failure = closeAndAddSuppressed(failure, execution::abort);
            }
            for (NodeLease nodeLease : nodeAcquisitions.values()) {
                failure = closeAndAddSuppressed(failure, nodeLease::release);
            }
            nodeAcquisitions.clear();
            failure = closeAndAddSuppressed(failure, nodeAllocator);

            failure.ifPresent(queryStateMachine::transitionToFailed);
        }

        private Optional<Throwable> closeAndAddSuppressed(Optional<Throwable> existingFailure, Closeable closeable)
        {
            try {
                closeable.close();
            }
            catch (Throwable t) {
                if (existingFailure.isEmpty()) {
                    return Optional.of(t);
                }
                if (existingFailure.get() != t) {
                    existingFailure.get().addSuppressed(t);
                }
            }
            return existingFailure;
        }

        private boolean processEvents()
        {
            try {
                Event event = eventQueue.poll(1, MINUTES);
                if (event == null) {
                    return true;
                }
                eventBuffer.add(event);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            while (true) {
                eventQueue.drainTo(eventBuffer, EVENT_BUFFER_CAPACITY - eventBuffer.size());
                if (eventBuffer.isEmpty()) {
                    return true;
                }
                for (Event e : eventBuffer) {
                    if (e == Event.ABORT) {
                        return false;
                    }
                    if (e == Event.WAKE_UP) {
                        continue;
                    }
                    e.accept(this);
                }
                eventBuffer.clear();
            }
        }

        private boolean schedule()
        {
            if (checkComplete()) {
                return false;
            }
            optimize();
            updateStageExecutions();
            scheduleTasks();
            processNodeAcquisitions();
            return true;
        }

        private boolean checkComplete()
        {
            if (queryStateMachine.isDone()) {
                return true;
            }

            for (StageExecution execution : stageExecutions.values()) {
                if (execution.getState() == StageState.FAILED) {
                    StageInfo stageInfo = execution.getStageInfo();
                    ExecutionFailureInfo failureCause = stageInfo.getFailureCause();
                    RuntimeException failure = failureCause == null ?
                            new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "stage failed due to unknown error: %s".formatted(execution.getStageId())) :
                            failureCause.toException();
                    queryStateMachine.transitionToFailed(failure);
                    return true;
                }
            }
            StageId rootStageId = getStageId(plan.getFragment().getId());
            StageExecution rootStageExecution = stageExecutions.get(rootStageId);
            if (!queryOutputSet && rootStageExecution != null && rootStageExecution.getState() == StageState.FINISHED) {
                ListenableFuture<List<ExchangeSourceHandle>> sourceHandles = getAllSourceHandles(rootStageExecution.getExchange().getSourceHandles());
                Futures.addCallback(sourceHandles, new FutureCallback<>()
                {
                    @Override
                    public void onSuccess(List<ExchangeSourceHandle> handles)
                    {
                        try {
                            queryStateMachine.updateInputsForQueryResults(
                                    ImmutableList.of(new SpoolingExchangeInput(handles, Optional.of(rootStageExecution.getSinkOutputSelector()))),
                                    true);
                            queryStateMachine.transitionToFinishing();
                        }
                        catch (Throwable t) {
                            onFailure(t);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        queryStateMachine.transitionToFailed(t);
                    }
                }, queryExecutor);
                queryOutputSet = true;
            }
            return false;
        }

        private void optimize()
        {
            plan = optimizePlan(plan);
            planInTopologicalOrder = sortPlanInTopologicalOrder(plan);
            stageRegistry.updatePlan(plan);
        }

        private SubPlan optimizePlan(SubPlan plan)
        {
            // re-optimize plan here based on available runtime statistics
            return plan;
        }

        private void updateStageExecutions()
        {
            Set<StageId> existingStages = new HashSet<>();
            PlanFragmentId rootFragmentId = plan.getFragment().getId();
            for (SubPlan subPlan : planInTopologicalOrder) {
                PlanFragmentId fragmentId = subPlan.getFragment().getId();
                StageId stageId = getStageId(fragmentId);
                existingStages.add(stageId);
                if (isReadyForExecution(subPlan) && !stageExecutions.containsKey(stageId)) {
                    createStageExecution(subPlan, fragmentId.equals(rootFragmentId), nextSchedulingPriority++);
                }
            }
            stageExecutions.forEach((stageId, stageExecution) -> {
                if (!existingStages.contains(stageId)) {
                    // stage got re-written during re-optimization
                    stageExecution.abort();
                }
            });
        }

        private boolean isReadyForExecution(SubPlan subPlan)
        {
            for (SubPlan child : subPlan.getChildren()) {
                StageExecution childExecution = stageExecutions.get(getStageId(child.getFragment().getId()));
                if (childExecution == null) {
                    return false;
                }
                // TODO enable speculative execution
                if (childExecution.getState() != StageState.FINISHED) {
                    return false;
                }
            }
            return true;
        }

        private void createStageExecution(SubPlan subPlan, boolean rootFragment, int schedulingPriority)
        {
            Closer closer = Closer.create();

            try {
                PlanFragment fragment = subPlan.getFragment();
                Session session = queryStateMachine.getSession();

                StageId stageId = getStageId(fragment.getId());
                SqlStage stage = SqlStage.createSqlStage(
                        stageId,
                        fragment,
                        TableInfo.extract(session, metadata, fragment),
                        remoteTaskFactory,
                        session,
                        summarizeTaskInfo,
                        nodeTaskMap,
                        queryExecutor,
                        schedulerStats);
                closer.register(stage::abort);
                stageRegistry.add(stage);
                stage.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.ofNullable(stageRegistry.getStageInfo())));

                ImmutableMap.Builder<PlanFragmentId, Exchange> sourceExchanges = ImmutableMap.builder();
                Map<PlanFragmentId, OutputDataSizeEstimate> outputEstimates = new HashMap<>();
                for (SubPlan child : subPlan.getChildren()) {
                    PlanFragmentId childFragmentId = child.getFragment().getId();
                    StageExecution childExecution = getStageExecution(getStageId(childFragmentId));
                    sourceExchanges.put(childFragmentId, childExecution.getExchange());
                    outputEstimates.put(childFragmentId, childExecution.getOutputDataSize());
                    stageConsumers.put(childExecution.getStageId(), stageId);
                }

                ImmutableMap.Builder<PlanNodeId, OutputDataSizeEstimate> outputDataSizeEstimates = ImmutableMap.builder();
                for (RemoteSourceNode remoteSource : stage.getFragment().getRemoteSourceNodes()) {
                    List<OutputDataSizeEstimate> estimates = new ArrayList<>();
                    for (PlanFragmentId fragmentId : remoteSource.getSourceFragmentIds()) {
                        OutputDataSizeEstimate fragmentEstimate = outputEstimates.get(fragmentId);
                        verify(fragmentEstimate != null, "fragmentEstimate not found for fragment %s", fragmentId);
                        estimates.add(fragmentEstimate);
                    }
                    outputDataSizeEstimates.put(remoteSource.getId(), OutputDataSizeEstimate.merge(estimates));
                }

                EventDrivenTaskSource taskSource = closer.register(taskSourceFactory.create(
                        createTaskSourceCallback(stageId),
                        session,
                        fragment,
                        sourceExchanges.buildOrThrow(),
                        partitioningSchemeFactory.get(fragment.getPartitioning()),
                        stage::recordGetSplitTime,
                        outputDataSizeEstimates.buildOrThrow()));
                taskSource.start();

                FaultTolerantPartitioningScheme sinkPartitioningScheme = partitioningSchemeFactory.get(fragment.getPartitioningScheme().getPartitioning().getHandle());
                ExchangeContext exchangeContext = new ExchangeContext(queryStateMachine.getQueryId(), new ExchangeId("external-exchange-" + stage.getStageId().getId()));
                Exchange exchange = closer.register(exchangeManager.createExchange(
                        exchangeContext,
                        sinkPartitioningScheme.getPartitionCount(),
                        rootFragment));

                boolean coordinatorStage = stage.getFragment().getPartitioning().equals(COORDINATOR_DISTRIBUTION);

                StageExecution execution = new StageExecution(
                        queryStateMachine,
                        taskDescriptorStorage,
                        stage,
                        taskSource,
                        sinkPartitioningScheme,
                        exchange,
                        memoryEstimatorFactory.createPartitionMemoryEstimator(),
                        // do not retry coordinator only tasks
                        coordinatorStage ? 1 : maxTaskExecutionAttempts,
                        schedulingPriority, dynamicFilterService);

                stageExecutions.put(execution.getStageId(), execution);

                for (SubPlan child : subPlan.getChildren()) {
                    PlanFragmentId childFragmentId = child.getFragment().getId();
                    StageExecution childExecution = getStageExecution(getStageId(childFragmentId));
                    execution.setSourceOutputSelector(childFragmentId, childExecution.getSinkOutputSelector());
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

        private StageId getStageId(PlanFragmentId fragmentId)
        {
            return StageId.create(queryStateMachine.getQueryId(), fragmentId);
        }

        private EventDrivenTaskSource.Callback createTaskSourceCallback(StageId stageId)
        {
            return new EventDrivenTaskSource.Callback()
            {
                @Override
                public void partitionsAdded(List<Partition> partitions)
                {
                    eventQueue.add(new PartitionsAddedTaskSourceEvent(stageId, partitions));
                }

                @Override
                public void noMorePartitions()
                {
                    eventQueue.add(new NoMorePartitionsTaskSourceEvent(stageId));
                }

                @Override
                public void partitionsUpdated(List<PartitionUpdate> partitionUpdates)
                {
                    eventQueue.add(new PartitionsUpdatedTaskSourceEvent(stageId, partitionUpdates));
                }

                @Override
                public void partitionsSealed(ImmutableIntArray partitionIds)
                {
                    eventQueue.add(new PartitionsSealedTaskSourceEvent(stageId, partitionIds));
                }

                @Override
                public void failed(Throwable t)
                {
                    eventQueue.add(new FailedTaskSourceEvent(stageId, t));
                }
            };
        }

        private void processNodeAcquisitions()
        {
            Iterator<Map.Entry<ScheduledTask, NodeLease>> nodeAcquisitionIterator = nodeAcquisitions.entrySet().iterator();
            while (nodeAcquisitionIterator.hasNext()) {
                Map.Entry<ScheduledTask, NodeLease> nodeAcquisition = nodeAcquisitionIterator.next();
                ScheduledTask scheduledTask = nodeAcquisition.getKey();
                NodeLease nodeLease = nodeAcquisition.getValue();
                StageExecution stageExecution = getStageExecution(scheduledTask.getStageId());
                if (stageExecution.getState().isDone()) {
                    nodeAcquisitionIterator.remove();
                    nodeLease.release();
                }
                else if (nodeLease.getNode().isDone()) {
                    nodeAcquisitionIterator.remove();
                    try {
                        InternalNode node = getDone(nodeLease.getNode());
                        Optional<RemoteTask> remoteTask = stageExecution.schedule(scheduledTask.getPartitionId(), node);
                        remoteTask.ifPresent(task -> {
                            task.addStateChangeListener(createExchangeSinkInstanceHandleUpdateRequiredListener());
                            task.addStateChangeListener(taskStatus -> {
                                if (taskStatus.getState().isDone()) {
                                    nodeLease.release();
                                }
                            });
                            task.addFinalTaskInfoListener(taskExecutionStats::update);
                            task.addFinalTaskInfoListener(taskInfo -> eventQueue.add(new RemoteTaskCompletedEvent(taskInfo.getTaskStatus())));
                            nodeLease.attachTaskId(task.getTaskId());
                            task.start();
                            if (queryStateMachine.getQueryState() == QueryState.STARTING) {
                                queryStateMachine.transitionToRunning();
                            }
                        });
                    }
                    catch (ExecutionException e) {
                        throw new UncheckedExecutionException(e);
                    }
                }
            }
        }

        private StateChangeListener<TaskStatus> createExchangeSinkInstanceHandleUpdateRequiredListener()
        {
            AtomicLong respondedToVersion = new AtomicLong(-1);
            return taskStatus -> {
                OutputBufferStatus outputBufferStatus = taskStatus.getOutputBufferStatus();
                if (outputBufferStatus.getOutputBuffersVersion().isEmpty()) {
                    return;
                }
                if (!outputBufferStatus.isExchangeSinkInstanceHandleUpdateRequired()) {
                    return;
                }
                long remoteVersion = outputBufferStatus.getOutputBuffersVersion().getAsLong();
                while (true) {
                    long localVersion = respondedToVersion.get();
                    if (remoteVersion <= localVersion) {
                        break;
                    }
                    if (respondedToVersion.compareAndSet(localVersion, remoteVersion)) {
                        eventQueue.add(new RemoteTaskExchangeSinkInstanceHandleUpdateRequiredEvent(taskStatus));
                        break;
                    }
                }
            };
        }

        private void scheduleTasks()
        {
            while (nodeAcquisitions.size() < maxTasksWaitingForNode && !schedulingQueue.isEmpty()) {
                ScheduledTask scheduledTask = schedulingQueue.poll();
                verify(scheduledTask != null, "scheduledTask is null");
                StageExecution stageExecution = getStageExecution(scheduledTask.getStageId());
                if (stageExecution.getState().isDone()) {
                    continue;
                }
                int partitionId = scheduledTask.getPartitionId();
                Optional<NodeRequirements> nodeRequirements = stageExecution.getNodeRequirements(partitionId);
                if (nodeRequirements.isEmpty()) {
                    continue;
                }
                MemoryRequirements memoryRequirements = stageExecution.getMemoryRequirements(partitionId);
                NodeLease lease = nodeAllocator.acquire(nodeRequirements.get(), memoryRequirements.getRequiredMemory());
                lease.getNode().addListener(() -> eventQueue.add(Event.WAKE_UP), queryExecutor);
                nodeAcquisitions.put(scheduledTask, lease);
            }
        }

        public void abort()
        {
            eventQueue.clear();
            eventQueue.add(Event.ABORT);
        }

        @Override
        public void onRemoteTaskCompleted(RemoteTaskCompletedEvent event)
        {
            TaskStatus taskStatus = event.getTaskStatus();
            TaskId taskId = taskStatus.getTaskId();
            TaskState taskState = taskStatus.getState();
            StageExecution stageExecution = getStageExecution(taskId.getStageId());
            if (taskState == TaskState.FINISHED) {
                stageExecution.taskFinished(taskId, taskStatus);

                if (delayStopwatch.isRunning()) {
                    verify(delaySchedulingDurationInMillis.isPresent(), "delaySchedulingDurationInMillis is expected to be present");
                    if (delayStopwatch.elapsed(MILLISECONDS) > delaySchedulingDurationInMillis.getAsLong()) {
                        delayStopwatch.reset();
                        delaySchedulingDurationInMillis = OptionalLong.empty();
                    }
                }
            }
            else if (taskState == TaskState.FAILED) {
                ExecutionFailureInfo failureInfo = taskStatus.getFailures().stream()
                        .findFirst()
                        .map(this::rewriteTransportFailure)
                        .orElse(toFailure(new TrinoException(GENERIC_INTERNAL_ERROR, "A task failed for an unknown reason")));

                List<ScheduledTask> scheduledTasks = stageExecution.taskFailed(taskId, failureInfo, taskStatus);
                scheduledTasks.forEach(task -> schedulingQueue.addOrUpdate(task, task.getPriority()));

                if (shouldDelayScheduling(failureInfo.getErrorCode())) {
                    if (delayStopwatch.isRunning()) {
                        // we are currently delaying tasks scheduling
                        checkState(delaySchedulingDurationInMillis.isPresent());

                        if (delayStopwatch.elapsed(MILLISECONDS) > delaySchedulingDurationInMillis.getAsLong()) {
                            // we are past previous delay period and still getting failures; let's make it longer
                            delayStopwatch.reset().start();
                            delaySchedulingDurationInMillis = OptionalLong.of(min(round(delaySchedulingDurationInMillis.getAsLong() * retryDelayScaleFactor), maxRetryDelayInMillis));
                            scheduledExecutorService.schedule(() -> eventQueue.add(Event.WAKE_UP), delaySchedulingDurationInMillis.getAsLong(), MILLISECONDS);
                        }
                    }
                    else {
                        // initialize delaying of tasks scheduling
                        delayStopwatch.start();
                        delaySchedulingDurationInMillis = OptionalLong.of(minRetryDelayInMillis);
                        scheduledExecutorService.schedule(() -> eventQueue.add(Event.WAKE_UP), delaySchedulingDurationInMillis.getAsLong(), MILLISECONDS);
                    }
                }
            }

            // update output selectors
            ExchangeSourceOutputSelector outputSelector = stageExecution.getSinkOutputSelector();
            for (StageId consumerStageId : stageConsumers.get(stageExecution.getStageId())) {
                getStageExecution(consumerStageId).setSourceOutputSelector(stageExecution.getStageFragmentId(), outputSelector);
            }
        }

        @Override
        public void onRemoteTaskExchangeSinkInstanceHandleUpdateRequiredEvent(RemoteTaskExchangeSinkInstanceHandleUpdateRequiredEvent event)
        {
            TaskId taskId = event.getTaskStatus().getTaskId();
            StageExecution stageExecution = getStageExecution(taskId.getStageId());
            stageExecution.updateExchangeSinkInstanceHandle(taskId);
        }

        @Override
        public void onPartitionAddedTaskSourceEvent(PartitionsAddedTaskSourceEvent event)
        {
            StageId stageId = event.getStageId();
            StageExecution stageExecution = getStageExecution(stageId);
            for (Partition partition : event.getPartitions()) {
                Optional<ScheduledTask> scheduledTask = stageExecution.addPartition(partition.partitionId(), partition.nodeRequirements());
                scheduledTask.ifPresent(task -> schedulingQueue.addOrUpdate(task, task.getPriority()));
            }
        }

        @Override
        public void onPartitionUpdatedTaskSourceEvent(PartitionsUpdatedTaskSourceEvent event)
        {
            StageExecution stageExecution = getStageExecution(event.getStageId());
            for (PartitionUpdate partitionUpdate : event.getPartitionUpdates()) {
                stageExecution.updatePartition(
                        partitionUpdate.partitionId(),
                        partitionUpdate.planNodeId(),
                        partitionUpdate.splits(),
                        partitionUpdate.noMoreSplits());
            }
        }

        @Override
        public void onPartitionSealedTaskSourceEvent(PartitionsSealedTaskSourceEvent event)
        {
            StageId stageId = event.getStageId();
            StageExecution stageExecution = getStageExecution(stageId);
            event.getPartitionIds().forEach(partitionId -> {
                Optional<ScheduledTask> scheduledTask = stageExecution.sealPartition(partitionId);
                scheduledTask.ifPresent(task -> {
                    if (nodeAcquisitions.containsKey(task)) {
                        // task is already waiting for node
                        return;
                    }
                    schedulingQueue.addOrUpdate(task, task.getPriority());
                });
            });
        }

        @Override
        public void onNoMorePartitionsTaskSourceEvent(NoMorePartitionsTaskSourceEvent event)
        {
            StageExecution stageExecution = getStageExecution(event.getStageId());
            stageExecution.noMorePartitions();
        }

        @Override
        public void onFailedTaskSourceEvent(FailedTaskSourceEvent event)
        {
            StageExecution stageExecution = getStageExecution(event.getStageId());
            stageExecution.fail(event.getFailure());
        }

        private StageExecution getStageExecution(StageId stageId)
        {
            StageExecution execution = stageExecutions.get(stageId);
            checkState(execution != null, "stage execution does not exist for stage: %s", stageId);
            return execution;
        }

        private static List<SubPlan> sortPlanInTopologicalOrder(SubPlan subPlan)
        {
            ImmutableList.Builder<SubPlan> result = ImmutableList.builder();
            Traverser.forTree(SubPlan::getChildren).depthFirstPreOrder(subPlan).forEach(result::add);
            return result.build();
        }

        private boolean shouldDelayScheduling(@Nullable ErrorCode errorCode)
        {
            return errorCode == null || errorCode.getType() == INTERNAL_ERROR || errorCode.getType() == EXTERNAL;
        }

        private ExecutionFailureInfo rewriteTransportFailure(ExecutionFailureInfo executionFailureInfo)
        {
            if (executionFailureInfo.getRemoteHost() == null || failureDetector.getState(executionFailureInfo.getRemoteHost()) != GONE) {
                return executionFailureInfo;
            }

            return new ExecutionFailureInfo(
                    executionFailureInfo.getType(),
                    executionFailureInfo.getMessage(),
                    executionFailureInfo.getCause(),
                    executionFailureInfo.getSuppressed(),
                    executionFailureInfo.getStack(),
                    executionFailureInfo.getErrorLocation(),
                    REMOTE_HOST_GONE.toErrorCode(),
                    executionFailureInfo.getRemoteHost());
        }
    }

    private static class StageExecution
    {
        private static final int SPECULATIVE_EXECUTION_PRIORITY = 1_000_000_000;

        private final QueryStateMachine queryStateMachine;
        private final TaskDescriptorStorage taskDescriptorStorage;

        private final SqlStage stage;
        private final EventDrivenTaskSource taskSource;
        private final FaultTolerantPartitioningScheme sinkPartitioningScheme;
        private final Exchange exchange;
        private final PartitionMemoryEstimator partitionMemoryEstimator;
        private final int maxTaskExecutionAttempts;
        private final int schedulingPriority;
        private final DynamicFilterService dynamicFilterService;
        private final long[] outputDataSize;

        private final Int2ObjectMap<StagePartition> partitions = new Int2ObjectOpenHashMap<>();
        private boolean noMorePartitions;

        private final IntSet remainingPartitions = new IntOpenHashSet();

        private ExchangeSourceOutputSelector.Builder sinkOutputSelectorBuilder;
        private ExchangeSourceOutputSelector finalSinkOutputSelector;

        private final Set<PlanNodeId> remoteSourceIds;
        private final Map<PlanFragmentId, RemoteSourceNode> remoteSources;
        private final Map<PlanFragmentId, ExchangeSourceOutputSelector> sourceOutputSelectors = new HashMap<>();

        private StageExecution(
                QueryStateMachine queryStateMachine,
                TaskDescriptorStorage taskDescriptorStorage,
                SqlStage stage,
                EventDrivenTaskSource taskSource,
                FaultTolerantPartitioningScheme sinkPartitioningScheme,
                Exchange exchange,
                PartitionMemoryEstimator partitionMemoryEstimator,
                int maxTaskExecutionAttempts,
                int schedulingPriority,
                DynamicFilterService dynamicFilterService)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
            this.stage = requireNonNull(stage, "stage is null");
            this.taskSource = requireNonNull(taskSource, "taskSource is null");
            this.sinkPartitioningScheme = requireNonNull(sinkPartitioningScheme, "sinkPartitioningScheme is null");
            this.exchange = requireNonNull(exchange, "exchange is null");
            this.partitionMemoryEstimator = requireNonNull(partitionMemoryEstimator, "partitionMemoryEstimator is null");
            this.maxTaskExecutionAttempts = maxTaskExecutionAttempts;
            this.schedulingPriority = schedulingPriority;
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
            outputDataSize = new long[sinkPartitioningScheme.getPartitionCount()];
            sinkOutputSelectorBuilder = ExchangeSourceOutputSelector.builder(ImmutableSet.of(exchange.getId()));
            ImmutableMap.Builder<PlanFragmentId, RemoteSourceNode> remoteSources = ImmutableMap.builder();
            ImmutableSet.Builder<PlanNodeId> remoteSourceIds = ImmutableSet.builder();
            for (RemoteSourceNode remoteSource : stage.getFragment().getRemoteSourceNodes()) {
                remoteSourceIds.add(remoteSource.getId());
                remoteSource.getSourceFragmentIds().forEach(fragmentId -> remoteSources.put(fragmentId, remoteSource));
            }
            this.remoteSourceIds = remoteSourceIds.build();
            this.remoteSources = remoteSources.buildOrThrow();
        }

        public StageId getStageId()
        {
            return stage.getStageId();
        }

        public PlanFragmentId getStageFragmentId()
        {
            return stage.getFragment().getId();
        }

        public StageState getState()
        {
            return stage.getState();
        }

        public StageInfo getStageInfo()
        {
            return stage.getStageInfo();
        }

        public Exchange getExchange()
        {
            return exchange;
        }

        public Optional<ScheduledTask> addPartition(int partitionId, NodeRequirements nodeRequirements)
        {
            if (getState().isDone()) {
                return Optional.empty();
            }

            ExchangeSinkHandle exchangeSinkHandle = exchange.addSink(partitionId);
            Session session = queryStateMachine.getSession();
            DataSize defaultTaskMemory = stage.getFragment().getPartitioning().equals(COORDINATOR_DISTRIBUTION) ?
                    getFaultTolerantExecutionDefaultCoordinatorTaskMemory(session) :
                    getFaultTolerantExecutionDefaultTaskMemory(session);
            StagePartition partition = new StagePartition(
                    partitionId,
                    exchangeSinkHandle,
                    remoteSourceIds,
                    nodeRequirements,
                    partitionMemoryEstimator.getInitialMemoryRequirements(session, defaultTaskMemory),
                    maxTaskExecutionAttempts);
            checkState(partitions.putIfAbsent(partitionId, partition) == null, "partition with id %s already exist in stage %s", partitionId, stage.getStageId());
            for (RemoteSourceNode remoteSource : stage.getFragment().getRemoteSourceNodes()) {
                ExchangeSourceOutputSelector selector = null;
                for (PlanFragmentId sourceFragment : remoteSource.getSourceFragmentIds()) {
                    ExchangeSourceOutputSelector fragmentSelector = sourceOutputSelectors.get(sourceFragment);
                    if (fragmentSelector == null) {
                        continue;
                    }
                    if (selector == null) {
                        selector = fragmentSelector;
                    }
                    else {
                        selector = selector.merge(fragmentSelector);
                    }
                    if (selector != null) {
                        partition.updateExchangeSourceOutputSelector(remoteSource.getId(), selector);
                    }
                }
            }
            remainingPartitions.add(partitionId);

            return Optional.of(new ScheduledTask(stage.getStageId(), partitionId, SPECULATIVE_EXECUTION_PRIORITY + schedulingPriority));
        }

        public void updatePartition(int partitionId, PlanNodeId planNodeId, List<Split> splits, boolean noMoreSplits)
        {
            if (getState().isDone()) {
                return;
            }

            StagePartition partition = getStagePartition(partitionId);
            partition.updateOpenTaskDescriptor(planNodeId, splits, noMoreSplits);
        }

        public Optional<ScheduledTask> sealPartition(int partitionId)
        {
            if (getState().isDone()) {
                return Optional.empty();
            }

            StagePartition partition = getStagePartition(partitionId);
            TaskDescriptor taskDescriptor = partition.sealTaskDescriptor(partitionId);
            taskDescriptorStorage.put(stage.getStageId(), taskDescriptor);

            if (!partition.isRunning()) {
                // if partition is not yet running update it's priority as it is no longer speculative
                return Optional.of(new ScheduledTask(stage.getStageId(), partitionId, schedulingPriority));
            }

            // TODO: split into smaller partitions here if necessary (for example if a task for a given partition failed with out of memory)

            return Optional.empty();
        }

        public void noMorePartitions()
        {
            if (getState().isDone()) {
                return;
            }

            noMorePartitions = true;
            if (remainingPartitions.isEmpty()) {
                stage.finish();
                // TODO close exchange early
                taskSource.close();
            }
        }

        public Optional<RemoteTask> schedule(int partitionId, InternalNode node)
        {
            if (getState().isDone()) {
                return Optional.empty();
            }

            StagePartition partition = getStagePartition(partitionId);
            verify(partition.getRemainingAttempts() >= 0, "remaining attempts is expected to be greater than or equal to zero: %s", partition.getRemainingAttempts());

            Set<PlanNodeId> remoteSourceIds = new HashSet<>();
            Map<PlanNodeId, ExchangeSourceOutputSelector> outputSelectors = new HashMap<>();
            for (RemoteSourceNode remoteSource : stage.getFragment().getRemoteSourceNodes()) {
                remoteSourceIds.add(remoteSource.getId());
                ExchangeSourceOutputSelector mergedSelector = null;
                for (PlanFragmentId sourceFragmentId : remoteSource.getSourceFragmentIds()) {
                    ExchangeSourceOutputSelector sourceFragmentSelector = sourceOutputSelectors.get(sourceFragmentId);
                    if (sourceFragmentSelector == null) {
                        continue;
                    }
                    if (mergedSelector == null) {
                        mergedSelector = sourceFragmentSelector;
                    }
                    else {
                        mergedSelector = mergedSelector.merge(sourceFragmentSelector);
                    }
                }
                if (mergedSelector != null) {
                    outputSelectors.put(remoteSource.getId(), mergedSelector);
                }
            }

            ListMultimap<PlanNodeId, Split> splits = ArrayListMultimap.create();
            Set<PlanNodeId> noMoreSplits = new HashSet<>();

            Optional<OpenTaskDescriptor> openTaskDescriptor = partition.getOpenTaskDescriptor();
            if (openTaskDescriptor.isPresent()) {
                splits.putAll(openTaskDescriptor.get().getSplits());
                for (PlanNodeId noMoreSplitsPlanNodeId : openTaskDescriptor.get().getNoMoreSplits()) {
                    // not a remote source
                    if (!remoteSourceIds.contains(noMoreSplitsPlanNodeId)) {
                        noMoreSplits.add(noMoreSplitsPlanNodeId);
                        continue;
                    }
                    ExchangeSourceOutputSelector selector = outputSelectors.get(noMoreSplitsPlanNodeId);
                    if (selector != null && selector.isFinal()) {
                        noMoreSplits.add(noMoreSplitsPlanNodeId);
                    }
                }
            }
            else {
                Optional<TaskDescriptor> taskDescriptor = taskDescriptorStorage.get(stage.getStageId(), partitionId);
                verify(taskDescriptor.isPresent() || queryStateMachine.getQueryState().isDone(), "task descriptor is closed but query is not in finished state");
                if (taskDescriptor.isEmpty()) {
                    // query execution is finished
                    return Optional.empty();
                }
                splits.putAll(taskDescriptor.get().getSplits());
                noMoreSplits.addAll(stage.getFragment().getPartitionedSources());
                for (RemoteSourceNode remoteSource : stage.getFragment().getRemoteSourceNodes()) {
                    ExchangeSourceOutputSelector selector = outputSelectors.get(remoteSource.getId());
                    if (selector != null && selector.isFinal()) {
                        noMoreSplits.add(remoteSource.getId());
                    }
                }
            }

            outputSelectors.forEach(((planNodeId, outputSelector) -> splits.put(planNodeId, createOutputSelectorSplit(outputSelector))));

            int attempt = maxTaskExecutionAttempts - partition.getRemainingAttempts();
            ExchangeSinkInstanceHandle exchangeSinkInstanceHandle = exchange.instantiateSink(partition.getExchangeSinkHandle(), attempt);
            SpoolingOutputBuffers outputBuffers = SpoolingOutputBuffers.createInitial(exchangeSinkInstanceHandle, sinkPartitioningScheme.getPartitionCount());
            Optional<RemoteTask> task = stage.createTask(
                    node,
                    partitionId,
                    attempt,
                    sinkPartitioningScheme.getBucketToPartitionMap(),
                    outputBuffers,
                    splits,
                    noMoreSplits,
                    Optional.of(partition.getMemoryRequirements().getRequiredMemory()));
            task.ifPresent(remoteTask -> partition.addTask(remoteTask, outputBuffers));
            return task;
        }

        public void updateExchangeSinkInstanceHandle(TaskId taskId)
        {
            if (getState().isDone()) {
                return;
            }
            StagePartition partition = getStagePartition(taskId.getPartitionId());
            ExchangeSinkInstanceHandle exchangeSinkInstanceHandle = exchange.updateSinkInstanceHandle(partition.getExchangeSinkHandle(), taskId.getAttemptId());
            partition.updateExchangeSinkInstanceHandle(taskId, exchangeSinkInstanceHandle);
        }

        public void taskFinished(TaskId taskId, TaskStatus taskStatus)
        {
            if (getState().isDone()) {
                return;
            }

            int partitionId = taskId.getPartitionId();
            StagePartition partition = getStagePartition(partitionId);
            exchange.sinkFinished(partition.getExchangeSinkHandle(), taskId.getAttemptId());
            SpoolingOutputStats.Snapshot outputStats = partition.taskFinished(taskId);

            if (!remainingPartitions.remove(partitionId)) {
                // a different task for the same partition finished before
                return;
            }

            updateOutputSize(outputStats);
            taskDescriptorStorage.remove(stage.getStageId(), partitionId);

            partitionMemoryEstimator.registerPartitionFinished(
                    queryStateMachine.getSession(),
                    partition.getMemoryRequirements(),
                    taskStatus.getPeakMemoryReservation(),
                    true,
                    Optional.empty());

            sinkOutputSelectorBuilder.include(exchange.getId(), taskId.getPartitionId(), taskId.getAttemptId());

            if (noMorePartitions && remainingPartitions.isEmpty() && !stage.getState().isDone()) {
                // TODO: support task splitting
                dynamicFilterService.stageCannotScheduleMoreTasks(stage.getStageId(), 0, partitions.size());
                exchange.noMoreSinks();
                exchange.allRequiredSinksFinished();
                verify(finalSinkOutputSelector == null, "finalOutputSelector is already set");
                sinkOutputSelectorBuilder.setPartitionCount(exchange.getId(), partitions.size());
                sinkOutputSelectorBuilder.setFinal();
                finalSinkOutputSelector = sinkOutputSelectorBuilder.build();
                sinkOutputSelectorBuilder = null;
                stage.finish();
            }
        }

        private void updateOutputSize(SpoolingOutputStats.Snapshot taskOutputStats)
        {
            for (int partitionId = 0; partitionId < sinkPartitioningScheme.getPartitionCount(); partitionId++) {
                long partitionSizeInBytes = taskOutputStats.getPartitionSizeInBytes(partitionId);
                checkArgument(partitionSizeInBytes >= 0, "partitionSizeInBytes must be greater than or equal to zero: %s", partitionSizeInBytes);
                outputDataSize[partitionId] += partitionSizeInBytes;
            }
        }

        public List<ScheduledTask> taskFailed(TaskId taskId, ExecutionFailureInfo failureInfo, TaskStatus taskStatus)
        {
            if (getState().isDone()) {
                return ImmutableList.of();
            }

            int partitionId = taskId.getPartitionId();
            StagePartition partition = getStagePartition(partitionId);
            partition.taskFailed(taskId);

            RuntimeException failure = failureInfo.toException();
            ErrorCode errorCode = failureInfo.getErrorCode();
            partitionMemoryEstimator.registerPartitionFinished(
                    queryStateMachine.getSession(),
                    partition.getMemoryRequirements(),
                    taskStatus.getPeakMemoryReservation(),
                    false,
                    Optional.ofNullable(errorCode));

            // update memory limits for next attempt
            MemoryRequirements currentMemoryLimits = partition.getMemoryRequirements();
            MemoryRequirements newMemoryLimits = partitionMemoryEstimator.getNextRetryMemoryRequirements(
                    queryStateMachine.getSession(),
                    partition.getMemoryRequirements(),
                    taskStatus.getPeakMemoryReservation(),
                    errorCode);
            partition.setMemoryRequirements(newMemoryLimits);
            log.debug(
                    "Computed next memory requirements for task from stage %s; previous=%s; new=%s; peak=%s; estimator=%s",
                    stage.getStageId(),
                    currentMemoryLimits,
                    newMemoryLimits,
                    taskStatus.getPeakMemoryReservation(),
                    partitionMemoryEstimator);

            if (errorCode != null && isOutOfMemoryError(errorCode) && newMemoryLimits.getRequiredMemory().toBytes() * 0.99 <= taskStatus.getPeakMemoryReservation().toBytes()) {
                String message = format(
                        "Cannot allocate enough memory for task %s. Reported peak memory reservation: %s. Maximum possible reservation: %s.",
                        taskId,
                        taskStatus.getPeakMemoryReservation(),
                        newMemoryLimits.getRequiredMemory());
                stage.fail(new TrinoException(() -> errorCode, message, failure));
                return ImmutableList.of();
            }

            if (partition.getRemainingAttempts() == 0 || (errorCode != null && errorCode.getType() == USER_ERROR)) {
                stage.fail(failure);
                // stage failed, don't reschedule
                return ImmutableList.of();
            }

            if (partition.getOpenTaskDescriptor().isPresent()) {
                // don't reschedule speculative tasks
                return ImmutableList.of();
            }

            // TODO: split into smaller partitions here if necessary (for example if a task for a given partition failed with out of memory)

            // reschedule a task
            return ImmutableList.of(new ScheduledTask(stage.getStageId(), partitionId, schedulingPriority));
        }

        public Optional<NodeRequirements> getNodeRequirements(int partitionId)
        {
            StagePartition partition = getStagePartition(partitionId);
            Optional<OpenTaskDescriptor> openTaskDescriptor = partition.getOpenTaskDescriptor();
            if (openTaskDescriptor.isPresent()) {
                return openTaskDescriptor.map(OpenTaskDescriptor::getNodeRequirements);
            }
            Optional<TaskDescriptor> taskDescriptor = taskDescriptorStorage.get(getStageId(), partitionId);
            if (taskDescriptor.isPresent()) {
                return taskDescriptor.map(TaskDescriptor::getNodeRequirements);
            }
            return Optional.empty();
        }

        public MemoryRequirements getMemoryRequirements(int partitionId)
        {
            StagePartition partition = getStagePartition(partitionId);
            return partition.getMemoryRequirements();
        }

        public OutputDataSizeEstimate getOutputDataSize()
        {
            // TODO enable speculative execution
            checkState(stage.getState() == StageState.FINISHED, "stage %s is expected to be in FINISHED state, got %s", stage.getStageId(), stage.getState());
            return new OutputDataSizeEstimate(ImmutableLongArray.copyOf(outputDataSize));
        }

        public ExchangeSourceOutputSelector getSinkOutputSelector()
        {
            if (finalSinkOutputSelector != null) {
                return finalSinkOutputSelector;
            }
            return sinkOutputSelectorBuilder.build();
        }

        public void setSourceOutputSelector(PlanFragmentId sourceFragmentId, ExchangeSourceOutputSelector selector)
        {
            sourceOutputSelectors.put(sourceFragmentId, selector);
            RemoteSourceNode remoteSourceNode = remoteSources.get(sourceFragmentId);
            verify(remoteSourceNode != null, "remoteSourceNode is null for fragment: %s", sourceFragmentId);
            ExchangeSourceOutputSelector mergedSelector = selector;
            for (PlanFragmentId fragmentId : remoteSourceNode.getSourceFragmentIds()) {
                if (fragmentId.equals(sourceFragmentId)) {
                    continue;
                }
                ExchangeSourceOutputSelector fragmentSelector = sourceOutputSelectors.get(fragmentId);
                if (fragmentSelector != null) {
                    mergedSelector = mergedSelector.merge(fragmentSelector);
                }
            }
            ExchangeSourceOutputSelector finalMergedSelector = mergedSelector;
            remainingPartitions.forEach((java.util.function.IntConsumer) value -> {
                StagePartition partition = partitions.get(value);
                verify(partition != null, "partition not found: %s", value);
                partition.updateExchangeSourceOutputSelector(remoteSourceNode.getId(), finalMergedSelector);
            });
        }

        public void abort()
        {
            Closer closer = createStageExecutionCloser();
            closer.register(stage::abort);
            try {
                closer.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public void fail(Throwable t)
        {
            Closer closer = createStageExecutionCloser();
            closer.register(() -> stage.fail(t));
            try {
                closer.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private Closer createStageExecutionCloser()
        {
            Closer closer = Closer.create();
            closer.register(taskSource);
            closer.register(exchange);
            return closer;
        }

        private StagePartition getStagePartition(int partitionId)
        {
            StagePartition partition = partitions.get(partitionId);
            checkState(partition != null, "partition with id %s does not exist in stage %s", partitionId, stage.getStageId());
            return partition;
        }
    }

    private static class StagePartition
    {
        private final int partitionId;
        private final ExchangeSinkHandle exchangeSinkHandle;
        private final Set<PlanNodeId> remoteSourceIds;

        // empty when task descriptor is closed and stored in TaskDescriptorStorage
        private Optional<OpenTaskDescriptor> openTaskDescriptor;
        private MemoryRequirements memoryRequirements;
        private int remainingAttempts;

        private final Map<TaskId, RemoteTask> tasks = new HashMap<>();
        private final Map<TaskId, SpoolingOutputBuffers> taskOutputBuffers = new HashMap<>();
        private final Set<TaskId> runningTasks = new HashSet<>();
        private final Set<PlanNodeId> finalSelectors = new HashSet<>();
        private final Set<PlanNodeId> noMoreSplits = new HashSet<>();
        private boolean finished;

        public StagePartition(
                int partitionId,
                ExchangeSinkHandle exchangeSinkHandle,
                Set<PlanNodeId> remoteSourceIds,
                NodeRequirements nodeRequirements,
                MemoryRequirements memoryRequirements,
                int maxTaskExecutionAttempts)
        {
            this.partitionId = partitionId;
            this.exchangeSinkHandle = requireNonNull(exchangeSinkHandle, "exchangeSinkHandle is null");
            this.remoteSourceIds = ImmutableSet.copyOf(requireNonNull(remoteSourceIds, "remoteSourceIds is null"));
            requireNonNull(nodeRequirements, "nodeRequirements is null");
            this.openTaskDescriptor = Optional.of(new OpenTaskDescriptor(ImmutableListMultimap.of(), ImmutableSet.of(), nodeRequirements));
            this.memoryRequirements = requireNonNull(memoryRequirements, "memoryRequirements is null");
            this.remainingAttempts = maxTaskExecutionAttempts;
        }

        public int getPartitionId()
        {
            return partitionId;
        }

        public ExchangeSinkHandle getExchangeSinkHandle()
        {
            return exchangeSinkHandle;
        }

        public Optional<OpenTaskDescriptor> getOpenTaskDescriptor()
        {
            return openTaskDescriptor;
        }

        public void updateOpenTaskDescriptor(PlanNodeId planNodeId, List<Split> splits, boolean noMoreSplits)
        {
            checkState(openTaskDescriptor.isPresent(), "openTaskDescriptor is empty");
            openTaskDescriptor = Optional.of(openTaskDescriptor.get().update(planNodeId, splits, noMoreSplits));
            if (noMoreSplits) {
                this.noMoreSplits.add(planNodeId);
            }
            for (RemoteTask task : tasks.values()) {
                task.addSplits(ImmutableListMultimap.<PlanNodeId, Split>builder()
                        .putAll(planNodeId, splits)
                        .build());
                if (noMoreSplits && (!remoteSourceIds.contains(planNodeId) || finalSelectors.contains(planNodeId))) {
                    task.noMoreSplits(planNodeId);
                }
            }
        }

        public TaskDescriptor sealTaskDescriptor(int partitionId)
        {
            checkState(openTaskDescriptor.isPresent(), "openTaskDescriptor is empty");
            TaskDescriptor taskDescriptor = openTaskDescriptor.get().createTaskDescriptor(partitionId);
            openTaskDescriptor = Optional.empty();
            return taskDescriptor;
        }

        public MemoryRequirements getMemoryRequirements()
        {
            return memoryRequirements;
        }

        public void setMemoryRequirements(MemoryRequirements memoryRequirements)
        {
            this.memoryRequirements = requireNonNull(memoryRequirements, "memoryRequirements is null");
        }

        public int getRemainingAttempts()
        {
            return remainingAttempts;
        }

        public void addTask(RemoteTask remoteTask, SpoolingOutputBuffers outputBuffers)
        {
            TaskId taskId = remoteTask.getTaskId();
            tasks.put(taskId, remoteTask);
            taskOutputBuffers.put(taskId, outputBuffers);
            runningTasks.add(taskId);
        }

        public SpoolingOutputStats.Snapshot taskFinished(TaskId taskId)
        {
            RemoteTask remoteTask = tasks.get(taskId);
            checkArgument(remoteTask != null, "task not found: %s", taskId);
            SpoolingOutputStats.Snapshot outputStats = remoteTask.retrieveAndDropSpoolingOutputStats();
            runningTasks.remove(taskId);
            tasks.values().forEach(RemoteTask::abort);
            finished = true;
            return outputStats;
        }

        public void taskFailed(TaskId taskId)
        {
            runningTasks.remove(taskId);
            remainingAttempts--;
        }

        public void updateExchangeSinkInstanceHandle(TaskId taskId, ExchangeSinkInstanceHandle handle)
        {
            SpoolingOutputBuffers outputBuffers = taskOutputBuffers.get(taskId);
            checkArgument(outputBuffers != null, "output buffers not found: %s", taskId);
            RemoteTask remoteTask = tasks.get(taskId);
            checkArgument(remoteTask != null, "task not found: %s", taskId);
            SpoolingOutputBuffers updatedOutputBuffers = outputBuffers.withExchangeSinkInstanceHandle(handle);
            taskOutputBuffers.put(taskId, updatedOutputBuffers);
            remoteTask.setOutputBuffers(updatedOutputBuffers);
        }

        public void updateExchangeSourceOutputSelector(PlanNodeId planNodeId, ExchangeSourceOutputSelector selector)
        {
            if (selector.isFinal()) {
                finalSelectors.add(planNodeId);
            }
            for (TaskId taskId : runningTasks) {
                RemoteTask task = tasks.get(taskId);
                verify(task != null, "task is null: %s", taskId);
                task.addSplits(ImmutableListMultimap.of(
                        planNodeId,
                        createOutputSelectorSplit(selector)));
                if (selector.isFinal() && noMoreSplits.contains(planNodeId)) {
                    task.noMoreSplits(planNodeId);
                }
            }
        }

        public boolean isRunning()
        {
            return !runningTasks.isEmpty();
        }

        public boolean isFinished()
        {
            return finished;
        }
    }

    private static Split createOutputSelectorSplit(ExchangeSourceOutputSelector selector)
    {
        return new Split(REMOTE_CATALOG_HANDLE, new RemoteSplit(new SpoolingExchangeInput(ImmutableList.of(), Optional.of(selector))));
    }

    private static class OpenTaskDescriptor
    {
        private final ListMultimap<PlanNodeId, Split> splits;
        private final Set<PlanNodeId> noMoreSplits;
        private final NodeRequirements nodeRequirements;

        private OpenTaskDescriptor(ListMultimap<PlanNodeId, Split> splits, Set<PlanNodeId> noMoreSplits, NodeRequirements nodeRequirements)
        {
            this.splits = ImmutableListMultimap.copyOf(requireNonNull(splits, "splits is null"));
            this.noMoreSplits = ImmutableSet.copyOf(requireNonNull(noMoreSplits, "noMoreSplits is null"));
            this.nodeRequirements = requireNonNull(nodeRequirements, "nodeRequirements is null");
        }

        public ListMultimap<PlanNodeId, Split> getSplits()
        {
            return splits;
        }

        public Set<PlanNodeId> getNoMoreSplits()
        {
            return noMoreSplits;
        }

        public NodeRequirements getNodeRequirements()
        {
            return nodeRequirements;
        }

        public OpenTaskDescriptor update(PlanNodeId planNodeId, List<Split> splits, boolean noMoreSplits)
        {
            ListMultimap<PlanNodeId, Split> updatedSplits = ImmutableListMultimap.<PlanNodeId, Split>builder()
                    .putAll(this.splits)
                    .putAll(planNodeId, splits)
                    .build();

            Set<PlanNodeId> updatedNoMoreSplits = this.noMoreSplits;
            if (noMoreSplits && !updatedNoMoreSplits.contains(planNodeId)) {
                updatedNoMoreSplits = ImmutableSet.<PlanNodeId>builder()
                        .addAll(this.noMoreSplits)
                        .add(planNodeId)
                        .build();
            }
            return new OpenTaskDescriptor(
                    updatedSplits,
                    updatedNoMoreSplits,
                    nodeRequirements);
        }

        public TaskDescriptor createTaskDescriptor(int partitionId)
        {
            Set<PlanNodeId> missingNoMoreSplits = Sets.difference(splits.keySet(), noMoreSplits);
            checkState(missingNoMoreSplits.isEmpty(), "missing no more splits for plan nodes: %s", missingNoMoreSplits);
            return new TaskDescriptor(
                    partitionId,
                    splits,
                    nodeRequirements);
        }
    }

    private static class ScheduledTask
    {
        private final StageId stageId;
        private final int partitionId;
        private final int priority;

        private ScheduledTask(StageId stageId, int partitionId, int priority)
        {
            this.stageId = requireNonNull(stageId, "stageId is null");
            this.partitionId = partitionId;
            this.priority = priority;
        }

        public StageId getStageId()
        {
            return stageId;
        }

        public int getPartitionId()
        {
            return partitionId;
        }

        public int getPriority()
        {
            return priority;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ScheduledTask that = (ScheduledTask) o;
            return partitionId == that.partitionId && Objects.equals(stageId, that.stageId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(stageId, partitionId);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("stageId", stageId)
                    .add("partitionId", partitionId)
                    .add("priority", priority)
                    .toString();
        }
    }

    private interface Event
    {
        Event ABORT = listener -> {
            throw new UnsupportedOperationException();
        };

        Event WAKE_UP = listener -> {
            throw new UnsupportedOperationException();
        };

        void accept(EventListener listener);
    }

    private interface EventListener
    {
        void onRemoteTaskCompleted(RemoteTaskCompletedEvent event);

        void onRemoteTaskExchangeSinkInstanceHandleUpdateRequiredEvent(RemoteTaskExchangeSinkInstanceHandleUpdateRequiredEvent event);

        void onPartitionAddedTaskSourceEvent(PartitionsAddedTaskSourceEvent event);

        void onPartitionUpdatedTaskSourceEvent(PartitionsUpdatedTaskSourceEvent event);

        void onPartitionSealedTaskSourceEvent(PartitionsSealedTaskSourceEvent event);

        void onNoMorePartitionsTaskSourceEvent(NoMorePartitionsTaskSourceEvent event);

        void onFailedTaskSourceEvent(FailedTaskSourceEvent event);
    }

    private abstract static class RemoteTaskEvent
            implements Event
    {
        private final TaskStatus taskStatus;

        protected RemoteTaskEvent(TaskStatus taskStatus)
        {
            this.taskStatus = requireNonNull(taskStatus, "taskStatus is null");
        }

        public TaskStatus getTaskStatus()
        {
            return taskStatus;
        }
    }

    private static class RemoteTaskCompletedEvent
            extends RemoteTaskEvent
    {
        public RemoteTaskCompletedEvent(TaskStatus taskStatus)
        {
            super(taskStatus);
        }

        @Override
        public void accept(EventListener listener)
        {
            listener.onRemoteTaskCompleted(this);
        }
    }

    private static class RemoteTaskExchangeSinkInstanceHandleUpdateRequiredEvent
            extends RemoteTaskEvent
    {
        protected RemoteTaskExchangeSinkInstanceHandleUpdateRequiredEvent(TaskStatus taskStatus)
        {
            super(taskStatus);
        }

        @Override
        public void accept(EventListener listener)
        {
            listener.onRemoteTaskExchangeSinkInstanceHandleUpdateRequiredEvent(this);
        }
    }

    private abstract static class TaskSourceEvent
            implements Event
    {
        private final StageId stageId;

        protected TaskSourceEvent(StageId stageId)
        {
            this.stageId = requireNonNull(stageId, "stageId is null");
        }

        public StageId getStageId()
        {
            return stageId;
        }
    }

    private static class PartitionsAddedTaskSourceEvent
            extends TaskSourceEvent
    {
        private final List<Partition> partitions;

        public PartitionsAddedTaskSourceEvent(StageId stageId, List<Partition> partitions)
        {
            super(stageId);
            this.partitions = ImmutableList.copyOf(requireNonNull(partitions, "partitions is null"));
        }

        public List<Partition> getPartitions()
        {
            return partitions;
        }

        @Override
        public void accept(EventListener listener)
        {
            listener.onPartitionAddedTaskSourceEvent(this);
        }
    }

    private static class NoMorePartitionsTaskSourceEvent
            extends TaskSourceEvent
    {
        public NoMorePartitionsTaskSourceEvent(StageId stageId)
        {
            super(stageId);
        }

        @Override
        public void accept(EventListener listener)
        {
            listener.onNoMorePartitionsTaskSourceEvent(this);
        }
    }

    private static class PartitionsUpdatedTaskSourceEvent
            extends TaskSourceEvent
    {
        private final List<PartitionUpdate> partitionUpdates;

        public PartitionsUpdatedTaskSourceEvent(StageId stageId, List<PartitionUpdate> partitionUpdates)
        {
            super(stageId);
            this.partitionUpdates = ImmutableList.copyOf(requireNonNull(partitionUpdates, "partitionUpdates is null"));
        }

        public List<PartitionUpdate> getPartitionUpdates()
        {
            return partitionUpdates;
        }

        @Override
        public void accept(EventListener listener)
        {
            listener.onPartitionUpdatedTaskSourceEvent(this);
        }
    }

    private static class PartitionsSealedTaskSourceEvent
            extends TaskSourceEvent
    {
        private final ImmutableIntArray partitionIds;

        public PartitionsSealedTaskSourceEvent(StageId stageId, ImmutableIntArray partitionIds)
        {
            super(stageId);
            this.partitionIds = requireNonNull(partitionIds, "partitionIds is null");
        }

        public ImmutableIntArray getPartitionIds()
        {
            return partitionIds;
        }

        @Override
        public void accept(EventListener listener)
        {
            listener.onPartitionSealedTaskSourceEvent(this);
        }
    }

    private static class FailedTaskSourceEvent
            extends TaskSourceEvent
    {
        private final Throwable failure;

        public FailedTaskSourceEvent(StageId stageId, Throwable failure)
        {
            super(stageId);
            this.failure = requireNonNull(failure, "failure is null");
        }

        public Throwable getFailure()
        {
            return failure;
        }

        @Override
        public void accept(EventListener listener)
        {
            listener.onFailedTaskSourceEvent(this);
        }
    }
}
