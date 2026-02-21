/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.flink.source;

import io.delta.flink.table.DeltaTable;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DeltaSource<T> implements Source<T, DeltaSourceSplit, DeltaEnumeratorState> {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaSource.class);

    // cache the discovered splits by planSplitsForBatch, which can be called twice. And they come
    // from two different threads: (1) source/stream construction by main thread (2) enumerator
    // creation. Hence need volatile here.
    private volatile List<DeltaSourceSplit> batchSplits;

    private final DeltaTable table;
    private final DeltaSourceConf conf;

    DeltaSource(DeltaTable table, DeltaSourceConf conf) {
        this.table = table;
        this.conf = conf;
    }

    String name() {
        return "DeltaSource-" + tableName;
    }

    private String planningThreadName() {
        // Ideally, operatorId should be used as the threadPoolName as Flink guarantees its uniqueness
        // within a job. SplitEnumeratorContext doesn't expose the OperatorCoordinator.Context, which
        // would contain the OperatorID. Need to discuss with Flink community whether it is ok to expose
        // a public API like the protected method "OperatorCoordinator.Context getCoordinatorContext()"
        // from SourceCoordinatorContext implementation. For now, <table name>-<random UUID> is used as
        // the unique thread pool name.
        return tableName + "-" + UUID.randomUUID();
    }

    /**
     * Cache the enumerated splits for batch execution to avoid double planning as there are two code
     * paths obtaining splits: (1) infer parallelism (2) enumerator creation.
     */
    private List<DeltaSourceSplit> planSplitsForBatch(String threadName) {
        if (batchSplits != null) {
            return batchSplits;
        }

        ExecutorService workerPool =
                ThreadPools.newFixedThreadPool(threadName, scanContext.planParallelism());
        try (TableLoader loader = tableLoader.clone()) {
            loader.open();
            this.batchSplits =
                    FlinkSplitPlanner.planIcebergSourceSplits(loader.loadTable(), scanContext, workerPool);
            LOG.info(
                    "Discovered {} splits from table {} during job initialization",
                    batchSplits.size(),
                    tableName);
            return batchSplits;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close table loader", e);
        } finally {
            workerPool.shutdown();
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return conf.isBounded() ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, DeltaSourceSplit> createReader(SourceReaderContext readerContext) {
        IcebergSourceReaderMetrics metrics =
                new IcebergSourceReaderMetrics(readerContext.metricGroup(), tableName);
        return new DeltaSourceReader<>(
                emitter, metrics, readerFunction, splitComparator, readerContext);
    }

    @Override
    public SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorState> createEnumerator(
            SplitEnumeratorContext<DeltaSourceSplit> enumContext) {
        return createEnumerator(enumContext, null);
    }

    @Override
    public SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<DeltaSourceSplit> enumContext, DeltaEnumeratorState enumState) {
        return createEnumerator(enumContext, enumState);
    }

    @Override
    public SimpleVersionedSerializer<DeltaSourceSplit> getSplitSerializer() {
        return new DeltaSourceSplitSerializer(scanContext.caseSensitive());
    }

    @Override
    public SimpleVersionedSerializer<DeltaEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new DeltaEnumeratorStateSerializer(scanContext.caseSensitive());
    }

    private SplitEnumerator<DeltaSourceSplit, DeltaEnumeratorState> createEnumerator(
            SplitEnumeratorContext<DeltaSourceSplit> enumContext,
            @Nullable DeltaEnumeratorState enumState) {
        SplitAssigner assigner;
        if (enumState == null) {
            assigner = assignerFactory.createAssigner();
        } else {
            LOG.info(
                    "Iceberg source restored {} splits from state for table {}",
                    enumState.pendingSplits().size(),
                    tableName);
            assigner = assignerFactory.createAssigner(enumState.pendingSplits());
        }
        if (scanContext.isStreaming()) {
            ContinuousSplitPlanner splitPlanner =
                    new ContinuousSplitPlannerImpl(tableLoader, scanContext, planningThreadName());
            return new ContinuousIcebergEnumerator(
                    enumContext, assigner, scanContext, splitPlanner, enumState);
        } else {
            if (enumState == null) {
                // Only do scan planning if nothing is restored from checkpoint state
                List<IcebergSourceSplit> splits = planSplitsForBatch(planningThreadName());
                assigner.onDiscoveredSplits(splits);
                // clear the cached splits after enumerator creation as they won't be needed anymore
                this.batchSplits = null;
            }

            return new StaticIcebergEnumerator(enumContext, assigner);
        }
    }

    private boolean shouldInferParallelism() {
        return !scanContext.isStreaming();
    }

    private int inferParallelism(ReadableConfig flinkConf, StreamExecutionEnvironment env) {
        int parallelism =
                SourceUtil.inferParallelism(
                        flinkConf,
                        scanContext.limit(),
                        () -> {
                            List<DeltaSourceSplit> splits = planSplitsForBatch(planningThreadName());
                            return splits.size();
                        });

        if (env.getMaxParallelism() > 0) {
            parallelism = Math.min(parallelism, env.getMaxParallelism());
        }

        return parallelism;
    }
}