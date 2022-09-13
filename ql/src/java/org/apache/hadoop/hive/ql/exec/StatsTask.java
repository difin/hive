/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.ql.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.stats.BasicStatsNoJobTask;
import org.apache.hadoop.hive.ql.stats.BasicStatsTask;
import org.apache.hadoop.hive.ql.stats.ColStatsProcessor;
import org.apache.hadoop.hive.ql.stats.IStatsProcessor;
import org.apache.thrift.TException;

import com.cronutils.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * StatsTask implementation.
 **/

public class StatsTask extends Task<StatsWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static transient final Logger LOG = LoggerFactory.getLogger(StatsTask.class);
  private static final long LOCK_CHECK_MIN_WAIT_MS = 50; // 50 milliseconds
  private static final long LOCK_CHECK_MAX_WAIT_MS = 5 * 1000; // 5 seconds
  private static final double LOCK_CHECK_BACKOFF_FACTOR = 1.5; // will reach max wait after ~ 11 attempts

  private boolean shouldProcessUnderHMSLock;
  private long acquireLockTimeoutMs;

  public StatsTask() {
    super();
  }

  List<IStatsProcessor> processors = new ArrayList<>();

  @Override
  public void initialize(QueryState queryState, QueryPlan queryPlan, TaskQueue taskQueue, Context context) {
    super.initialize(queryState, queryPlan, taskQueue, context);

    if (work.getBasicStatsWork() != null) {
      BasicStatsTask task = new BasicStatsTask(conf, work.getBasicStatsWork());
      task.followedColStats = work.hasColStats();
      processors.add(0, task);
    } else if (work.isFooterScan()) {
      BasicStatsNoJobTask t = new BasicStatsNoJobTask(conf, work.getBasicStatsNoJobWork());
      processors.add(0, t);
    }
    if (work.hasColStats()) {
      processors.add(new ColStatsProcessor(work.getColStats(), conf));
    }

    for (IStatsProcessor p : processors) {
      p.initialize(context.getOpContext());
    }
    shouldProcessUnderHMSLock = HiveConf.getBoolVar(conf, ConfVars.HIVE_STATS_LOCK_ENABLED);
    acquireLockTimeoutMs = HiveConf.getTimeVar(conf, ConfVars.HIVE_STATS_LOCK_TIMEOUT, TimeUnit.MILLISECONDS);
  }


  @Override
  public int execute() {
    if (context.getExplainAnalyze() == AnalyzeState.RUNNING) {
      return 0;
    }
    if (work.isAggregating() && work.isFooterScan()) {
      throw new RuntimeException("Can not have both basic stats work and stats no job work!");
    }
    int ret = 0;
    Optional<Long> lockId = Optional.empty();
    Hive db = getHive();

    try {

      if (work.isFooterScan()) {
        work.getBasicStatsNoJobWork().setPartitions(work.getPartitions());
      }

      if (shouldProcessUnderHMSLock) {
        lockId = Optional.of(acquireLock(db));
      }

      Table tbl = getTable(db);

      for (IStatsProcessor task : processors) {
        task.setDpPartSpecs(dpPartSpecs);
        ret = task.process(db, tbl);
        if (ret != 0) {
          return ret;
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to run stats task", e);
      setException(e);
      return 1;
    } finally {
      unlock(getHive(), lockId);
    }
    return 0;
  }


  private Table getTable(Hive db) throws SemanticException, HiveException {
    return db.getTable(work.getFullTableName());
  }

  @Override
  public StageType getType() {
    return StageType.STATS;
  }

  @Override
  public String getName() {
    return "STATS TASK";
  }

  private Collection<Partition> dpPartSpecs;

  @Override
  protected void receiveFeed(FeedType feedType, Object feedValue) {
    // this method should be called by MoveTask when there are dynamic
    // partitions generated
    if (feedType == FeedType.DYNAMIC_PARTITIONS) {
      dpPartSpecs = (Collection<Partition>) feedValue;
    }
  }

  public static ExecutorService newThreadPool(HiveConf conf) {
    int numThreads = HiveConf.getIntVar(conf, ConfVars.HIVE_STATS_GATHER_NUM_THREADS);

    ExecutorService executor = Executors.newFixedThreadPool(numThreads, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("StatsNoJobTask-Thread-%d").build());
    LOG.info("Initialized threadpool for stats computation with {} threads", numThreads);
    return executor;
  }

  @VisibleForTesting
  long acquireLock(Hive hive) throws UnknownHostException, TException, InterruptedException, HiveException {
    final LockComponent lockComponent = new LockComponent(LockType.EXCL_WRITE, LockLevel.TABLE, work.getCurrentDatabaseName());
    lockComponent.setTablename(work.getTable().getTableName());
    final LockRequest lockRequest = new LockRequest(Lists.newArrayList(lockComponent),
        System.getProperty("user.name"),
        InetAddress.getLocalHost().getHostName());

    LockResponse lockResponse = hive.getMSC().lock(lockRequest);
    LockState state = lockResponse.getState();
    long lockId = lockResponse.getLockid();

    long timeToSleep = LOCK_CHECK_MIN_WAIT_MS;
    final long start = System.currentTimeMillis();

    try {
      while (true) {
        if (!state.equals(LockState.WAITING)) {
          break;
        }
        if (System.currentTimeMillis() - start > acquireLockTimeoutMs) {
          throw new HiveException(String.format("Timed out after %s ms waiting for lock on %s", acquireLockTimeoutMs, work.getFullTableName()));
        }
        Thread.sleep(timeToSleep);
        timeToSleep =
            timeToSleep * LOCK_CHECK_BACKOFF_FACTOR > LOCK_CHECK_MAX_WAIT_MS ?
                LOCK_CHECK_MAX_WAIT_MS :
                (long) (timeToSleep * LOCK_CHECK_BACKOFF_FACTOR);
        lockResponse = hive.getMSC().checkLock(lockId);
        state = lockResponse.getState();
      }
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted while waiting for acquiring lock on {}", work.getFullTableName());
    } finally {
      if (!state.equals(LockState.ACQUIRED)) {
        unlock(hive, Optional.of(lockId));
      }
    }

    if (!state.equals(LockState.ACQUIRED)) {
      throw new HiveException(String.format("Could not acquire the lock on %s, " +
          "lock request ended in state %s", work.getFullTableName(), state));
    }
    return lockId;
  }

  private void unlock(Hive hive, Optional<Long> lockId) {
    if (lockId.isPresent()) {
      try {
        hive.getMSC().unlock(lockId.get());
      } catch (Exception e) {
        LOG.warn("Failed to unlock {}", work.getFullTableName(), e);
      }
    }
  }


}
