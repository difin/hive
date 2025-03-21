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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.conf.HiveConf.shouldComputeLineage;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveOpConverterPostProc;
import org.apache.hadoop.hive.ql.optimizer.correlation.CorrelationOptimizer;
import org.apache.hadoop.hive.ql.optimizer.correlation.ReduceSinkDeDuplication;
import org.apache.hadoop.hive.ql.optimizer.lineage.Generator;
import org.apache.hadoop.hive.ql.optimizer.listbucketingpruner.ListBucketingPruner;
import org.apache.hadoop.hive.ql.optimizer.metainfo.annotation.AnnotateWithOpTraits;
import org.apache.hadoop.hive.ql.optimizer.pcr.PartitionConditionRemover;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.optimizer.stats.annotation.AnnotateWithStatistics;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcessor;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.ppd.PredicatePushDown;
import org.apache.hadoop.hive.ql.ppd.PredicateTransitivePropagate;
import org.apache.hadoop.hive.ql.ppd.SimplePredicatePushDown;
import org.apache.hadoop.hive.ql.ppd.SyntheticJoinPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the optimizer.
 */
public class Optimizer {
  private ParseContext pctx;
  private List<Transform> transformations;
  private static final Logger LOG = LoggerFactory.getLogger(Optimizer.class.getName());

  /**
   * Create the list of transformations.
   *
   * @param hiveConf
   */
  public void initialize(HiveConf hiveConf) {

    boolean isTezExecEngine = HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez");
    boolean bucketMapJoinOptimizer = false;

    transformations = new ArrayList<Transform>();

    // Add the additional postprocessing transformations needed if
    // we are translating Calcite operators into Hive operators.
    transformations.add(new HiveOpConverterPostProc());

    // Add the transformation that computes the lineage information.
    if (shouldComputeLineage(hiveConf)) {
      transformations.add(Generator.fromConf(hiveConf));
    }

    // Try to transform OR predicates in Filter into simpler IN clauses first
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_POINT_LOOKUP_OPTIMIZER) &&
            !pctx.getContext().isCboSucceeded()) {
      final int min = HiveConf.getIntVar(hiveConf,
          HiveConf.ConfVars.HIVE_POINT_LOOKUP_OPTIMIZER_MIN);
      transformations.add(new PointLookupOptimizer(min));
    }

    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_PARTITION_COLUMN_SEPARATOR)) {
        transformations.add(new PartitionColumnsSeparator());
    }

    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_PPD) &&
            !pctx.getContext().isCboSucceeded()) {
      transformations.add(new PredicateTransitivePropagate());
      if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_CONSTANT_PROPAGATION)) {
        transformations.add(new ConstantPropagate());
      }
      transformations.add(new SyntheticJoinPredicate());
      transformations.add(new PredicatePushDown());
    } else if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_PPD) &&
            pctx.getContext().isCboSucceeded()) {
      transformations.add(new SyntheticJoinPredicate());
      transformations.add(new SimplePredicatePushDown());
      transformations.add(new RedundantDynamicPruningConditionsRemoval());
    }

    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_CONSTANT_PROPAGATION) &&
        (!pctx.getContext().isCboSucceeded() || pctx.getContext().getOperation() == Context.Operation.MERGE)) {
      // We run constant propagation twice because after predicate pushdown, filter expressions
      // are combined and may become eligible for reduction (like is not null filter).
      // CBO can not handle merge statements and some constraint check can be evaluated by constant propagation
      transformations.add(new ConstantPropagate());
    }



    transformations.add(new SortedDynPartitionTimeGranularityOptimizer());

    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_PPD)) {
      transformations.add(new PartitionPruner());
      transformations.add(new PartitionConditionRemover());
      if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_LIST_BUCKETING)) {
        /* Add list bucketing pruner. */
        transformations.add(new ListBucketingPruner());
      }
      if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_CONSTANT_PROPAGATION) &&
              !pctx.getContext().isCboSucceeded()) {
        // PartitionPruner may create more folding opportunities, run ConstantPropagate again.
        transformations.add(new ConstantPropagate());
      }
    }

    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_GROUPBY) ||
        HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_MAP_GROUPBY_SORT)) {
      transformations.add(new GroupByOptimizer());
    }
    transformations.add(new ColumnPruner());
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_COUNT_DISTINCT_OPTIMIZER)
        && (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_IN_TEST) || isTezExecEngine)) {
      transformations.add(new CountDistinctRewriteProc());
    }
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPTIMIZE_SKEWJOIN_COMPILETIME)) {
      if (!isTezExecEngine) {
        transformations.add(new SkewJoinOptimizer());
      } else {
        LOG.warn("Skew join is currently not supported in tez! Disabling the skew join optimization.");
      }
    }
    transformations.add(new SamplePruner());

    MapJoinProcessor mapJoinProcessor = new MapJoinProcessor();
    transformations.add(mapJoinProcessor);

    if ((HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_BUCKET_MAPJOIN))
      && !isTezExecEngine) {
      transformations.add(new BucketMapJoinOptimizer());
      bucketMapJoinOptimizer = true;
    }

    // If optimize hive.optimize.bucketmapjoin.sortedmerge is set, add both
    // BucketMapJoinOptimizer and SortedMergeBucketMapJoinOptimizer
    if ((HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_SORT_MERGE_BUCKET_MAPJOIN))
        && !isTezExecEngine) {
      if (!bucketMapJoinOptimizer) {
        // No need to add BucketMapJoinOptimizer twice
        transformations.add(new BucketMapJoinOptimizer());
      }
      transformations.add(new SortedMergeBucketMapJoinOptimizer());
    }

    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPTIMIZE_BUCKETING_SORTING)) {
      transformations.add(new BucketingSortingReduceSinkOptimizer());
    }

    transformations.add(new UnionProcessor());

    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.N_WAY_JOIN_REORDER)) {
      transformations.add(new JoinReorder());
    }

    if (HiveConf.getBoolVar(hiveConf,
        HiveConf.ConfVars.TEZ_OPTIMIZE_BUCKET_PRUNING)
        && HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_PPD)
        && HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_INDEX_FILTER)) {
      final boolean compatMode =
          HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.TEZ_OPTIMIZE_BUCKET_PRUNING_COMPAT);
      transformations.add(new FixedBucketPruningOptimizer(compatMode));
    }

    transformations.add(new BucketVersionPopulator());

    if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_REDUCE_DEDUPLICATION) &&
        !isTezExecEngine) {
      transformations.add(new ReduceSinkDeDuplication());
    }
    transformations.add(new NonBlockingOpDeDupProc());
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_IDENTITY_PROJECT_REMOVER)
        && !HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP)) {
      transformations.add(new IdentityProjectRemover());
    }
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_LIMIT_OPT_ENABLE)) {
      transformations.add(new GlobalLimitOptimizer());
    }
    if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_CORRELATION) &&
        !HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_GROUPBY_SKEW) &&
        !HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPTIMIZE_SKEWJOIN_COMPILETIME) &&
        !isTezExecEngine) {
      transformations.add(new CorrelationOptimizer());
    }
    if (HiveConf.getFloatVar(hiveConf, HiveConf.ConfVars.HIVE_LIMIT_PUSHDOWN_MEMORY_USAGE) > 0) {
      transformations.add(new LimitPushdownOptimizer());
    }
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPTIMIZE_LIMIT)) {
      transformations.add(new OrderlessLimitPushDownOptimizer());
    }
    if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPTIMIZE_METADATA_QUERIES)) {
      transformations.add(new StatsOptimizer());
    }
    if (pctx.getContext().isExplainSkipExecution() && !isTezExecEngine) {
      transformations.add(new AnnotateWithStatistics());
      transformations.add(new AnnotateWithOpTraits());
    }

    if (!HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION).equals("none")) {
      transformations.add(new SimpleFetchOptimizer()); // must be called last
    }

    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_FETCH_TASK_AGGR)) {
      transformations.add(new SimpleFetchAggregation());
    }

    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPTIMIZE_TABLE_PROPERTIES_FROM_SERDE)) {
      transformations.add(new TablePropertyEnrichmentOptimizer());
    }

  }

  /**
   * Invoke all the transformations one-by-one, and alter the query plan.
   *
   * @return ParseContext
   * @throws SemanticException
   */
  public ParseContext optimize() throws SemanticException {
    for (Transform t : transformations) {
      t.beginPerfLogging();
      pctx = t.transform(pctx);
      t.endPerfLogging(t.toString());
    }
    return pctx;
  }

  /**
   * @return the pctx
   */
  public ParseContext getPctx() {
    return pctx;
  }

  /**
   * @param pctx
   *          the pctx to set
   */
  public void setPctx(ParseContext pctx) {
    this.pctx = pctx;
  }

}
