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

package org.apache.hadoop.hive.impala.funcmapper;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import com.google.common.collect.Iterables;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExtractDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.RexNodeConverter;
import org.apache.hadoop.hive.ql.parse.type.FunctionHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Extract function resolver.  This is used for date specific Calcite
 * signatures that need mapping into Impala. The Calcite signatures have
 * a signature of "EXTRACT(FLAG(YEAR):SYMBOL, DATE)" while the Impala
 * signatures are like "YEAR(DATE)". The "SYMBOL" Calcite parameter needs
 * to be stripped when comparing or mapping to Impala signatures.
 * The function coming into Calcite also has the form of "YEAR(DATE)", so
 * the appopriate translation into Calcite is needed.
 */
public class ExtractFunctionResolver extends ImpalaFunctionResolverImpl {

  ExtractFunctionResolver(FunctionHelper helper, SqlOperator op, List<RexNode> inputNodes
      ) throws HiveException {
    super(helper, op, inputNodes);
    if (argTypes.size() != 1) {
      throw new HiveException("Function " + func + " should have exactly 1 argument");
    }
  }

  @Override
  public List<RexNode> getConvertedInputs(ImpalaFunctionSignature candidate) {
    return rewriteExtractDateChildren(op, candidate, inputNodes, rexBuilder);
  }

  @Override
  public List<RelDataType> getCastOperandTypes(
      ImpalaFunctionSignature castCandidate) {
    Preconditions.checkState(castCandidate.getArgTypes().size() == 1,
        "Num of arguments for " + this.func + " expected to be 1.");
    // first argument in candidate is "SYMBOL" and does not have to be cast, so we
    // grab it from this Calcite signature. The second argument needs to be
    // grabbed from the first argument of the cast candidate.
    return Lists.newArrayList(argTypes.get(0), castCandidate.getArgTypes().get(0));
  }

  /**
   * rewrites the extract input. This is almost a copy of the code in RexNodeConverter,
   * but the difference is that Impala expects the date string to be converted into a
   * timestamp always whereas the code in RexNodeConverter sometimes turns it into a date.
   */
  private static List<RexNode> rewriteExtractDateChildren(SqlOperator op,
      ImpalaFunctionSignature candidate, List<RexNode> childRexNodeLst, RexBuilder rexBuilder) {
    List<RexNode> newChildRexNodeLst = new ArrayList<>(2);
    if (op == HiveExtractDate.YEAR) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.YEAR));
    } else if (op == HiveExtractDate.QUARTER) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.QUARTER));
    } else if (op == HiveExtractDate.MONTH) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.MONTH));
    } else if (op == HiveExtractDate.WEEK) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.WEEK));
    } else if (op == HiveExtractDate.DAY) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.DAY));
    } else if (op == HiveExtractDate.HOUR) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.HOUR));
    } else if (op == HiveExtractDate.MINUTE) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.MINUTE));
    } else if (op == HiveExtractDate.SECOND) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.SECOND));
    }

    final RexNode child = Iterables.getOnlyElement(childRexNodeLst);

    RelDataType candidateType = candidate.getArgTypes().get(0);

    if (SqlTypeUtil.isInterval(child.getType()) ||
        candidateType.getSqlTypeName().equals(child.getType().getSqlTypeName())) {
      newChildRexNodeLst.add(child);
    } else {
      RelDataType nullableType =
          rexBuilder.getTypeFactory().createTypeWithNullability(candidateType, true);
      RexNode castType = rexBuilder.makeCast(nullableType, child);
      newChildRexNodeLst.add(castType);
    }
    return newChildRexNodeLst;
  }
}
