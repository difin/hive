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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.columnstats.merge;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.ByteBuffer;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class DecimalColumnStatsMergerTest {
  private static final Decimal DECIMAL_1 = getDecimal("1", 1, 0);
  private static final Decimal DECIMAL_3 = getDecimal("3", 3, 0);
  private static final Decimal DECIMAL_5 = getDecimal("5", 5, 0);
  private static final Decimal DECIMAL_20 = getDecimal("20", 2, -1);

  private static final DecimalColumnStatsDataInspector DATA_3 = new DecimalColumnStatsDataInspector();
  private static final DecimalColumnStatsDataInspector DATA_5 = new DecimalColumnStatsDataInspector();

  static {
    DATA_3.setLowValue(DECIMAL_3);
    DATA_3.setHighValue(DECIMAL_3);
    DATA_5.setLowValue(DECIMAL_5);
    DATA_5.setHighValue(DECIMAL_5);
  }

  private final DecimalColumnStatsMerger merger = new DecimalColumnStatsMerger();

  /**
   * Creates a decimal and checks its string representation.
   */
  private static Decimal getDecimal(String expected, int number, int scale) {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.asIntBuffer().put(number);
    Decimal d = new Decimal((short) scale, bb);
    assertEquals(expected, MetaStoreUtils.decimalToString(d));
    return d;
  }

  @Test
  public void testMergeNullMinMaxValues() {
    ColumnStatisticsObj objNulls = new ColumnStatisticsObj();
    createData(objNulls, null, null);

    merger.merge(objNulls, objNulls);

    Assert.assertNull(objNulls.getStatsData().getDecimalStats().getLowValue());
    Assert.assertNull(objNulls.getStatsData().getDecimalStats().getHighValue());
  }

  @Test
  public void testMergeNonNullAndNullLowerValuesOldIsNull() {
    ColumnStatisticsObj oldObj = new ColumnStatisticsObj();
    createData(oldObj, null, null);

    ColumnStatisticsObj newObj = new ColumnStatisticsObj();
    createData(newObj, DECIMAL_3, null);

    merger.merge(oldObj, newObj);

    Assert.assertEquals(DECIMAL_3, oldObj.getStatsData().getDecimalStats().getLowValue());
  }

  @Test
  public void testMergeNonNullAndNullLowerValuesNewIsNull() {
    ColumnStatisticsObj oldObj = new ColumnStatisticsObj();
    createData(oldObj, DECIMAL_3, null);

    ColumnStatisticsObj newObj = new ColumnStatisticsObj();
    createData(newObj, null, null);

    merger.merge(oldObj, newObj);

    Assert.assertEquals(DECIMAL_3, oldObj.getStatsData().getDecimalStats().getLowValue());
  }

  @Test
  public void testMergeNonNullAndNullHigherValuesOldIsNull() {
    ColumnStatisticsObj oldObj = new ColumnStatisticsObj();
    createData(oldObj, null, null);

    ColumnStatisticsObj newObj = new ColumnStatisticsObj();
    createData(newObj, null, DECIMAL_3);

    merger.merge(oldObj, newObj);

    Assert.assertEquals(DECIMAL_3, oldObj.getStatsData().getDecimalStats().getHighValue());
  }

  @Test
  public void testMergeNonNullAndNullHigherValuesNewIsNull() {
    ColumnStatisticsObj oldObj = new ColumnStatisticsObj();
    createData(oldObj, null, DECIMAL_3);

    ColumnStatisticsObj newObj = new ColumnStatisticsObj();
    createData(newObj, null, null);

    merger.merge(oldObj, newObj);

    Assert.assertEquals(DECIMAL_3, oldObj.getStatsData().getDecimalStats().getHighValue());
  }

  @Test
  public void testMergeLowValuesFirstWins() {
    ColumnStatisticsObj oldObj = new ColumnStatisticsObj();
    createData(oldObj, DECIMAL_3, null);

    ColumnStatisticsObj newObj = new ColumnStatisticsObj();
    createData(newObj, DECIMAL_5, null);

    merger.merge(oldObj, newObj);

    Assert.assertEquals(DECIMAL_3, oldObj.getStatsData().getDecimalStats().getLowValue());
  }

  @Test
  public void testMergeLowValuesSecondWins() {
    ColumnStatisticsObj oldObj = new ColumnStatisticsObj();
    createData(oldObj, DECIMAL_5, null);

    ColumnStatisticsObj newObj = new ColumnStatisticsObj();
    createData(newObj, DECIMAL_3, null);

    merger.merge(oldObj, newObj);

    Assert.assertEquals(DECIMAL_3, oldObj.getStatsData().getDecimalStats().getLowValue());
  }

  @Test
  public void testMergeHighValuesFirstWins() {
    ColumnStatisticsObj oldObj = new ColumnStatisticsObj();
    createData(oldObj, null, DECIMAL_5);

    ColumnStatisticsObj newObj = new ColumnStatisticsObj();
    createData(newObj, null, DECIMAL_3);

    merger.merge(oldObj, newObj);

    Assert.assertEquals(DECIMAL_5, oldObj.getStatsData().getDecimalStats().getHighValue());
  }

  @Test
  public void testMergeHighValuesSecondWins() {
    ColumnStatisticsObj oldObj = new ColumnStatisticsObj();
    createData(oldObj, null, DECIMAL_3);

    ColumnStatisticsObj newObj = new ColumnStatisticsObj();
    createData(newObj, null, DECIMAL_5);

    merger.merge(oldObj, newObj);

    Assert.assertEquals(DECIMAL_5, oldObj.getStatsData().getDecimalStats().getHighValue());
  }

  @Test
  public void testDecimalCompareEqual() {
    assertTrue(DECIMAL_3.equals(getDecimal("3", 3, 0)));
    // the equals method does not check for numerical equality,
    // e.g., DECIMAL_3 is not equal to getDecimal("3", 30, 1)
  }

  @Test
  public void testDecimalCompareDoesntEqual() {
    assertFalse(DECIMAL_3.equals(DECIMAL_5));
    assertFalse(DECIMAL_3.equals(getDecimal("30", 3, -1)));
  }

  private void checkMergedValue(Decimal low, Decimal high) {
    Objects.requireNonNull(low);
    Objects.requireNonNull(high);
    assertTrue(MetaStoreUtils.decimalToDouble(low) < MetaStoreUtils.decimalToDouble(high));

    {
      ColumnStatisticsObj aggData = new ColumnStatisticsObj();
      createData(aggData, low, low);
      ColumnStatisticsObj otherData = new ColumnStatisticsObj();
      createData(otherData, high, high);
      merger.merge(aggData, otherData);

      assertEquals(low, aggData.getStatsData().getDecimalStats().getLowValue());
      assertEquals(high, aggData.getStatsData().getDecimalStats().getHighValue());
    }

    {
      ColumnStatisticsObj aggData = new ColumnStatisticsObj();
      createData(aggData, high, high);
      ColumnStatisticsObj otherData = new ColumnStatisticsObj();
      createData(otherData, low, low);
      merger.merge(aggData, otherData);

      assertEquals(low, aggData.getStatsData().getDecimalStats().getLowValue());
      assertEquals(high, aggData.getStatsData().getDecimalStats().getHighValue());
    }
  }

  @Test
  public void testCompareSimple() {
    checkMergedValue(DECIMAL_3, DECIMAL_5);
  }

  @Test
  public void testCompareUnscaledValue() {
    checkMergedValue(DECIMAL_3, DECIMAL_20);
  }

  @Test
  public void testCompareScaledValue() {
    checkMergedValue(getDecimal("-123.2", -1232, 1), getDecimal("-10.2", -102, 1));

    checkMergedValue(getDecimal("1.02", 102, 2), getDecimal("123.2", 1232, 1));

    checkMergedValue(getDecimal("1.02", 102, 2), getDecimal("1232000", 1232, -3));
  }

  @Test
  public void testCompareNullsMin() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector();
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector();
    merger.setLowValue(data1, data2);
    Assert.assertNull(data1.getLowValue());
  }

  @Test
  public void testCompareNullsMax() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector();
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector();
    merger.setHighValue(data1, data2);
    Assert.assertNull(data1.getHighValue());
  }

  @Test
  public void testCompareFirstNullMin() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector();
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector(DATA_3);
    merger.setLowValue(data1, data2);
    Assert.assertEquals(DECIMAL_3, data1.getLowValue());
  }

  @Test
  public void testCompareSecondNullMin() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector(DATA_3);
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector();
    merger.setLowValue(data1, data2);
    Assert.assertEquals(DECIMAL_3, data1.getLowValue());
  }

  @Test
  public void testCompareFirstNullMax() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector(DATA_3);
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector();
    merger.setHighValue(data1, data2);
    Assert.assertEquals(DECIMAL_3, data1.getHighValue());
  }

  @Test
  public void testCompareSecondNullMax() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector();
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector(DATA_3);
    merger.setHighValue(data1, data2);
    Assert.assertEquals(DECIMAL_3, data1.getHighValue());
  }

  private DecimalColumnStatsDataInspector createData(ColumnStatisticsObj objNulls, Decimal lowValue,
      Decimal highValue) {
    ColumnStatisticsData statisticsData = new ColumnStatisticsData();
    DecimalColumnStatsDataInspector data = new DecimalColumnStatsDataInspector();

    statisticsData.setDecimalStats(data);
    objNulls.setStatsData(statisticsData);

    data.setLowValue(lowValue);
    data.setHighValue(highValue);
    return data;
  }
}
