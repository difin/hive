/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for OzonePathUtils.
 *
 * These tests verify path parsing and translation logic without requiring
 * an actual Ozone cluster.
 */
public class OzonePathUtilsTest {

    @Test
    public void testParseOfsPath() {
        String ofsUrl = "ofs://om/analytics/warehouse/sales/data/file.parquet";
        OzonePathUtils.OzonePath path = OzonePathUtils.parseOfsPath(ofsUrl);

        assertEquals("om", path.omHost);
        assertEquals("analytics", path.volume);
        assertEquals("warehouse", path.bucket);
        assertEquals("sales/data/file.parquet", path.key);
    }

    @Test
    public void testParseOfsPathWithoutKey() {
        String ofsUrl = "ofs://om/analytics/warehouse";
        OzonePathUtils.OzonePath path = OzonePathUtils.parseOfsPath(ofsUrl);

        assertEquals("om", path.omHost);
        assertEquals("analytics", path.volume);
        assertEquals("warehouse", path.bucket);
        assertEquals("", path.key);
    }

    @Test
    public void testGenerateS3BucketName() {
        String bucketName = OzonePathUtils.generateS3BucketName("analytics", "warehouse");
        assertEquals("analytics-warehouse", bucketName);
    }

    @Test
    public void testGenerateS3BucketNameWithSpecialChars() {
        // Test with special characters that need to be sanitized
        String bucketName = OzonePathUtils.generateS3BucketName("Data_Lake", "Raw_Files");
        assertEquals("data-lake-raw-files", bucketName);

        // S3 bucket names must be lowercase and alphanumeric + dash
        assertTrue("Bucket name should be lowercase", bucketName.equals(bucketName.toLowerCase()));
        assertTrue("Bucket name should match S3 rules", bucketName.matches("[a-z0-9-]+"));
    }

    @Test
    public void testTranslateOfsToS3a() {
        String ofsUrl = "ofs://om/analytics/warehouse/sales/data";
        String s3aUrl = OzonePathUtils.translateOfsToS3a(ofsUrl);
        assertEquals("s3a://analytics-warehouse/sales/data", s3aUrl);
    }

    @Test
    public void testTranslateOfsToS3aWithFile() {
        String ofsUrl = "ofs://om/analytics/warehouse/sales/data/file.parquet";
        String s3aUrl = OzonePathUtils.translateOfsToS3a(ofsUrl);
        assertEquals("s3a://analytics-warehouse/sales/data/file.parquet", s3aUrl);
    }

    @Test
    public void testTranslateOfsToS3() {
        String ofsUrl = "ofs://om/analytics/warehouse/sales/data";
        String s3Url = OzonePathUtils.translateOfsToS3(ofsUrl);
        assertEquals("s3://analytics-warehouse/sales/data", s3Url);
    }

    @Test
    public void testIsOfsScheme() {
        assertTrue(OzonePathUtils.isOfsScheme("ofs://om/vol/bkt/table"));
        assertFalse(OzonePathUtils.isOfsScheme("s3://bucket/table"));
        assertFalse(OzonePathUtils.isOfsScheme("s3a://bucket/table"));
        assertFalse(OzonePathUtils.isOfsScheme("hdfs://namenode/table"));
        assertFalse(OzonePathUtils.isOfsScheme("file:///tmp/table"));
        assertFalse(OzonePathUtils.isOfsScheme(null));
        assertFalse(OzonePathUtils.isOfsScheme(""));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseInvalidOfsPath() {
        OzonePathUtils.parseOfsPath("s3://bucket/key");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseNullPath() {
        OzonePathUtils.parseOfsPath(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseMalformedOfsPath() {
        // Missing bucket component
        OzonePathUtils.parseOfsPath("ofs://om/volume");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGenerateS3BucketNameNullVolume() {
        OzonePathUtils.generateS3BucketName(null, "bucket");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGenerateS3BucketNameNullBucket() {
        OzonePathUtils.generateS3BucketName("volume", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGenerateS3BucketNameEmptyVolume() {
        OzonePathUtils.generateS3BucketName("", "bucket");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGenerateS3BucketNameEmptyBucket() {
        OzonePathUtils.generateS3BucketName("volume", "");
    }
}
