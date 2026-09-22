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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for Ozone path parsing and protocol translation.
 * <p>
 * Handles translation between different URI schemes:
 * <ul>
 *   <li>ofs:// (Ozone File System) - native Ozone protocol</li>
 *   <li>s3a:// (S3A) - Hadoop S3 compatible protocol</li>
 *   <li>s3:// (S3) - Native S3 protocol used by external clients</li>
 * </ul>
 */
public class OzonePathUtils {

    /**
     * Pattern to match ofs:// URLs
     * Format: ofs://om-host/volume/bucket/key
     * Groups: 1=om-host, 2=volume, 3=bucket, 4=/key (optional)
     */
    private static final Pattern OFS_PATTERN =
        Pattern.compile("ofs://([^/]+)/([^/]+)/([^/]+)(/.*)?");

    /**
     * Pattern to match s3a:// URLs
     * Format: s3a://bucket/key
     * Groups: 1=bucket, 2=/key (optional)
     */
    private static final Pattern S3A_PATTERN =
        Pattern.compile("s3a://([^/]+)(/.*)?");

    /**
     * Data class representing an Ozone path parsed from ofs:// URL
     */
    public static class OzonePath {
        public final String omHost;
        public final String volume;
        public final String bucket;
        public final String key;

        public OzonePath(String omHost, String volume, String bucket, String key) {
            this.omHost = omHost;
            this.volume = volume;
            this.bucket = bucket;
            this.key = key != null ? key : "";
        }

        @Override
        public String toString() {
            return String.format("OzonePath{omHost='%s', volume='%s', bucket='%s', key='%s'}",
                omHost, volume, bucket, key);
        }

        /**
         * Convert to s3a:// URL using generated bucket name
         */
        public String toS3aUrl() {
            String s3Bucket = generateS3BucketName(volume, bucket);
            if (key.isEmpty()) {
                return "s3a://" + s3Bucket;
            } else {
                return "s3a://" + s3Bucket + "/" + key;
            }
        }

        /**
         * Convert to s3:// URL using generated bucket name
         */
        public String toS3Url() {
            return toS3aUrl().replace("s3a://", "s3://");
        }
    }

    /**
     * Parse ofs:// URL into components
     * <p>
     * Example: ofs://om-host/volume/bucket/path/to/file
     * Returns: OzonePath{omHost="om-host", volume="volume", bucket="bucket", key="path/to/file"}
     *
     * @param ofsUrl the ofs:// URL to parse
     * @return parsed OzonePath object
     * @throws IllegalArgumentException if URL is not a valid ofs:// URL
     */
    public static OzonePath parseOfsPath(String ofsUrl) {
        if (ofsUrl == null || !ofsUrl.startsWith("ofs://")) {
            throw new IllegalArgumentException("Not an ofs:// URL: " + ofsUrl);
        }

        Matcher matcher = OFS_PATTERN.matcher(ofsUrl);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                "Invalid ofs:// URL format. Expected: ofs://om-host/volume/bucket[/key]. Got: " + ofsUrl);
        }

        String omHost = matcher.group(1);
        String volume = matcher.group(2);
        String bucket = matcher.group(3);
        String keyPath = matcher.group(4);

        // Remove leading slash from key path
        String key = "";
        if (keyPath != null && keyPath.startsWith("/")) {
            key = keyPath.substring(1);
        }

        return new OzonePath(omHost, volume, bucket, key);
    }

    /**
     * Generate S3-compatible bucket name from Ozone volume/bucket
     * <p>
     * Naming convention: {volume}-{bucket}
     * <p>
     * Ensures S3-compatible naming:
     * <ul>
     *   <li>Converts to lowercase</li>
     *   <li>Replaces non-alphanumeric characters with dashes</li>
     *   <li>Collapses multiple consecutive dashes</li>
     *   <li>Removes leading/trailing dashes</li>
     * </ul>
     *
     * @param volume Ozone volume name
     * @param bucket Ozone bucket name
     * @return S3-compatible bucket name
     */
    public static String generateS3BucketName(String volume, String bucket) {
        if (volume == null || volume.isEmpty()) {
            throw new IllegalArgumentException("Volume cannot be null or empty");
        }
        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalArgumentException("Bucket cannot be null or empty");
        }

        // Combine volume and bucket with dash separator
        String combined = volume + "-" + bucket;

        // Ensure S3-compatible naming (lowercase, alphanumeric + dash)
        return combined.toLowerCase()
            .replaceAll("[^a-z0-9-]", "-")  // Replace non-alphanumeric with dash
            .replaceAll("-+", "-")           // Collapse multiple dashes
            .replaceAll("^-|-$", "");        // Remove leading/trailing dashes
    }

    /**
     * Translate ofs:// path to s3a:// path
     * <p>
     * Example: ofs://om/analytics/warehouse/table/data/file.parquet
     * Returns: s3a://analytics-warehouse/table/data/file.parquet
     *
     * @param ofsUrl the ofs:// URL to translate
     * @return translated s3a:// URL
     */
    public static String translateOfsToS3a(String ofsUrl) {
        OzonePath ozPath = parseOfsPath(ofsUrl);
        return ozPath.toS3aUrl();
    }

    /**
     * Translate ofs:// path to s3:// path
     * <p>
     * Example: ofs://om/analytics/warehouse/table/data/file.parquet
     * Returns: s3://analytics-warehouse/table/data/file.parquet
     *
     * @param ofsUrl the ofs:// URL to translate
     * @return translated s3:// URL
     */
    public static String translateOfsToS3(String ofsUrl) {
        OzonePath ozPath = parseOfsPath(ofsUrl);
        return ozPath.toS3Url();
    }

    /**
     * Check if URL uses ofs:// scheme
     *
     * @param url the URL to check
     * @return true if URL starts with ofs://
     */
    public static boolean isOfsScheme(String url) {
        return url != null && url.startsWith("ofs://");
    }

    /**
     * Detect the scheme of a URL
     *
     * @param url the URL to check
     * @return "ofs", "s3a", "s3", or "unknown"
     */
    public static String detectScheme(String url) {
        if (url == null) {
            return "unknown";
        }
        if (url.startsWith("ofs://")) {
            return "ofs";
        }
        if (url.startsWith("s3a://")) {
            return "s3a";
        }
        if (url.startsWith("s3://")) {
            return "s3";
        }
        return "unknown";
    }

    // Private constructor to prevent instantiation
    private OzonePathUtils() {
        throw new UnsupportedOperationException("Utility class");
    }
}
