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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages Ozone link buckets for S3 Gateway access.
 * <p>
 * Link buckets are metadata-only symbolic links that expose Ozone buckets
 * through the S3 Gateway interface. This manager handles:
 * <ul>
 *   <li>Detection of existing link buckets</li>
 *   <li>Automatic creation of link buckets when needed</li>
 *   <li>Caching of link bucket existence to reduce overhead</li>
 * </ul>
 */
public class OzoneLinkBucketManager {
    private static final Logger LOG = LoggerFactory.getLogger(OzoneLinkBucketManager.class);

    public static final String CONF_S3_VOLUME = "hive.metastore.catalog.ozone.s3.volume";
    public static final String CONF_AUTO_CREATE_LINKS = "hive.metastore.catalog.ozone.link.auto.create";
    /**
     * When FALSE, disables the synchronous in-line translation of an Ozone
     * table's {ofs://} metadata to {s3a://} on the {loadTable} path. Default is TRUE
     */
    public static final String CONF_INLINE_REWRITE_ENABLED =
        "hive.metastore.catalog.ozone.inline.rewrite.enabled";
    public static final String DEFAULT_S3_VOLUME = "s3v";
    private static volatile OzoneLinkBucketManager instance;
    private final OzoneClientManager clientManager;
    private final Configuration conf;
    private final String s3VolumeName;
    private final boolean autoCreateLinks;
    private final boolean ozoneInlineRewriteEnabled;

    // Cache to track which link buckets exist (avoid repeated Ozone RPC calls)
    private final ConcurrentHashMap<String, Boolean> linkBucketCache = new ConcurrentHashMap<>();

    /**
     * Returns the singleton, lazily initializing it on first call.
     *
     * <p>Returns {@code null} if Ozone is not enabled in the configuration, allowing callers
     * to skip Ozone-specific logic with a simple null-check.
     *
     * @param conf Hadoop configuration, used only on the first call
     * @return the singleton instance, or {@code null} if Ozone is not configured
     */
    public static OzoneLinkBucketManager getInstance(Configuration conf) {
        if (instance == null) {
            synchronized (OzoneLinkBucketManager.class) {
                if (instance == null) {
                    try {
                        OzoneClientManager clientManager = new OzoneClientManager(conf);
                        if (clientManager.isEnabled()) {
                            instance = new OzoneLinkBucketManager(clientManager, conf);
                            LOG.info("OzoneLinkBucketManager lazily initialized");
                        } else {
                            LOG.debug("Ozone is not enabled in configuration, skipping init");
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to initialize OzoneLinkBucketManager, Ozone protocol " +
                            "translation will be unavailable: {}", e.getMessage());
                    }
                }
            }
        }
        return instance;
    }

    private OzoneLinkBucketManager(OzoneClientManager clientManager, Configuration conf) {
        this.clientManager = clientManager;
        this.conf = conf;
        this.s3VolumeName = conf.get(CONF_S3_VOLUME, DEFAULT_S3_VOLUME);
        this.autoCreateLinks = conf.getBoolean(CONF_AUTO_CREATE_LINKS, true);
        this.ozoneInlineRewriteEnabled = conf.getBoolean(CONF_INLINE_REWRITE_ENABLED, true);

        LOG.info("OzoneLinkBucketManager initialized. S3 volume: {}, Auto-create links: {}, "
                 + "In-line path rewrite: {}", s3VolumeName, autoCreateLinks, ozoneInlineRewriteEnabled);
    }

    /**
     * Ensure S3 link bucket exists for the given ofs:// path.
     * <p>
     * This method:
     * 1. Parses the ofs:// path to extract volume/bucket
     * 2. Generates S3-compatible bucket name
     * 3. Checks if link bucket already exists (with caching)
     * 4. Creates link bucket if needed (when auto-create is enabled)
     *
     * @param ofsPath the ofs:// URL (e.g., ofs://om/vol/bkt/key)
     * @return the S3 bucket name that links to the Ozone bucket
     * @throws IOException if link bucket operations fail
     */
    public String ensureS3Link(String ofsPath) throws IOException {
        if (!clientManager.isEnabled()) {
            throw new IllegalStateException(
                "Cannot create link bucket: Ozone client is not enabled");
        }

        OzonePathUtils.OzonePath ozPath = OzonePathUtils.parseOfsPath(ofsPath);
        String linkBucketName = OzonePathUtils.generateS3BucketName(
            ozPath.volume, ozPath.bucket);

        // Check cache first (fast path)
        if (linkBucketCache.getOrDefault(linkBucketName, false)) {
            LOG.debug("Link bucket exists (cached): {}", linkBucketName);
            return linkBucketName;
        }

        ObjectStore objectStore = clientManager.getObjectStore();

        // Ensure s3v volume exists
        ensureS3Volume(objectStore);

        OzoneVolume s3Volume = objectStore.getVolume(s3VolumeName);

        try {
            // Try to get the bucket (will throw exception if doesn't exist)
            OzoneBucket linkedBucket = s3Volume.getBucket(linkBucketName);

            // Verify it's a link bucket pointing to correct source
            String sourceVol = linkedBucket.getSourceVolume();
            String sourceBkt = linkedBucket.getSourceBucket();

            if (sourceVol != null && sourceBkt != null) {
                // This is a link bucket - verify it points to the right place
                if (!ozPath.volume.equals(sourceVol) || !ozPath.bucket.equals(sourceBkt)) {
                    LOG.warn("Link bucket '{}' exists but points to wrong source. " +
                             "Expected: /{}/{}, Found: /{}/{}. Using existing link.",
                             linkBucketName, ozPath.volume, ozPath.bucket, sourceVol, sourceBkt);
                }

                LOG.info("Link bucket already exists: {} -> /{}/{}",
                         linkBucketName, sourceVol, sourceBkt);
                linkBucketCache.put(linkBucketName, true);
                return linkBucketName;

            } else {
                // Bucket exists but is not a link bucket
                throw new IOException(
                    "Bucket '" + linkBucketName + "' exists in /" + s3VolumeName +
                    " but is not a link bucket (sourceVolume and sourceBucket are null). " +
                    "Please use a different bucket name or delete the existing bucket.");
            }

        } catch (IOException e) {
            // Bucket doesn't exist - create it if auto-create is enabled
            if (!autoCreateLinks) {
                throw new IllegalStateException(
                    "Link bucket '" + linkBucketName + "' does not exist and " +
                    "auto-creation is disabled. Please create manually:\n" +
                    "  ozone sh bucket link /" + ozPath.volume + "/" + ozPath.bucket +
                    " /" + s3VolumeName + "/" + linkBucketName);
            }

            // Verify source bucket exists before creating link
            verifySourceBucketExists(objectStore, ozPath.volume, ozPath.bucket);

            // Create the link bucket
            LOG.info("Creating link bucket: {} -> /{}/{}",
                     linkBucketName, ozPath.volume, ozPath.bucket);

            createLinkBucket(s3Volume, linkBucketName, ozPath.volume, ozPath.bucket);

            linkBucketCache.put(linkBucketName, true);
            LOG.info("Successfully created link bucket: {}", linkBucketName);

            return linkBucketName;
        }
    }

    /**
     * Create a link bucket using BucketArgs with source volume/bucket
     *
     * @param s3Volume the s3v volume
     * @param linkBucketName the name for the link bucket
     * @param sourceVolume the source Ozone volume
     * @param sourceBucket the source Ozone bucket
     * @throws IOException if bucket creation fails
     */
    private void createLinkBucket(OzoneVolume s3Volume, String linkBucketName,
                                   String sourceVolume, String sourceBucket) throws IOException {
        try {
            BucketArgs.Builder builder = BucketArgs.newBuilder();
            builder.setSourceVolume(sourceVolume);
            builder.setSourceBucket(sourceBucket);
            BucketArgs bucketArgs = builder.build();

            // Create the bucket
            s3Volume.createBucket(linkBucketName, bucketArgs);

            LOG.debug("Link bucket created successfully: {} in /{}", linkBucketName, s3VolumeName);

        } catch (IOException e) {
            LOG.error("Failed to create link bucket '{}' in /{} for source /{}/{}",
                      linkBucketName, s3VolumeName, sourceVolume, sourceBucket, e);
            throw new IOException(
                "Failed to create link bucket '" + linkBucketName + "' in /" + s3VolumeName +
                " linking to /" + sourceVolume + "/" + sourceBucket + ": " + e.getMessage(), e);
        }
    }

    /**
     * Verify that the source Ozone bucket exists before creating a link
     *
     * @param objectStore the ObjectStore
     * @param volume the source volume name
     * @param bucket the source bucket name
     * @throws IOException if source bucket doesn't exist or cannot be accessed
     */
    private void verifySourceBucketExists(ObjectStore objectStore, String volume, String bucket)
            throws IOException {
        try {
            OzoneVolume sourceVolume = objectStore.getVolume(volume);
            OzoneBucket sourceBucket = sourceVolume.getBucket(bucket);
            LOG.debug("Verified source bucket exists: /{}/{}", volume, bucket);
        } catch (IOException e) {
            throw new IOException(
                "Cannot create link bucket: source bucket /" + volume + "/" + bucket +
                " does not exist or cannot be accessed", e);
        }
    }

    /**
     * Ensure the S3 volume (default: s3v) exists, create if needed
     *
     * @param objectStore the ObjectStore
     * @throws IOException if volume operations fail
     */
    private void ensureS3Volume(ObjectStore objectStore) throws IOException {
        try {
            objectStore.getVolume(s3VolumeName);
            LOG.debug("S3 volume exists: /{}", s3VolumeName);
        } catch (IOException e) {
            LOG.warn("S3 volume '{}' does not exist. Attempting to create...", s3VolumeName);
            try {
                objectStore.createVolume(s3VolumeName);
                LOG.info("Created S3 volume: /{}", s3VolumeName);
            } catch (IOException createEx) {
                throw new IOException(
                    "S3 volume '" + s3VolumeName + "' does not exist and could not be created. " +
                    "Please create manually: ozone sh volume create /" + s3VolumeName, createEx);
            }
        }
    }

    /**
     * Translates an Ozone {@code ofs://} table for external S3 clients.
     *
     * <p>This is the single entry point for all Ozone-specific translation that must happen
     * before returning a {@code LoadTableResponse}. It performs two steps:
     * <ol>
     *   <li>Ensures a S3 link bucket exists in the S3 volume ({@code s3v}) pointing to the
     *       native Ozone volume/bucket, so external clients can access data via the S3 Gateway.</li>
     *   <li>Rewrites the Iceberg table metadata, replacing all {@code ofs://} paths with
     *       {@code s3a://} paths, so external clients get a metadata snapshot they can read
     *       without the Ozone filesystem library.</li>
     * </ol>
     *
     * <p>If the table location is not an {@code ofs://} path, or in-line path rewriting is
     * disabled via {#CONF_INLINE_REWRITE_ENABLED}, returns the original metadata
     * unchanged.
     *
     * @param table the Iceberg BaseTable to translate
     * @return translated {TableMetadata} with { s3a://} paths, or the original
     *         metadata if translation was disabled, not needed or failed
     */
    public TableMetadata translateMetadataForExternalClient(BaseTable table) {
        String location = table.location();
        if (!OzonePathUtils.isOfsScheme(location)) {
            return table.operations().current();
        }

        if (!ozoneInlineRewriteEnabled) {
            LOG.info("In-line path rewrite is disabled ({}=false) for ozone; returning ofs:// "
                     + "metadata for table: {}", CONF_INLINE_REWRITE_ENABLED, location);
            return table.operations().current();
        }

        TableMetadata original = table.operations().current();
        try {
            // Step 1: Ensure S3 link bucket exists
            String s3BucketName = ensureS3Link(location);
            LOG.info("Ensured S3 link bucket for ofs:// table: {} -> {}", location, s3BucketName);

            // Step 2: Project metadata paths from ofs:// to s3a://
            OzoneMetadataRewriter rewriter = new OzoneMetadataRewriter(conf);
            TableMetadata translated = rewriter.getOrCreateProjectedMetadata(table);
            if (translated != null) {
                LOG.info("Using projected metadata with s3a:// paths for table: {}", location);
                return translated;
            }
        } catch (IOException e) {
            LOG.error("Failed to translate ofs:// table metadata for {}, returning original: {}",
                location, e.getMessage(), e);
        }
        return original;
    }

    public void clearCache() {
        int size = linkBucketCache.size();
        linkBucketCache.clear();
        LOG.info("Cleared link bucket cache ({} entries)", size);
    }

    /**
     * Get the S3 volume name
     *
     * @return S3 volume name
     */
    public String getS3VolumeName() {
        return s3VolumeName;
    }

    /**
     * Check if auto-creation of link buckets is enabled
     *
     * @return true if auto-create is enabled
     */
    public boolean isAutoCreateEnabled() {
        return autoCreateLinks;
    }

    /**
     * Check if in-line translation of {ofs://}  metadata to {s3a://} is enabled.
     * @return true if in-line path rewriting is enabled
     */
    public boolean isOzoneInlineRewriteEnabled() {
        return ozoneInlineRewriteEnabled;
    }
}
