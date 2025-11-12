package org.apache.iceberg.metering;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.common.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class CatalogMeteringEventPublisher implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(CatalogMeteringEventPublisher.class);
  private static final String FILE_PREFIX = "events.json.";
  private static final String DEFAULT_FILE_PERMISSIONS = "rwxrwxr-x";
  private static final String METERING_FILE_LOC_DIR = "/var/metering/rest-catalog";
  private static final String CONFIG_ACCOUNT_ID = "rest.catalog.env.account.id";
  private static final String CONFIG_ENV_CRN = "rest.catalog.environment.crn";
  private static final String CONFIG_RESOURCE_CRN = "rest.catalog.resource.crn";
  private static final long FILE_ROTATION_INTERVAL_MS = TimeUnit.HOURS.toMillis(24);
  private static final UserGroupInformation UGI;

  static {
    try {
      UGI = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final ScheduledExecutorService scheduler;
  private final Configuration configuration;
  private final AtomicInteger apiCount = new AtomicInteger(0);
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private volatile Path currentFilePath;
  private volatile long currentFileCreationTime;
  private volatile int sequenceNumber;

  public CatalogMeteringEventPublisher(Configuration configuration) {
    this.configuration = configuration;
    this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable, "catalog-metering-publisher");
      thread.setDaemon(true);
      return thread;
    });
    try {
      initialize();
    } catch (Exception ex) {
      logger.error("Error while configuring metering, so the metering events cannot be published", ex);
    }
  }

  private void initialize() {
    initializeDirectory();
    initializeFileState();
    rotateFileIfNeeded();
    publishMeteringEventAtFixedRate();
    addMeteringShutdownHook();
  }

  public void updateAPICount() {
    this.apiCount.getAndIncrement();
  }

  private void publishMeteringEventAtFixedRate() {
    scheduler.scheduleAtFixedRate(this::publishMeteringEventNow, 0, 1, TimeUnit.MINUTES);
  }

  private void publishMeteringEventNow() {
    if (isClosed.get()) {
      return;
    }
    try {
      int currentAPICount = apiCount.getAndSet(0);
      if (currentAPICount == 0) {
        logger.debug("No API calls to meter. Skipping event publication.");
        return;
      }
      CatalogMeteringPayload.CatalogMeteredValue catalogMeteredValue =
          new CatalogMeteringPayload.CatalogMeteredValue(currentAPICount, "IRC_READ_API_COUNT", "API_CALL_COUNT");
      CatalogMeteringEvent event = createMeteringEvent(Collections.singletonList(catalogMeteredValue));
      String meteringEvent = objectMapper.writeValueAsString(event);
      writeOrAppendToFile(meteringEvent);
    } catch (Exception e) {
      logger.error("Error during publishing the metering event", e);
    }
  }

  private synchronized void writeOrAppendToFile(String content) {
    if (isClosed.get()) {
      logger.warn("Attempted to write to a closed metering publisher. Content will be lost.");
      return;
    }
    rotateFileIfNeeded();
    try (
        BufferedWriter bw = new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(currentFilePath.toFile(), true), StandardCharsets.UTF_8))) {
      bw.write(content);
      bw.newLine();
    } catch (IOException e) {
      logger.error("An error occurred while writing to the file: {}", e.getMessage());
    }
  }

  private synchronized void rotateFileIfNeeded() {
    long now = System.currentTimeMillis();
    if (currentFilePath == null || (now - currentFileCreationTime) >= FILE_ROTATION_INTERVAL_MS) {
      currentFilePath = createNewMeteringFile(sequenceNumber++);
      currentFileCreationTime = now;
    }
  }

  private Path createNewMeteringFile(int sequence) {
    Path filePath = Paths.get(METERING_FILE_LOC_DIR, FILE_PREFIX + sequence);
    UGI.doAs((PrivilegedAction<Object>) () -> {
      try {
        Files.createFile(filePath);
        setFilePermissions(filePath);
        logger.info("Created new metering file: {}", filePath);
        return null;
      } catch (IOException ex) {
        logger.error("Error performing Failed to create new metering file {}", filePath, ex);
        throw new RuntimeException("Failed to create new metering file: " + filePath, ex);
      }
    });
    return filePath;
  }

  private void initializeDirectory() {
    Path dirPath = Paths.get(METERING_FILE_LOC_DIR);
    UGI.doAs((PrivilegedAction<Object>) () -> {
      try {
        Files.createDirectories(dirPath);
        setFilePermissions(dirPath);
        logger.info("Created metering directory at {}", dirPath);
        return null;
      } catch (IOException ex) {
        logger.error("Error Failed to create new metering file {}", dirPath, ex);
        throw new RuntimeException("Failed to create new metering file", ex);
      }
    });
  }

  private void setFilePermissions(Path file) {
    try {
      Set<PosixFilePermission> perms = PosixFilePermissions.fromString(DEFAULT_FILE_PERMISSIONS);
      Files.setPosixFilePermissions(file, perms);
      logger.info("Set file permissions on {} to {} => {}", file, DEFAULT_FILE_PERMISSIONS, DEFAULT_FILE_PERMISSIONS);
    } catch (UnsupportedOperationException e) {
      logger.warn("Skipping permission setting on non-POSIX filesystem: {}", e.getMessage());
    } catch (Exception e) {
      logger.warn("Failed to set file permissions on {} to {}: {}", file, DEFAULT_FILE_PERMISSIONS, e.getMessage());
    }
  }

  private CatalogMeteringEvent createMeteringEvent(List<CatalogMeteringPayload.CatalogMeteredValue> accumulativeEvent) {
    return new CatalogMeteringEvent.CatalogMeteringEventBuilder().withAccountId(configuration.get(CONFIG_ACCOUNT_ID))
        .withEnvironmentCrn(configuration.get(CONFIG_ENV_CRN)).withResourceCrn(configuration.get(CONFIG_RESOURCE_CRN))
        .withAccumulativeEvents(new CatalogMeteringPayload(accumulativeEvent)).build();
  }

  private void initializeFileState() {
    Path dirPath = Paths.get(METERING_FILE_LOC_DIR);
    if (Files.notExists(dirPath) || !Files.isDirectory(dirPath)) {
      this.sequenceNumber = 0;
      return;
    }
    int maxSequence = 0;
    Path latestFile = null;

    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath, FILE_PREFIX + "*")) {
      for (Path file : stream) {
        Path fileNamePath = file.getFileName();
        if (fileNamePath == null) {
          logger.warn("Encountered file with null filename: {}", file);
          continue;
        }
        String filename = fileNamePath.toString();
        String[] parts = filename.split("\\.");
        if (parts.length >= 3) {
          try {
            int seq = Integer.parseInt(parts[2]);
            if (seq > maxSequence) {
              maxSequence = seq;
              latestFile = file;
            }
          } catch (NumberFormatException exception) {
            logger.error("Wrong metering event file format", exception);
          }
        }
      }
    } catch (IOException exception) {
      logger.error("Error initializing file state, defaulting to 0", exception);
      this.sequenceNumber = 0;
      return;
    }
    if (latestFile != null) {
      this.currentFilePath = latestFile;
      try {
        this.currentFileCreationTime =
            Files.readAttributes(latestFile, BasicFileAttributes.class).creationTime().toMillis();
        logger.info("Resuming with existing file: {} created at {}", latestFile,
            new Date(this.currentFileCreationTime));
      } catch (IOException e) {
        logger.warn("Could not read creation time for {}. Forcing new file.", latestFile, e);
        this.currentFilePath = null;
        this.currentFileCreationTime = 0;
      }
      this.sequenceNumber = maxSequence + 1;
    } else {
      this.sequenceNumber = 0;
    }
  }

  private void addMeteringShutdownHook() {
    ShutdownHookManager.addGracefulShutDownHook(this::close, 10);
  }

  @Override
  public void close() {
    // We must shut down the scheduler FIRST.
    // This stops any new 1-minute tasks from running and racing with our final publish.
    logger.info("Shutting down catalog metering publisher scheduler...");
    scheduler.shutdown();
    try {
      // Wait for any currently-running task to finish.
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
        logger.warn("Executor did not terminate gracefully. Forcing shutdown.");
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }

    // Now that the scheduler is dead, we are the only thread that can publish.
    // We can safely flush the final count.
    logger.info("metering count to be published before shutdown: {}", apiCount.get());

    // This will now work, because isClosed is still 'false'
    publishMeteringEventNow();

    // FINALLY, set the flag to 'true' to signal we are fully closed.
    // We use compareAndSet as a final guard against concurrent close() calls.
    if (!isClosed.compareAndSet(false, true)) {
      logger.warn("Close method was called concurrently, but shutdown is complete.");
    }

    logger.info("Catalog metering publisher shut down.");
  }
}
