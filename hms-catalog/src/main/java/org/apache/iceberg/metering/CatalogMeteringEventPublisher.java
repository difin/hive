package org.apache.iceberg.metering;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
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
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CatalogMeteringEventPublisher {
  private static final Logger logger = LoggerFactory.getLogger(CatalogMeteringEventPublisher.class);
  private static final String FILE_PREFIX = "events.json.";
  private static final String DEFAULT_FILE_PERMISSIONS = "775";
  private static final String METERING_FILE_LOC_DIR = "/var/metering/rest-catalog";
  private static final long FILE_ROTATION_INTERVAL_MS = TimeUnit.HOURS.toMillis(24);
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final Configuration configuration;
  private final AtomicInteger apiCount = new AtomicInteger(0);
  private volatile Path currentFilePath;
  private volatile long currentFileCreationTime;
  private volatile int sequenceNumber;
  private static final UserGroupInformation UGI;

  static {
    try {
      UGI = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public CatalogMeteringEventPublisher(Configuration configuration) {
    this.configuration = configuration;
    try{
      initializeDirectory();
      this.sequenceNumber = initializeSequenceNumber();
      rotateFileIfNeeded();
      publishMeteringEventAtFixedRate();
    } catch (Exception ex) {
      logger.error("Error while configuring metering, so the metering events cannot be published", ex);
    }
  }

  public void updateAPICount() {
    this.apiCount.getAndIncrement();
  }

  private void publishMeteringEventAtFixedRate() {
    scheduler.scheduleAtFixedRate(() -> {
      try {
        int currentAPICount = apiCount.getAndSet(0);
        CatalogMeteringPayload.CatalogMeteredValue catalogMeteredValue =
            new CatalogMeteringPayload.CatalogMeteredValue(currentAPICount, "IRC_READ_API_COUNT", "API_CALL_COUNT");
        CatalogMeteringEvent event = createMeteringEvent(Collections.singletonList(catalogMeteredValue));
        String meteringEvent = objectMapper.writeValueAsString(event);
        writeOrAppendToFile(meteringEvent);
      } catch (Exception e) {
        logger.error("Error during publishing the metering event", e);
      }
    }, 0, 1, TimeUnit.MINUTES);
  }

  private void writeOrAppendToFile(String content) {
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
      String rwxPerms = parsePosixPerm();
      Set<PosixFilePermission> perms = PosixFilePermissions.fromString(rwxPerms);
      Files.setPosixFilePermissions(file, perms);
      logger.info("Set file permissions on {} to {} => {}", file, DEFAULT_FILE_PERMISSIONS, rwxPerms);
    } catch (UnsupportedOperationException e) {
      logger.warn("Skipping permission setting on non-POSIX filesystem: {}", e.getMessage());
    } catch (Exception e) {
      logger.warn("Failed to set file permissions on {} to {}: {}", file, DEFAULT_FILE_PERMISSIONS, e.getMessage());
    }
  }

  private String parsePosixPerm() {
    final String[] MAP = { "---", "--x", "-w-", "-wx", "r--", "r-x", "rw-", "rwx" };
    StringBuilder result = new StringBuilder();
    for (char c : DEFAULT_FILE_PERMISSIONS.toCharArray()) {
      int val = c - '0';
      if (val < 0 || val > 7) {
        throw new IllegalArgumentException("Invalid permission digit: " + c);
      }
      result.append(MAP[val]);
    }
    return result.toString();
  }

  private CatalogMeteringEvent createMeteringEvent(List<CatalogMeteringPayload.CatalogMeteredValue> accumulativeEvent) {
    return new CatalogMeteringEvent.CatalogMeteringEventBuilder().withAccountId(
            configuration.get("rest.catalog.env.account.id"))
        .withEnvironmentCrn(configuration.get("rest.catalog.environment.crn"))
        .withResourceCrn(configuration.get("rest.catalog.resource.crn"))
        .withAccumulativeEvents(new CatalogMeteringPayload(accumulativeEvent)).build();
  }

  private int initializeSequenceNumber() {
    try {
      Path dirPath = Paths.get(METERING_FILE_LOC_DIR);
      if (Files.notExists(dirPath) || !Files.isDirectory(dirPath)) {
        return 0;
      }
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath, FILE_PREFIX + "*")) {
        int maxSequence = 0;
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
              }
            } catch (NumberFormatException exception) {
              logger.error("Wrong metering event file format", exception);
            }
          }
        }
        return maxSequence + 1;
      }
    } catch (IOException exception) {
      logger.error("Error initializing sequence number, defaulting to 0", exception);
      return 0;
    }
  }

}
