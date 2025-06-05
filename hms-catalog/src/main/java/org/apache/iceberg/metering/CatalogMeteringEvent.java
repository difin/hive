package org.apache.iceberg.metering;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

/**
 * Represents Catalog metering event to be consumed by metering service running on CM as an agent.
 */

public class CatalogMeteringEvent implements Serializable {
  private static final long serialVersionUID = 1L;
  private final int version = 1;
  private final String id = UUID.randomUUID().toString();
  private final long timestamp = Instant.now().toEpochMilli();
  private String serviceType = "CDSH";
  private final String accountId;
  private final String resourceCrn;
  private final String environmentCrn;
  private final CatalogMeteringPayload accumulativeEvents;

  public CatalogMeteringEvent(CatalogMeteringEventBuilder builder) {
    this.accountId = builder.accountId;
    this.resourceCrn = builder.resourceCrn;
    this.accumulativeEvents = builder.accumulativeEvents;
    this.environmentCrn = builder.environmentCrn;
  }

  public String getId() {
    return id;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getServiceType() {
    return serviceType;
  }

  public int getVersion() {
    return version;
  }

  public String getAccountId() {
    return accountId;
  }

  public String getResourceCrn() {
    return resourceCrn;
  }

  public String getEnvironmentCrn() {
    return environmentCrn;
  }

  public CatalogMeteringPayload getAccumulativeEvents() {
    return accumulativeEvents;
  }

  public static class CatalogMeteringEventBuilder {
    private String accountId;
    private String resourceCrn;
    private String environmentCrn;
    private CatalogMeteringPayload accumulativeEvents;

    public CatalogMeteringEventBuilder withAccountId(String accountId) {
      this.accountId = accountId;
      return this;
    }

    public CatalogMeteringEventBuilder withResourceCrn(String resourceCrn) {
      this.resourceCrn = resourceCrn;
      return this;
    }

    public CatalogMeteringEventBuilder withEnvironmentCrn(String environmentCrn) {
      this.environmentCrn = environmentCrn;
      return this;
    }

    public CatalogMeteringEventBuilder withAccumulativeEvents(CatalogMeteringPayload accumulativeEvents) {
      this.accumulativeEvents = accumulativeEvents;
      return this;
    }

    public CatalogMeteringEvent build() {
      return new CatalogMeteringEvent(this);
    }
  }
}
