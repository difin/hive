package org.apache.iceberg.metering;

import java.io.Serializable;
import java.util.List;

/**
 * Captures Catalog metering payload created by Catalog Service after rest-catalog APIs are called. This
 * encapsulates API count as quantity that is metered.
 */

public class CatalogMeteringPayload implements Serializable {

  private static final long serialVersionUID = 1L;
  private List<CatalogMeteredValue> accumulativeEvent;

  public CatalogMeteringPayload(List<CatalogMeteredValue> accumulativeEvent) {
    this.accumulativeEvent = accumulativeEvent;
  }

  public List<CatalogMeteredValue> getAccumulativeEvent() {
    return accumulativeEvent;
  }

  public static class CatalogMeteredValue implements Serializable {
    private static final long serialVersionUID = 1L;
    private final long quantity;
    private final String serviceFeature;
    private final String eventType;

    public CatalogMeteredValue(long quantity, String serviceFeature, String eventType) {
      this.quantity = quantity;
      this.serviceFeature = serviceFeature;
      this.eventType = eventType;
    }

    public long getQuantity() {
      return quantity;
    }

    public String getServiceFeature() {
      return serviceFeature;
    }

    public String getEventType() {
      return eventType;
    }
  }
}
