package org.apache.iceberg.rest;
import org.apache.iceberg.rest.RESTRequest;
import org.immutables.value.Value;

@Value.Immutable
public interface RegisterTableRequest extends RESTRequest {

  String name();

  String metadataLocation();

  @Override
  default void validate() {
    // nothing to validate as it's not possible to create an invalid instance
  }
}