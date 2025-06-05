package org.apache.iceberg.rest;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.*;

public class TestDataSharingHA {

  @Rule
  public WireMockRule broker0 = new WireMockRule(18080);
  @Rule
  public WireMockRule broker1 = new WireMockRule(18081);
  @Rule
  public WireMockRule broker2 = new WireMockRule(18082);

  @Test
  public void testRoundRobinAndFailover() throws Exception {
    broker0.addMockServiceRequestListener(
        (request, response) -> System.out.println("Broker0 received request: " + request.getUrl()));
    broker1.addMockServiceRequestListener(
        (request, response) -> System.out.println("Broker1 received request: " + request.getUrl()));

    /* broker-0 returns 404, broker-1 returns a valid delegation token */
    broker0.stubFor(get(urlMatching(".*/dt/knoxtoken/api/v1/token"))
        .willReturn(aResponse().withStatus(404)));
    broker1.stubFor(
        get(urlMatching(".*/dt/knoxtoken/api/v1/token"))
            .willReturn(okJson("{\"access_token\":\"tok\",\"expires_in\":3600}")));
    broker1.stubFor(
        get(urlMatching(".*/aws-cab/cab/api/v1/credentials.*"))
            .willReturn(okJson("{\"Credentials\": {\"SessionToken\":\"tok\",\"AccessKeyId\":\"AK123\",\"SecretAccessKey\":\"secret\"}}")));

    Configuration conf = new Configuration();
    conf.set("hive.metastore.catalog.idbroker.url",
        "http://localhost:18080/gateway,http://localhost:18081/gateway");

    DataSharing ds = new DataSharing(conf);
    System.out.println("Broker URLs: " + String.join(", ", conf.get("hive.metastore.catalog.idbroker.url").split(",")));

    /* should transparently fall over to broker-1 */
    Map<String, String> creds = ds.getAccessToken("s3a://bucket/table/metadata/v1.metadata.json");
    assertNotNull("access token fetched", creds);
    assertEquals("tok", creds.get(DataSharing.S3_SESSION_TOKEN));
  }

  @Test
  public void testLastKnownGoodTracking() throws Exception {
    broker0.addMockServiceRequestListener(
        (request, response) -> System.out.println("Broker0 received request: " + request.getUrl()));
    broker1.addMockServiceRequestListener(
        (request, response) -> System.out.println("Broker1 received request: " + request.getUrl()));

    /* Set up two brokers with the first one failing and the second succeeding */
    broker0.stubFor(get(urlMatching(".*/dt/knoxtoken/api/v1/token")).willReturn(aResponse().withStatus(500)));
    broker1.stubFor(
        get(urlMatching(".*/dt/knoxtoken/api/v1/token"))
            .willReturn(okJson("{\"access_token\":\"tok1\",\"expires_in\":3600}")));
    broker1.stubFor(
        get(urlMatching(".*/aws-cab/cab/api/v1/credentials.*"))
            .willReturn(okJson("{\"Credentials\": {\"SessionToken\":\"tok1\",\"AccessKeyId\":\"AK123\",\"SecretAccessKey\":\"secret\"}}")));

    Configuration conf = new Configuration();
    conf.set("hive.metastore.catalog.idbroker.url",
        "http://localhost:18080/gateway,http://localhost:18081/gateway");

    DataSharing ds = new DataSharing(conf);

    /* First call should fall over to broker-1 */
    Map<String, String> creds = ds.getAccessToken("s3a://bucket/table/metadata/v1.metadata.json");
    assertNotNull("access token fetched", creds);
    assertEquals("tok1", creds.get(DataSharing.S3_SESSION_TOKEN));

    /* Change the responses - now broker0 works and broker1 fails */
    broker0.stubFor(
        get(urlMatching(".*/dt/knoxtoken/api/v1/token"))
            .willReturn(okJson("{\"access_token\":\"tok2\",\"expires_in\":3600}")));
    broker0.stubFor(
        get(urlMatching(".*/aws-cab/cab/api/v1/credentials.*"))
            .willReturn(okJson("{\"Credentials\": {\"SessionToken\":\"tok2\",\"AccessKeyId\":\"AK123\",\"SecretAccessKey\":\"secret\"}}")));
    broker1.stubFor(get(urlMatching(".*/dt/knoxtoken/api/v1/token")).willReturn(aResponse().withStatus(500)));

    /* Force token to be refreshed using the test helper method */
    ds.clearDelegationTokenCache();

    /* Second call should use broker-1 first (lastKnownGood) and then fall back to broker-0 */
    creds = ds.getAccessToken("s3a://bucket/table/metadata/v1.metadata.json");
    assertNotNull("access token fetched", creds);
    assertEquals("tok2", creds.get(DataSharing.S3_SESSION_TOKEN));
  }

  @Test(expected = NotAuthorizedException.class)
  public void testAllEndpointsFailure() throws Exception {
    // Set up request logging for debugging
    broker0.addMockServiceRequestListener(
        (request, response) -> System.out.println("Broker0 received request: " + request.getUrl()));
    broker1.addMockServiceRequestListener(
        (request, response) -> System.out.println("Broker1 received request: " + request.getUrl()));
    broker2.addMockServiceRequestListener(
        (request, response) -> System.out.println("Broker2 received request: " + request.getUrl()));

    /* All brokers return failures */
    broker0.stubFor(get(urlMatching(".*/dt/knoxtoken/api/v1/token")).willReturn(aResponse().withStatus(404)));
    broker1.stubFor(get(urlMatching(".*/dt/knoxtoken/api/v1/token")).willReturn(aResponse().withStatus(500)));
    broker2.stubFor(get(urlMatching(".*/dt/knoxtoken/api/v1/token")).willReturn(aResponse().withStatus(403)));

    Configuration conf = new Configuration();
    conf.set("hive.metastore.catalog.idbroker.url",
        "http://localhost:18080/gateway,http://localhost:18081/gateway,http://localhost:18082/gateway");

    DataSharing ds = new DataSharing(conf);
    /* Should throw NotAuthorizedException since all endpoints fail */
    ds.getAccessToken("s3a://bucket/table/metadata/v1.metadata.json");
    fail("Should have thrown NotAuthorizedException");
  }
}
