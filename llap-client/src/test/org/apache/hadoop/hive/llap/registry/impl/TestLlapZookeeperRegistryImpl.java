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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.registry.impl;

import static java.lang.Integer.parseInt;
import static org.apache.hadoop.hive.llap.registry.impl.LlapZookeeperRegistryImpl.COMPUTE_NAME_PROPERTY_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstanceSet;
import org.apache.hadoop.hive.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.registry.impl.ZkRegistryBase;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.stream.Collectors;

public class TestLlapZookeeperRegistryImpl {

  public static final String COMPUTE_1 = "compute1";
  public static final String COMPUTE_2 = "compute2";
  private static final long TIMEOUT_MS = 10000;
  private HiveConf hiveConf = new HiveConf();

  private LlapZookeeperRegistryImpl registry;
  private LlapZookeeperRegistryImpl compute1;

  private CuratorFramework curatorFramework;
  private TestingServer server;
  private final static String NAMESPACE_PREFIX = "llap-";

  @Before
  public void setUp() throws Exception {
    hiveConf.set(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM.varname, "localhost");
    registry = new LlapZookeeperRegistryImpl("TestLlapZookeeperRegistryImpl", hiveConf);
    compute1 = new LlapZookeeperRegistryImpl("TestLlapZookeeperRegistryImpl", COMPUTE_1, hiveConf);

    server = new TestingServer();
    server.start();

    curatorFramework = CuratorFrameworkFactory.
            builder().
            connectString(server.getConnectString()).
            sessionTimeoutMs(1000).
            retryPolicy(new RetryOneTime(1000)).
            build();
    curatorFramework.start();

    trySetMock(registry, "zooKeeperClient", curatorFramework);
    trySetMock(compute1, "zooKeeperClient", curatorFramework);
  }

  @After
  public void tearDown() throws IOException {
    curatorFramework.close();
    server.stop();
  }

  @Test
  public void testRegister() throws Exception {
    // Given
    int expectedExecutorCount = HiveConf.getIntVar(hiveConf, HiveConf.ConfVars.LLAP_DAEMON_NUM_EXECUTORS);
    int expectedQueueSize = HiveConf.getIntVar(hiveConf, HiveConf.ConfVars.LLAP_DAEMON_TASK_SCHEDULER_WAIT_QUEUE_SIZE);

    // When
    registry.register();
    ServiceInstanceSet<LlapServiceInstance> serviceInstanceSet =
        registry.getInstances("LLAP", 1000);

    // Then
    Collection<LlapServiceInstance> llaps = serviceInstanceSet.getAll();
    assertEquals(1, llaps.size());
    LlapServiceInstance serviceInstance = llaps.iterator().next();
    Map<String, String> attributes = serviceInstance.getProperties();

    assertEquals(expectedQueueSize,
        parseInt(attributes.get(LlapRegistryService.LLAP_DAEMON_TASK_SCHEDULER_ENABLED_WAIT_QUEUE_SIZE)));
    assertEquals(expectedExecutorCount,
        parseInt(attributes.get(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS)));
  }

  @Test
  public void testUpdate() throws Exception {
    // Given
    String expectedExecutorCount = "2";
    String expectedQueueSize = "20";
    Map<String, String> capacityValues = new HashMap<>(2);
    capacityValues.put(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS, expectedExecutorCount);
    capacityValues.put(LlapRegistryService.LLAP_DAEMON_TASK_SCHEDULER_ENABLED_WAIT_QUEUE_SIZE, expectedQueueSize);

    // When
    registry.register();
    registry.updateRegistration(capacityValues.entrySet());
    ServiceInstanceSet<LlapServiceInstance> serviceInstanceSet =
            registry.getInstances("LLAP", 1000);

    // Then
    Collection<LlapServiceInstance> llaps = serviceInstanceSet.getAll();
    assertEquals(1, llaps.size());
    LlapServiceInstance serviceInstance = llaps.iterator().next();
    Map<String, String> attributes = serviceInstance.getProperties();

    assertEquals(expectedQueueSize,
            attributes.get(LlapRegistryService.LLAP_DAEMON_TASK_SCHEDULER_ENABLED_WAIT_QUEUE_SIZE));
    assertEquals(expectedExecutorCount,
            attributes.get(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS));
  }

  @Test
  public void testComputeGroup() throws Exception {
    LlapServiceInstanceSet instances = compute1.getInstances("LLAP", 1000);
    PersistentEphemeralNode znode1 = createZnodeForComputeGroup(COMPUTE_1, "id1");
    PersistentEphemeralNode znode2 = createZnodeForComputeGroup(COMPUTE_2, "id2");

    assertEventually(() -> instances.getAllForComputeGroup(s -> COMPUTE_1.equals(s)).size() == 1,
            "should see 1 instance in compute group 1");

    assertEventually(() -> instances.getAllForComputeGroup(Pattern.compile(".*").asPredicate()).size() == 2,
            "should see all the two instances from all compute groups");

    assertEventually(() -> instances.getAll().size() == 1, "should have size 1 after adding 1 to its compute group");
    CloseableUtils.closeQuietly(znode2);
    assertEventually(() -> instances.getAll().size() == 1, "should have size 1 after removing from different compute group");
    CloseableUtils.closeQuietly(znode1);
    assertEventually(() -> instances.getAll().size() == 0, "should have size 0 after removing from same compute group");
  }

  @Test
  public void testPersistentNodePath() {
    String persistentNodeName = "/pnode0";

    Assert.assertEquals("/user-" + System.getProperty("user.name") + persistentNodeName,
        registry.getPersistentNodePath());
  }

  private static String getAllThreadStacksAsString() {
    // dump stack traces with info about locks
    ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] infos = bean.dumpAllThreads(true, true);
    return Arrays.stream(infos).map(Object::toString).collect(Collectors.joining());
  }

  static <T> void assertEventually(Callable<Boolean> matcher, String message) throws Exception {
    long started = System.currentTimeMillis();
    while (!matcher.call()) {
      Thread.sleep(100);
      if (System.currentTimeMillis() - started > TIMEOUT_MS) {
        final String dump = getAllThreadStacksAsString();
        fail(message + " was not satisfied withing " + TIMEOUT_MS + "ms. Stack traces:\n\n" + dump);
      }
    }
  }

  private PersistentEphemeralNode createZnodeForComputeGroup(final String computeGroup, String id) throws Exception {
    ServiceRecord record = new ServiceRecord();
    record.addInternalEndpoint(RegistryTypeUtils.ipcEndpoint("llap", new InetSocketAddress("dynamic", 4000)));
    record.addInternalEndpoint(RegistryTypeUtils.ipcEndpoint("shuffle", new InetSocketAddress("dynamic", 4000)));
    record.addInternalEndpoint(RegistryTypeUtils.ipcEndpoint("llapmng", new InetSocketAddress("dynamic", 4000)));
    record.addInternalEndpoint(RegistryTypeUtils.ipcEndpoint("llapoutputformat", new InetSocketAddress("dynamic", 4000)));
    record.addExternalEndpoint(RegistryTypeUtils.webEndpoint("services", new URI("dynamic" + ":4000")));
    record.set(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS, 10);
    record.set(HiveConf.ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname, 100);
    record.set(ZkRegistryBase.UNIQUE_IDENTIFIER, id);
    record.set(COMPUTE_NAME_PROPERTY_KEY, computeGroup);
    PersistentEphemeralNode znode = new PersistentEphemeralNode(
            curatorFramework,
            PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL,
            compute1.getWorkersPath().replace(compute1.getComputeGroup(), computeGroup) + "/worker-",
            new RegistryUtils.ServiceRecordMarshal().toBytes(record));
    znode.start();
    if (!znode.waitForInitialCreate(10, TimeUnit.SECONDS)) {
      fail("Max znode creation wait time exhausted");
    }
    return znode;
  }

  static <T> void trySetMock(Object o, String field, T value) {
    try {
      Field fieldToChange = Arrays.stream(FieldUtils.getAllFields(o.getClass()))
              .filter(f -> f.getName().equals(field))
              .findFirst()
              .orElseThrow(NoSuchFieldException::new);

      fieldToChange.setAccessible(true);

      fieldToChange.set(o, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Mocking is only supported, if only one field is assignable from the given class.");
    }
  }
}
