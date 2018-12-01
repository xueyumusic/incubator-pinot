/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.helix.core.assignment.instance;

import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class InstanceTagsTest {

  @Test
  public void testUntaggedInstance() {
    InstanceTags instanceTags = InstanceTags.fromHelixTag("instance", "controller_untagged");
    assertEquals(instanceTags.getInstanceName(), "instance");
    assertEquals(instanceTags.getTag(InstanceTags.ROLE_KEY), InstanceRole.CONTROLLER.toString());
    assertEquals(instanceTags.getTag(InstanceTags.POOL_NAME_KEY), "untagged");
    assertEquals(instanceTags.getTag(InstanceTags.GROUP_ID_KEY), "0");

    instanceTags = InstanceTags.fromHelixTag("instance", "broker_untagged");
    assertEquals(instanceTags.getInstanceName(), "instance");
    assertEquals(instanceTags.getTag(InstanceTags.ROLE_KEY), InstanceRole.BROKER.toString());
    assertEquals(instanceTags.getTag(InstanceTags.POOL_NAME_KEY), "untagged");
    assertEquals(instanceTags.getTag(InstanceTags.GROUP_ID_KEY), "0");

    instanceTags = InstanceTags.fromHelixTag("instance", "server_untagged");
    assertEquals(instanceTags.getInstanceName(), "instance");
    assertEquals(instanceTags.getTag(InstanceTags.ROLE_KEY), InstanceRole.SERVER.toString());
    assertEquals(instanceTags.getTag(InstanceTags.POOL_NAME_KEY), "untagged");
    assertEquals(instanceTags.getTag(InstanceTags.GROUP_ID_KEY), "0");

    instanceTags = InstanceTags.fromHelixTag("instance", "minion_untagged");
    assertEquals(instanceTags.getInstanceName(), "instance");
    assertEquals(instanceTags.getTag(InstanceTags.ROLE_KEY), InstanceRole.MINION.toString());
    assertEquals(instanceTags.getTag(InstanceTags.POOL_NAME_KEY), "untagged");
    assertEquals(instanceTags.getTag(InstanceTags.GROUP_ID_KEY), "0");
  }

  @Test
  public void testLegacyHelixTag() {
    InstanceTags instanceTags = InstanceTags.fromHelixTag("instance", "controllerPool_CONTROLLER");
    assertEquals(instanceTags.getInstanceName(), "instance");
    assertEquals(instanceTags.getTag(InstanceTags.ROLE_KEY), InstanceRole.CONTROLLER.toString());
    assertEquals(instanceTags.getTag(InstanceTags.POOL_NAME_KEY), "controllerPool");
    assertEquals(instanceTags.getTag(InstanceTags.GROUP_ID_KEY), "0");

    instanceTags = InstanceTags.fromHelixTag("instance", "brokerPool_BROKER");
    assertEquals(instanceTags.getInstanceName(), "instance");
    assertEquals(instanceTags.getTag(InstanceTags.ROLE_KEY), InstanceRole.BROKER.toString());
    assertEquals(instanceTags.getTag(InstanceTags.POOL_NAME_KEY), "brokerPool");
    assertEquals(instanceTags.getTag(InstanceTags.GROUP_ID_KEY), "0");

    instanceTags = InstanceTags.fromHelixTag("instance", "offlineServerPool_OFFLINE");
    assertEquals(instanceTags.getInstanceName(), "instance");
    assertEquals(instanceTags.getTag(InstanceTags.ROLE_KEY), InstanceRole.OFFLINE.toString());
    assertEquals(instanceTags.getTag(InstanceTags.POOL_NAME_KEY), "offlineServerPool");
    assertEquals(instanceTags.getTag(InstanceTags.GROUP_ID_KEY), "0");

    instanceTags = InstanceTags.fromHelixTag("instance", "realtimeServerPool_REALTIME");
    assertEquals(instanceTags.getInstanceName(), "instance");
    assertEquals(instanceTags.getTag(InstanceTags.ROLE_KEY), InstanceRole.REALTIME.toString());
    assertEquals(instanceTags.getTag(InstanceTags.POOL_NAME_KEY), "realtimeServerPool");
    assertEquals(instanceTags.getTag(InstanceTags.GROUP_ID_KEY), "0");

    instanceTags = InstanceTags.fromHelixTag("instance", "minionPool_MINION");
    assertEquals(instanceTags.getInstanceName(), "instance");
    assertEquals(instanceTags.getTag(InstanceTags.ROLE_KEY), InstanceRole.MINION.toString());
    assertEquals(instanceTags.getTag(InstanceTags.POOL_NAME_KEY), "minionPool");
    assertEquals(instanceTags.getTag(InstanceTags.GROUP_ID_KEY), "0");
  }

  @Test
  public void testKeyValuePairs() {
    InstanceTags instanceTags = InstanceTags.fromHelixTag("instance", "key1:value1;key2:value2;key3:value3");
    assertEquals(instanceTags.getInstanceName(), "instance");
    assertEquals(instanceTags.getTag("key1"), "value1");
    assertEquals(instanceTags.getTag("key2"), "value2");
    assertEquals(instanceTags.getTag("key3"), "value3");
  }

  @Test
  public void testIllegalHelixTag() {
    try {
      InstanceTags.fromHelixTag("instance", "illegalRole_untagged");
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }

    try {
      InstanceTags.fromHelixTag("instance", "somePool_ILLEGALROLE");
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }

    try {
      InstanceTags.fromHelixTag("instance", "illegal:key:value:pair;key:value");
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }
}
