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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;


public class InstanceTags {
  private static final char TAG_DELIMITER = ';';
  private static final char KEY_VALUE_DELIMITER = ':';
  private static final char LEGACY_DELIMITER = '_';
  private static final String UNTAGGED = "untagged";

  public static final String ROLE_KEY = "role";
  public static final String POOL_NAME_KEY = "poolName";
  public static final String GROUP_ID_KEY = "groupId";

  private final String _instanceName;
  private final Map<String, String> _tags;

  public InstanceTags(String instanceName, Map<String, String> tags) {
    _instanceName = instanceName;
    _tags = tags;
  }

  public static InstanceTags fromHelixTag(String instanceName, String helixTag) {
    Map<String, String> tags = new HashMap<>();

    // For backward-compatible
    if (helixTag.indexOf(KEY_VALUE_DELIMITER) == -1) {
      int delimiterIndex = helixTag.lastIndexOf(LEGACY_DELIMITER);
      Preconditions.checkArgument(delimiterIndex != -1, "Invalid Helix tag: " + helixTag);

      String part1 = helixTag.substring(0, delimiterIndex);
      String part2 = helixTag.substring(delimiterIndex + 1);
      if (part2.equals(UNTAGGED)) {
        // Untagged instances, e.g. broker_untagged, server_untagged, minion_untagged
        InstanceRole role = InstanceRole.valueOf(part1.toUpperCase());
        tags.put(ROLE_KEY, role.toString());
        tags.put(POOL_NAME_KEY, UNTAGGED);
        tags.put(GROUP_ID_KEY, Integer.toString(0));
      } else {
        // Tagged instances, e.g. poolName_OFFLINE, poolName_REALTIME, poolName_BROKER
        String poolName = helixTag.substring(0, delimiterIndex);
        String role = helixTag.substring(delimiterIndex + 1);
        Preconditions.checkArgument(EnumUtils.isValidEnum(InstanceRole.class, role), "Invalid role: " + role);

        tags.put(ROLE_KEY, role);
        tags.put(POOL_NAME_KEY, poolName);
        tags.put(GROUP_ID_KEY, Integer.toString(0));
      }
    } else {
      // E.g. role:BROKER;poolName:brokerPool;groupId:0
      String[] pairs = StringUtils.split(helixTag, TAG_DELIMITER);
      for (String pair : pairs) {
        String[] keyAndValue = StringUtils.split(pair, KEY_VALUE_DELIMITER);
        Preconditions.checkArgument(keyAndValue.length == 2, "Invalid ket-value pair: " + pair);
        tags.put(keyAndValue[0], keyAndValue[1]);
      }
    }

    return new InstanceTags(instanceName, tags);
  }

  public static String toHelixTag(InstanceTags instanceTags) {
    StringBuilder stringBuilder = new StringBuilder();
    for (Map.Entry<String, String> entry : instanceTags._tags.entrySet()) {
      stringBuilder.append(entry.getKey()).append(KEY_VALUE_DELIMITER).append(entry.getValue()).append(TAG_DELIMITER);
    }
    return stringBuilder.substring(0, stringBuilder.length() - 1);
  }

  public String getInstanceName() {
    return _instanceName;
  }

  @Nullable
  public String getTag(String key) {
    return _tags.get(key);
  }

  @Override
  public int hashCode() {
    return 37 * _instanceName.hashCode() + _tags.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof InstanceTags) {
      InstanceTags that = (InstanceTags) obj;
      return _instanceName.equals(that._instanceName) && _tags.equals(that._tags);
    }
    return false;
  }
}
