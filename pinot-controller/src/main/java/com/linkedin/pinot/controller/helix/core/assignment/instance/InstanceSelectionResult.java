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

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class InstanceSelectionResult {
  private final Map<Integer, List<String>> _replicaIdToInstancesMap;

  public InstanceSelectionResult() {
    _replicaIdToInstancesMap = new HashMap<>();
  }

  public InstanceSelectionResult(Map<Integer, List<String>> replicaIdToInstancesMap) {
    _replicaIdToInstancesMap = replicaIdToInstancesMap;
  }

  public int getNumReplicas() {
    return _replicaIdToInstancesMap.size();
  }

  public List<String> getInstances(int replicaId) {
    return _replicaIdToInstancesMap.get(replicaId);
  }

  public void setInstances(int replicaId, List<String> instances) {
    _replicaIdToInstancesMap.put(replicaId, instances);
  }
}
