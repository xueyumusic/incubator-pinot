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
package com.linkedin.pinot.controller.helix.core.assignment.instance.constraint;

import com.linkedin.pinot.controller.helix.core.assignment.instance.InstanceTags;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


public class TagExactMatchInstanceConstraint implements InstanceConstraint {
  private final Map<String, String> _exactMatchTags;

  public TagExactMatchInstanceConstraint(Map<String, String> exactMatchTags) {
    _exactMatchTags = exactMatchTags;
  }

  @Override
  public Map<Integer, Set<InstanceTags>> apply(Map<Integer, Set<InstanceTags>> replicaIdToInstanceTagsMap) {
    for (Set<InstanceTags> instanceTagsSet : replicaIdToInstanceTagsMap.values()) {
      Iterator<InstanceTags> iterator = instanceTagsSet.iterator();
      while (iterator.hasNext()) {
        InstanceTags instanceTags = iterator.next();
        for (Map.Entry<String, String> entry : _exactMatchTags.entrySet()) {
          String tag = instanceTags.getTag(entry.getKey());
          if (!entry.getValue().equals(tag)) {
            iterator.remove();
            break;
          }
        }
      }
    }
    return replicaIdToInstanceTagsMap;
  }
}
