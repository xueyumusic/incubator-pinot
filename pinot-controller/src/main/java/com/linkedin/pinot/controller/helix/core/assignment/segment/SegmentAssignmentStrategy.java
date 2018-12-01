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
package com.linkedin.pinot.controller.helix.core.assignment.segment;

import com.linkedin.pinot.common.config.AssignmentConfig;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.controller.helix.core.assignment.instance.InstanceSelectionResult;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


public interface SegmentAssignmentStrategy {

  Map<String, List<String>> assignNewSegment(AssignmentConfig assignmentConfig, SegmentZKMetadata segmentZKMetadata,
      InstanceSelectionResult selectedInstances, @Nullable Map<String, List<String>> existingAssignmentResult);

  Map<String, List<String>> rebalanceSegments(AssignmentConfig assignmentConfig,
      List<SegmentZKMetadata> segmentZKMetadataList, InstanceSelectionResult selectedInstances,
      Map<String, List<String>> existingAssignmentResult);
}
