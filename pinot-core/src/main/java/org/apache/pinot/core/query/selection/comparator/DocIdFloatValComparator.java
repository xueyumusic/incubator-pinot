/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.query.selection.comparator;

import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.BlockSingleValIterator;


public class DocIdFloatValComparator implements IDocIdValComparator {

  int orderToggleMultiplier = 1;
  private final BlockSingleValIterator blockValSetIterator;

  public DocIdFloatValComparator(Block block, boolean ascending) {
    blockValSetIterator = (BlockSingleValIterator) block.getBlockValueSet().iterator();
    if (!ascending) {
      orderToggleMultiplier = -1;
    }
  }

  public int compare(int docId1, int docId2) {
    blockValSetIterator.skipTo(docId1);
    float val1 = blockValSetIterator.nextFloatVal();
    blockValSetIterator.skipTo(docId2);
    float val2 = blockValSetIterator.nextFloatVal();
    return Float.compare(val1, val2) * orderToggleMultiplier;
  }
}
