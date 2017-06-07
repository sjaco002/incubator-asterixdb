/*
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
package org.apache.hyracks.storage.am.lsm.rtree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilterSpecification;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.common.MultiComparator;

public class LSMRTreeWithAntiMatterTuplesDiskComponentBulkLoader extends AbstractLSMDiskComponentBulkLoader {

    //with filter
    public LSMRTreeWithAntiMatterTuplesDiskComponentBulkLoader(LSMRTreeDiskComponent component,
            BloomFilterSpecification bloomFilterSpec, float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex, ILSMComponentFilterManager filterManager, int[] indexFields, int[] filterFields,
            MultiComparator filterCmp) throws HyracksDataException {
        super(component, bloomFilterSpec, fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex, filterManager,
                indexFields, filterFields, filterCmp);
    }

    //without filter
    public LSMRTreeWithAntiMatterTuplesDiskComponentBulkLoader(LSMRTreeDiskComponent component,
            BloomFilterSpecification bloomFilterSpec, float fillFactor, boolean verifyInput, long numElementsHint,
            boolean checkIfEmptyIndex) throws HyracksDataException {
        super(component, bloomFilterSpec, fillFactor, verifyInput, numElementsHint, checkIfEmptyIndex, null, null, null,
                null);
    }

    @Override
    protected BloomFilter getBloomFilter(ILSMDiskComponent component) {
        return ((LSMRTreeDiskComponent) component).getBloomFilter();
    }

    @Override
    protected ITreeIndex getIndex(ILSMDiskComponent component) {
        return ((LSMRTreeDiskComponent) component).getRTree();
    }

}
