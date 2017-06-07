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
package org.apache.hyracks.storage.am.lsm.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.common.IIndexBulkLoader;

public class LSMBTreeBulkLoader implements IIndexBulkLoader {
    private final LSMBTree lsmIndex;
    private final ILSMDiskComponent component;
    private final IIndexBulkLoader componentBulkLoader;

    public LSMBTreeBulkLoader(LSMBTree lsmIndex, float fillFactor, boolean verifyInput, long numElementsHint)
            throws HyracksDataException {
        this.lsmIndex = lsmIndex;
        this.component = lsmIndex.createBulkLoadTarget();
        this.componentBulkLoader =
                lsmIndex.createComponentBulkLoader(component, fillFactor, verifyInput, numElementsHint, false, true);
    }

    @Override
    public void add(ITupleReference tuple) throws HyracksDataException {
        componentBulkLoader.add(tuple);
    }

    @Override
    public void end() throws HyracksDataException {
        componentBulkLoader.end();
        if (component.getComponentSize() > 0) {
            //TODO(amoudi): Ensure Bulk load follow the same lifecycle Other Operations (Flush, Merge, etc).
            //then after operation should be called from harness as well
            //https://issues.apache.org/jira/browse/ASTERIXDB-1764
            lsmIndex.getIOOperationCallback().afterOperation(LSMOperationType.FLUSH, null, component);
            lsmIndex.getLsmHarness().addBulkLoadedComponent(component);
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        componentBulkLoader.end();
    }

}