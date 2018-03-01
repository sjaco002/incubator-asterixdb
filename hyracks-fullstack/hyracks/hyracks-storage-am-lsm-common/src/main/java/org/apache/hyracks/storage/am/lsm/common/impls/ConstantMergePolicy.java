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

package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.common.IIndexAccessParameters;

public class ConstantMergePolicy implements ILSMMergePolicy {
    private int numComponents;
    private long numFlushes = 0;
    private long numMerges = 0;
    private double mergeCost = 0.0;
    private static final Logger LOGGER = Logger.getLogger(ConstantMergePolicy.class.getName());

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested, boolean isMergeOp)
            throws HyracksDataException {
        if (!isMergeOp) {
            numFlushes++;
        }
        List<ILSMDiskComponent> immutableComponents = index.getDiskComponents();
        if (!areComponentsMergable(immutableComponents)) {
            return;
        }

        if (fullMergeIsRequested) {
            IIndexAccessParameters iap =
                    new IndexAccessParameters(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            ILSMIndexAccessor accessor = index.createAccessor(iap);
            accessor.scheduleFullMerge(index.getIOOperationCallback());
            long mergeSize = getMergeSize(immutableComponents);
            logDiskComponentsSnapshot(immutableComponents);
            logMergeInfo(mergeSize, true, immutableComponents.size(), immutableComponents.size());
            numMerges++;
            mergeCost = mergeCost + ((double) mergeSize) / (1024 * 1024 * 1024);
        } else if (immutableComponents.size() >= numComponents) {
            ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            accessor.scheduleMerge(index.getIOOperationCallback(), immutableComponents);
            long mergeSize = getMergeSize(immutableComponents);
            logDiskComponentsSnapshot(immutableComponents);
            logMergeInfo(mergeSize, false, immutableComponents.size(), immutableComponents.size());
            numMerges++;
            mergeCost = mergeCost + ((double) mergeSize) / (1024 * 1024 * 1024);
        }
    }

    @Override
    public void configure(Map<String, String> properties) {
        numComponents = Integer.parseInt(properties.get(ConstantMergePolicyFactory.NUM_COMPONENTS));
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        // see PrefixMergePolicy.isMergeLagging() for the rationale behind this code.

        /**
         * case 1.
         * if totalImmutableCommponentCount < threshold,
         * merge operation is not lagged ==> return false.
         * case 2.
         * if a) totalImmutableCommponentCount >= threshold && b) there is an ongoing merge,
         * merge operation is lagged. ==> return true.
         * case 3. *SPECIAL CASE*
         * if a) totalImmutableCommponentCount >= threshold && b) there is *NO* ongoing merge,
         * merge operation is lagged. ==> *schedule a merge operation* and then return true.
         * This is a special case that requires to schedule a merge operation.
         * Otherwise, all flush operations will be hung.
         * This case can happen in a following situation:
         * The system may crash when
         * condition 1) the mergableImmutableCommponentCount >= threshold and
         * condition 2) merge operation is going on.
         * After the system is recovered, still condition 1) is true.
         * If there are flush operations in the same dataset partition after the recovery,
         * all these flush operations may not proceed since there is no ongoing merge and
         * there will be no new merge either in this situation.
         */

        List<ILSMDiskComponent> immutableComponents = index.getDiskComponents();
        int totalImmutableComponentCount = immutableComponents.size();

        // [case 1]
        if (totalImmutableComponentCount < numComponents) {
            return false;
        }

        boolean isMergeOngoing = isMergeOngoing(immutableComponents);

        // here, implicitly (totalImmutableComponentCount >= numComponents) is true by passing case 1.
        if (isMergeOngoing) {
            // [case 2]
            return true;
        } else {
            // [case 3]
            // schedule a merge operation after making sure that all components are mergable
            if (!areComponentsMergable(immutableComponents)) {
                throw new IllegalStateException();
            }
            ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            accessor.scheduleMerge(index.getIOOperationCallback(), immutableComponents);
            long mergeSize = getMergeSize(immutableComponents);
            logDiskComponentsSnapshot(immutableComponents);
            logMergeInfo(mergeSize, false, immutableComponents.size(), immutableComponents.size());
            numMerges++;
            mergeCost = mergeCost + ((double) mergeSize) / (1024 * 1024 * 1024);
            return true;
        }
    }

    /**
     * checks whether all given components are mergable or not
     *
     * @param immutableComponents
     * @return true if all components are mergable, false otherwise.
     */
    private boolean areComponentsMergable(List<ILSMDiskComponent> immutableComponents) {
        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ComponentState.READABLE_UNWRITABLE) {
                return false;
            }
        }
        return true;
    }

    /**
     * This method returns whether there is an ongoing merge operation or not by checking
     * each component state of given components.
     *
     * @return true if there is an ongoing merge operation, false otherwise.
     */
    private boolean isMergeOngoing(List<ILSMDiskComponent> immutableComponents) {
        int size = immutableComponents.size();
        for (int i = 0; i < size; i++) {
            if (immutableComponents.get(i).getState() == ComponentState.READABLE_MERGING) {
                return true;
            }
        }
        return false;
    }

    private void logMergeInfo(long size, boolean isFullMerge, int mergedComponents, int totalComponents) {
        if (LOGGER.isLoggable(Level.SEVERE)) {
            if (isFullMerge) {
                LOGGER.severe(
                        "Full Merged: " + size + ", " + mergedComponents + ", " + totalComponents + ", " + new Date());
            } else {
                LOGGER.severe("Merged: " + size + ", " + mergedComponents + ", " + totalComponents + ", " + new Date());
            }
        }
    }

    private long getMergeSize(List<ILSMDiskComponent> immutableComponents) {
        long mergeSize = 0;
        for (int j = 0; j < immutableComponents.size(); j++) {
            mergeSize = mergeSize + immutableComponents.get(j).getComponentSize();
        }
        return mergeSize;
    }

    private void logDiskComponentsSnapshot(List<ILSMDiskComponent> immutableComponents) {

        if (LOGGER.isLoggable(Level.SEVERE)) {
            String snapshotStr = "";
            for (int j = 0; j < immutableComponents.size(); j++) {

                snapshotStr = snapshotStr + "," + immutableComponents.get(j).getComponentSize();
            }
            if (snapshotStr.length() > 1) {
                snapshotStr = snapshotStr.substring(1);
            }
            LOGGER.severe("Merge Snapshot: " + snapshotStr);
        }
    }

    @Override
    public long getNumberOfFlushes() {

        return numFlushes;
    }

    @Override
    public long getNumberOfMerges() {

        return numMerges;
    }

    @Override
    public double getMergeCost() {

        return mergeCost;
    }
}
