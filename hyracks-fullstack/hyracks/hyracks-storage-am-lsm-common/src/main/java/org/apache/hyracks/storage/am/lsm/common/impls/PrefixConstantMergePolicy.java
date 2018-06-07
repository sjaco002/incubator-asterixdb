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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class PrefixConstantMergePolicy implements ILSMMergePolicy {

    private long maxMergableComponentSize;
    private int maxToleranceComponentCount;
    private int maxTotalNumberComponents;
    private static final Logger LOGGER = Logger.getLogger(PrefixConstantMergePolicy.class.getName());
    private long numFlushes = 0;
    private long numMerges = 0;
    private double mergeCost = 0.0;

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested, boolean isMergeOp)
            throws HyracksDataException {
        if (!isMergeOp) {
            numFlushes++;
        }
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());

        if (!areComponentsReadableWritableState(immutableComponents)) {
            return;
        }

        if (fullMergeIsRequested) {
            ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
            accessor.scheduleFullMerge();
            long mergeSize = getMergeSize(immutableComponents);
            logDiskComponentsSnapshot(immutableComponents);
            logMergeInfo(mergeSize, true, immutableComponents.size(), immutableComponents.size());
            numMerges++;
            mergeCost = mergeCost + ((double) mergeSize) / (1024 * 1024 * 1024);
            return;
        }

        scheduleMerge(index);
    }

    @Override
    public void configure(Map<String, String> properties) {
        maxMergableComponentSize = Long.parseLong(properties.get("max-mergable-component-size"));
        maxToleranceComponentCount = Integer.parseInt(properties.get("max-tolerance-component-count"));
        maxTotalNumberComponents = Integer.parseInt(properties.get("max-total-number-components"));
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {

        List<ILSMDiskComponent> immutableComponents = index.getDiskComponents();
        int totalImmutableComponentCount = immutableComponents.size();

        // [case 1]
        if (totalImmutableComponentCount < maxTotalNumberComponents) {
            return false;
        }

        boolean isMergeOngoing = isMergeOngoing(immutableComponents);

        if (isMergeOngoing) {
            // [case 2]
            return true;
        } else {
            // [case 3]
            // make sure that all components are of READABLE_UNWRITABLE state.
            if (!areComponentsReadableWritableState(immutableComponents)) {
                throw new IllegalStateException();
            }
            // schedule a merge operation
            boolean isMergeTriggered = scheduleMerge(index);
            if (!isMergeTriggered) {
                throw new IllegalStateException();
            }
            return true;
        }
    }

    /**
     * This method returns whether there is an ongoing merge operation or not by checking
     * each component state of given components.
     *
     * @param immutableComponents
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

    /**
     * This method returns the number of mergable components among the given list
     * of immutable components that are ordered from the latest component to order ones. A caller
     * need to make sure the order in the list.
     *
     * @param immutableComponents
     * @return the number of mergable component
     */
    private int getMergableImmutableComponentCount(List<ILSMDiskComponent> immutableComponents) {
        int count = 0;
        for (ILSMDiskComponent c : immutableComponents) {
            long componentSize = c.getComponentSize();
            //stop when the first non-mergable component is found.
            if (c.getState() != ComponentState.READABLE_UNWRITABLE || componentSize > maxMergableComponentSize) {
                break;
            }
            ++count;
        }
        return count;
    }

    /**
     * checks whether all given components are of READABLE_UNWRITABLE state
     *
     * @param immutableComponents
     * @return true if all components are of READABLE_UNWRITABLE state, false otherwise.
     */
    private boolean areComponentsReadableWritableState(List<ILSMDiskComponent> immutableComponents) {
        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ComponentState.READABLE_UNWRITABLE) {
                return false;
            }
        }
        return true;
    }

    /**
     * schedule a merge operation according to this prefix merge policy
     *
     * @param index
     * @return true if merge is scheduled, false otherwise.
     * @throws HyracksDataException
     * @throws IndexException
     */
    private boolean scheduleMerge(final ILSMIndex index) throws HyracksDataException {
        // 1.  Look at the candidate components for merging in oldest-first order.  If one exists, identify the prefix of the sequence of
        // all such components for which the sum of their sizes exceeds MaxMrgCompSz.  Schedule a merge of those components into a new component.
        // 2.  If a merge from 1 doesn't happen, see if the set of candidate components for merging exceeds MaxTolCompCnt.  If so, schedule
        // a merge all of the current candidates into a new single component.
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        // Reverse the components order so that we look at components from oldest to newest.
        Collections.reverse(immutableComponents);
        boolean merged = false;
        long totalSize = 0;
        int startIndex = -1;
        for (int i = 0; i < immutableComponents.size(); i++) {
            ILSMDiskComponent c = immutableComponents.get(i);
            long componentSize = c.getComponentSize();
            if (componentSize > maxMergableComponentSize) {
                startIndex = i;
                totalSize = 0;
                continue;
            }
            totalSize += componentSize;
            boolean isLastComponent = i + 1 == immutableComponents.size() ? true : false;
            if (totalSize > maxMergableComponentSize
                    || (isLastComponent && i - startIndex >= maxToleranceComponentCount)) {
                List<ILSMDiskComponent> mergableComponents = new ArrayList<ILSMDiskComponent>();
                for (int j = startIndex + 1; j <= i; j++) {
                    mergableComponents.add(immutableComponents.get(j));
                }
                // Reverse the components order back to its original order
                Collections.reverse(mergableComponents);
                ILSMIndexAccessor accessor = index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                accessor.scheduleMerge(mergableComponents);
                long mergeSize = getMergeSize(mergableComponents);
                logDiskComponentsSnapshot(immutableComponents);
                logMergeInfo(mergeSize, false, mergableComponents.size(), immutableComponents.size());
                numMerges++;
                mergeCost = mergeCost + ((double) mergeSize) / (1024 * 1024 * 1024);
                merged = true;
                return true;
            }

        }
        if (!merged) {
            if (immutableComponents.size() >= maxTotalNumberComponents) {
                logDiskComponentsSnapshot(immutableComponents);
                Collections.reverse(immutableComponents);
                ILSMIndexAccessor accessor =
                        (ILSMIndexAccessor) index.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                accessor.scheduleMerge(immutableComponents);
                long mergeSize = getMergeSize(immutableComponents);
                logMergeInfo(mergeSize, false, immutableComponents.size(), immutableComponents.size());
                numMerges++;
                mergeCost = mergeCost + ((double) mergeSize) / (1024 * 1024 * 1024);
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
