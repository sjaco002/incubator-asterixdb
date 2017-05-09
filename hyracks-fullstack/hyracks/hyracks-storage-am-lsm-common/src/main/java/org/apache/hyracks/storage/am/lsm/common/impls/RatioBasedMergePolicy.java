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
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class RatioBasedMergePolicy implements ILSMMergePolicy {

    private double lambda;
    private int min;
    private int max;
    private long numFlushes = 0;
    private long numMerges = 0;
    private double mergeCost = 0.0;
    private static final Logger LOGGER = Logger.getLogger(RatioBasedMergePolicy.class.getName());

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested, boolean isMergeOp)
            throws HyracksDataException {
        if (!isMergeOp) {
            numFlushes++;
        }
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getImmutableComponents());
        if (!areComponentsReadableWritableState(immutableComponents)) {
            return;
        }
        if (fullMergeIsRequested) {
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleFullMerge(index.getIOOperationCallback());
            long mergeSize = getMergeSize(immutableComponents);
            logDiskComponentsSnapshot(immutableComponents, false);
            logMergeInfo(mergeSize, true, immutableComponents.size(), immutableComponents.size());
            numMerges++;
            mergeCost = mergeCost + ((double) mergeSize) / (1024 * 1024 * 1024);
            return;
        }
        scheduleMerge(index);

    }

    private boolean scheduleMerge(final ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getImmutableComponents());
        Collections.reverse(immutableComponents);
        int size = immutableComponents.size();
        if (size <= min - 1) {
            return false;
        }
        long sum = getTotalSize(immutableComponents);
        int endIndex = size - min;
        int mergedIndex = -1;

        for (int i = 0; i <= endIndex; i++) {
            if (immutableComponents.get(i)
                    .getComponentSize() <= (lambda * (sum - immutableComponents.get(i).getComponentSize()))) {
                mergedIndex = i;
                break;
            }
            sum = sum - immutableComponents.get(i).getComponentSize();
        }
        if (mergedIndex != -1 || size > max) {
            int startIndex = size - min;
            boolean special = true;
            if (mergedIndex != -1) {
                startIndex = mergedIndex;
                special = false;
            }

            long mergeSize = 0;
            List<ILSMDiskComponent> mergableComponents = new ArrayList<ILSMDiskComponent>();
            for (int i = startIndex; i < immutableComponents.size(); i++) {
                mergeSize = mergeSize + immutableComponents.get(i).getComponentSize();
                mergableComponents.add(immutableComponents.get(i));
            }
            Collections.reverse(mergableComponents);
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleMerge(index.getIOOperationCallback(), mergableComponents);
            logDiskComponentsSnapshot(immutableComponents, special);
            logMergeInfo(mergeSize, false, mergableComponents.size(), immutableComponents.size());
            numMerges++;
            mergeCost = mergeCost + ((double) mergeSize) / (1024 * 1024 * 1024);
            return true;
        }
        return false;

    }

    private boolean areComponentsReadableWritableState(List<ILSMDiskComponent> immutableComponents) {
        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ComponentState.READABLE_UNWRITABLE) {
                return false;
            }
        }
        return true;
    }

    private boolean isMergeOngoing(List<ILSMDiskComponent> immutableComponents) {
        int size = immutableComponents.size();
        for (int i = 0; i < size; i++) {
            if (immutableComponents.get(i).getState() == ComponentState.READABLE_MERGING) {
                return true;
            }
        }
        return false;
    }

    private long getTotalSize(List<ILSMDiskComponent> immutableComponents) {
        long sum = 0;
        for (int i = 0; i < immutableComponents.size(); i++) {
            sum = sum + immutableComponents.get(i).getComponentSize();
        }
        return sum;
    }

    @Override
    public void configure(Map<String, String> properties) {
        lambda = Double.parseDouble(properties.get("lambda"));
        min = Integer.parseInt(properties.get("min"));
        max = Integer.parseInt(properties.get("max"));
    }

    private void logMergeInfo(long size, boolean isFullMerge, int mergedComponents, int totalComponents) {
        if (LOGGER.isLoggable(Level.SEVERE)) {
            if (isFullMerge) {
                LOGGER.severe("Full Merged: " + size + ", " + mergedComponents + ", " + totalComponents);
            } else {
                LOGGER.severe("Merged: " + size + ", " + mergedComponents + ", " + totalComponents);
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

    private void logDiskComponentsSnapshot(List<ILSMDiskComponent> immutableComponents, boolean special) {

        if (LOGGER.isLoggable(Level.SEVERE)) {
            String snapshotStr = "";
            for (int j = 0; j < immutableComponents.size(); j++) {

                snapshotStr =
 snapshotStr + "," + immutableComponents.get(j).getComponentSize();
            }
            if (snapshotStr.length() > 1) {
                snapshotStr = snapshotStr.substring(1);
            }
            if (!special) {
                LOGGER.severe("Merge Snapshot: " + snapshotStr);
                return;
            }
            LOGGER.severe("Special Merge Snapshot: " + snapshotStr);
        }
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = index.getImmutableComponents();
        boolean isMergeOngoing = isMergeOngoing(immutableComponents);
        if (isMergeOngoing) {
            return true;
        }
        /*if (!areComponentsReadableWritableState(immutableComponents)) {
            throw new IllegalStateException();
        }
        boolean isMergeTriggered  = scheduleMerge(index);
        return isMergeTriggered;*/
        return false;

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
