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

public class KSlotMergePolicy implements ILSMMergePolicy {

    private int numComponents;
    private long[] mergeCosts;
    //private boolean mergedCall = false;
    private long numFlushes = 0;
    private long numMerges = 0;
    private double mergeCost = 0.0;
    private static final Logger LOGGER = Logger.getLogger(KSlotMergePolicy.class.getName());

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested, boolean isMergeOp)
            throws HyracksDataException {

        if (isMergeOp) {
            return;
        }
        numFlushes++;
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        if (!areComponentsReadableWritableState(immutableComponents)) {
            return;
        }
        if (fullMergeIsRequested) {
            updateMergeCosts(0, 0);
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleFullMerge(index.getIOOperationCallback());
            long mergeSize = getMergeSize(immutableComponents);
            logDiskComponentsSnapshot(immutableComponents);
            logMergeInfo(mergeSize, true, immutableComponents.size(), immutableComponents.size());
            numMerges++;
            mergeCost = mergeCost + ((double) mergeSize) / (1024 * 1024 * 1024);
            return;
        }
        scheduleMerge(index);

    }

    private boolean scheduleMerge(final ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<ILSMDiskComponent>(index.getDiskComponents());
        Collections.reverse(immutableComponents);
        int mergedIndex = -1;
        long delta = 0;
        int size = immutableComponents.size();
        if (size <= 1) {
            return false;
        }
        int startIndex = -1;
        if (size <= numComponents) {
            startIndex = size - 2;
            delta = immutableComponents.get(size - 1).getComponentSize();
        } else {
            startIndex = numComponents - 2;
            delta = immutableComponents.get(numComponents - 1).getComponentSize()
                    + immutableComponents.get(numComponents).getComponentSize();
        }

        for (int i = startIndex; i >= -1; i--) {
            if (i == -1) {
                mergedIndex = i + 1;
            } else if (immutableComponents.get(i).getComponentSize() >= mergeCosts[i]
                    + delta) {
                mergedIndex = i + 1;
                break;
            }
        }
        updateMergeCosts(mergedIndex, delta);
        if (mergedIndex != size - 1) {
            long mergeSize = 0;
            List<ILSMDiskComponent> mergableComponents = new ArrayList<>();
            for (int j = mergedIndex; j < immutableComponents.size(); j++) {
                mergeSize = mergeSize + immutableComponents.get(j).getComponentSize();
                mergableComponents.add(immutableComponents.get(j));
            }
            Collections.reverse(mergableComponents);
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleMerge(index.getIOOperationCallback(), mergableComponents);
            logDiskComponentsSnapshot(immutableComponents);
            logMergeInfo(mergeSize, false, mergableComponents.size(), immutableComponents.size());
            numMerges++;
            mergeCost = mergeCost + ((double) mergeSize) / (1024 * 1024 * 1024);
            return true;
        }
        return false;

    }

    private int needMerge(final ILSMIndex index) {
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        Collections.reverse(immutableComponents);
        int mergedIndex = -1;
        int size = immutableComponents.size();
        if (size <= 1) {
            return -1;
        }
        int startIndex = -1;
        if (size <= numComponents) {
            startIndex = size - 2;
        } else {
            startIndex = numComponents - 2;
        }
        for (int i = startIndex; i >= -1; i--) {
            if (i == -1) {
                mergedIndex = i + 1;
            } else if (immutableComponents.get(i).getComponentSize() >= mergeCosts[i]) {
                mergedIndex = i + 1;
                break;
            }
        }
        if (mergedIndex != size - 1) {
            return mergedIndex;
        }
        return -1;

    }

    private boolean scheduleLaggedMerge(final ILSMIndex index, int mergedIndex)
 throws HyracksDataException {
        long mergeSize = 0;
        List<ILSMDiskComponent> immutableComponents = new ArrayList<>(index.getDiskComponents());
        Collections.reverse(immutableComponents);
        List<ILSMDiskComponent> mergableComponents = new ArrayList<>();
        for (int j = mergedIndex; j < immutableComponents.size(); j++) {
            mergeSize = mergeSize + immutableComponents.get(j).getComponentSize();
            mergableComponents.add(immutableComponents.get(j));
        }
        Collections.reverse(mergableComponents);
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        accessor.scheduleMerge(index.getIOOperationCallback(), mergableComponents);
        logDiskComponentsSnapshot(immutableComponents);
        logMergeInfo(mergeSize, false, mergableComponents.size(), immutableComponents.size());
        numMerges++;
        mergeCost = mergeCost + ((double) mergeSize) / (1024 * 1024 * 1024);
        return true;

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

    @Override
    public void configure(Map<String, String> properties) {
        numComponents = Integer.parseInt(properties.get("num-components"));
        mergeCosts = new long[numComponents];

    }

    private void updateMergeCosts(int startIndex, long delta) {

        for (int i = startIndex; i < numComponents; i++) {
            mergeCosts[i] = 0;
        }
        for (int i = 0; i < startIndex; i++) {
            mergeCosts[i] = mergeCosts[i] + delta;
        }
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

    private void logDiskComponentsSnapshot(List<ILSMDiskComponent> immutableComponents) {

        if (LOGGER.isLoggable(Level.SEVERE)) {
            String snapshotStr = "";
            for (int j = 0; j < immutableComponents.size(); j++) {

                snapshotStr =
 snapshotStr + "," + immutableComponents.get(j).getComponentSize();
            }
            if (snapshotStr.length() > 1) {
                snapshotStr = snapshotStr.substring(1);
            }
            LOGGER.severe("Merge Snapshot: " + snapshotStr);
        }
    }

    @Override
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException {
        List<ILSMDiskComponent> immutableComponents = index.getDiskComponents();
        /*int mergedIndex = needMerge(index);
        if (mergedIndex==-1){
        return false;
        }*/
        boolean isMergeOngoing = isMergeOngoing(immutableComponents);
        if (isMergeOngoing) {
            return true;
        }
        /*if (!areComponentsReadableWritableState(immutableComponents)) {
            throw new IllegalStateException();
        }
        boolean isMergeTriggered  = scheduleLaggedMerge(index,mergedIndex);
        if (!isMergeTriggered) {
            throw new IllegalStateException();
        }
        return true;*/
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
