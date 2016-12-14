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
import org.apache.hyracks.storage.am.common.api.IndexException;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.ComponentState;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;

public class ExploringMergePolicy implements ILSMMergePolicy {

    private double ratio;
    private int min;
    private int max;
    private long numFlushes = 0;
    private long numMerges = 0;
    private double mergeCost = 0.0;
    private static final Logger LOGGER = Logger.getLogger(ExploringMergePolicy.class.getName());

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested, boolean isMergeOp)
            throws HyracksDataException, IndexException {
        if (!isMergeOp) {
            numFlushes++;
        } else {
            return;
        }
        List<ILSMComponent> immutableComponents = new ArrayList<ILSMComponent>(index.getImmutableComponents());
        if (!areComponentsReadableWritableState(immutableComponents)) {
            return;
        }
        if (fullMergeIsRequested) {
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleFullMerge(index.getIOOperationCallback());
            long mergeSize = getMergeSize(immutableComponents);
            logDiskComponentsSnapshot(immutableComponents, false);
            logMergeInfo(mergeSize, true, immutableComponents.size(), immutableComponents.size(), 0,
                    immutableComponents.size() - 1);
            numMerges++;
            mergeCost = mergeCost + ((double) mergeSize) / (1024 * 1024 * 1024);
            return;
        }
        scheduleMerge(index);
    }

    private boolean scheduleMerge(final ILSMIndex index) throws HyracksDataException, IndexException {
        List<ILSMComponent> immutableComponents = new ArrayList<ILSMComponent>(index.getImmutableComponents());
        Collections.reverse(immutableComponents);
        int length = immutableComponents.size();
        if (length <= min - 1) {
            return false;
        }
        boolean mightBeStuck = (length > max) ? true : false;
        List<ILSMComponent> bestSelection = new ArrayList<ILSMComponent>(0);
        List<ILSMComponent> smallest = new ArrayList<ILSMComponent>(0);
        List<ILSMComponent> mergableComponents = new ArrayList<ILSMComponent>();
        long bestSize = 0;
        long smallestSize = Long.MAX_VALUE;
        int bestStart = -1;
        int bestEnd = -1;
        int smallestStart = -1;
        int smallestEnd = -1;
        boolean merging = false;
        int mergeStart = -1;
        int mergeEnd = -1;
        boolean special = false;

        for (int start = 0; start < length; start++) {
            for (int currentEnd = start + min - 1; currentEnd < length; currentEnd++) {
                List<ILSMComponent> potentialMatchFiles = immutableComponents.subList(start, currentEnd + 1);
                if (potentialMatchFiles.size() < min) {
                    continue;
                }
                long size = getTotalSize(potentialMatchFiles);
                if (mightBeStuck && size < smallestSize) {
                    smallest = potentialMatchFiles;
                    smallestSize = size;
                    smallestStart = start;
                    smallestEnd = currentEnd;
                }
                if (!fileInRatio(potentialMatchFiles)) {
                    continue;
                }
                if (isBetterSelection(bestSelection, bestSize, potentialMatchFiles, size, mightBeStuck)) {
                    bestSelection = potentialMatchFiles;
                    bestSize = size;
                    bestStart = start;
                    bestEnd = currentEnd;
                }
            }
        }
        if (bestSelection.size() == 0 && mightBeStuck && smallestStart != -1 && smallestEnd != -1) {
            merging = true;
            special = true;
            mergeStart = smallestStart;
            mergeEnd = smallestEnd;
            mergableComponents = new ArrayList<ILSMComponent>(smallest);
        } else if (bestStart != -1 && bestEnd != -1) {
            merging = true;
            mergeStart = bestStart;
            mergeEnd = bestEnd;
            mergableComponents = new ArrayList<ILSMComponent>(bestSelection);

        }
        if (merging) {
            Collections.reverse(mergableComponents);
            long mergeSize = getMergeSize(mergableComponents);
            ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                    NoOpOperationCallback.INSTANCE);
            accessor.scheduleMerge(index.getIOOperationCallback(), mergableComponents);
            logDiskComponentsSnapshot(immutableComponents, special);
            logMergeInfo(mergeSize, false, mergableComponents.size(), immutableComponents.size(), mergeStart, mergeEnd);
            numMerges++;
            mergeCost = mergeCost + ((double) mergeSize) / (1024 * 1024 * 1024);

            return true;
        }
        return false;

    }

    public boolean isBetterSelection(List<ILSMComponent> bestSelection, long bestSize, List<ILSMComponent> selection,
            long size, boolean mightBeStuck) {
        if (mightBeStuck && bestSize > 0 && size > 0) {
            double thresholdQuantity = ((double) bestSelection.size() / bestSize);
            return thresholdQuantity < ((double) selection.size() / size);
        }
        return selection.size() > bestSelection.size() || (selection.size() == bestSelection.size() && size < bestSize);
    }

    public boolean fileInRatio(final List<ILSMComponent> files) {
        if (files.size() < 2) {
            return true;
        }
        long totalFileSize = getTotalSize(files);
        for (ILSMComponent file : files) {
            long singleFileSize = ((AbstractDiskLSMComponent) file).getComponentSize();
            long sumAllOtherFileSizes = totalFileSize - singleFileSize;
            if (singleFileSize > sumAllOtherFileSizes * ratio) {
                return false;
            }
        }
        return true;
    }

    private boolean areComponentsReadableWritableState(List<ILSMComponent> immutableComponents) {
        for (ILSMComponent c : immutableComponents) {
            if (c.getState() != ComponentState.READABLE_UNWRITABLE) {
                return false;
            }
        }
        return true;
    }

    private boolean isMergeOngoing(List<ILSMComponent> immutableComponents) {
        int size = immutableComponents.size();
        for (int i = 0; i < size; i++) {
            if (immutableComponents.get(i).getState() == ComponentState.READABLE_MERGING) {
                return true;
            }
        }
        return false;
    }

    private long getTotalSize(List<ILSMComponent> files) {
        long sum = 0;
        for (int i = 0; i < files.size(); i++) {
            sum = sum + ((AbstractDiskLSMComponent) files.get(i)).getComponentSize();
        }
        return sum;
    }

    @Override
    public void configure(Map<String, String> properties) {
        ratio = Double.parseDouble(properties.get("lambda"));
        min = Integer.parseInt(properties.get("min"));
        max = Integer.parseInt(properties.get("max"));
    }

    private void logMergeInfo(long size, boolean isFullMerge, int mergedComponents, int totalComponents, int startIndex,
            int endIndex) {
        if (LOGGER.isLoggable(Level.SEVERE)) {
            if (isFullMerge) {
                LOGGER.severe("Full Merged: " + size + ", " + mergedComponents + ", " + totalComponents + ", "
                        + startIndex + ", " + endIndex);
            } else {
                LOGGER.severe("Merged: " + size + ", " + mergedComponents + ", " + totalComponents + ", " + startIndex
                        + ", " + endIndex);
            }
        }
    }

    private long getMergeSize(List<ILSMComponent> files) {
        long mergeSize = 0;
        for (int j = 0; j < files.size(); j++) {
            mergeSize = mergeSize + ((AbstractDiskLSMComponent) files.get(j)).getComponentSize();
        }
        return mergeSize;
    }

    private void logDiskComponentsSnapshot(List<ILSMComponent> immutableComponents, boolean special) {

        if (LOGGER.isLoggable(Level.SEVERE)) {
            String snapshotStr = "";
            for (int j = 0; j < immutableComponents.size(); j++) {

                snapshotStr =
                        snapshotStr + "," + ((AbstractDiskLSMComponent) immutableComponents.get(j)).getComponentSize();
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
    public boolean isMergeLagging(ILSMIndex index) throws HyracksDataException, IndexException {
        List<ILSMComponent> immutableComponents = index.getImmutableComponents();
        boolean isMergeOngoing = isMergeOngoing(immutableComponents);
        if (isMergeOngoing) {
            return true;
        }
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