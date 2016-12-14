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

public class BinomialMergePolicy implements ILSMMergePolicy {

    private int numComponents;
    private long numFlushes = 0;
    private long numMerges = 0;
    private double mergeCost = 0.0;
    private static final Logger LOGGER = Logger.getLogger(BinomialMergePolicy.class.getName());

    @Override
    public void diskComponentAdded(final ILSMIndex index, boolean fullMergeIsRequested, boolean isMergeOp)
            throws HyracksDataException, IndexException {

        if (isMergeOp) {
            return;
        }
        numFlushes++;
        List<ILSMComponent> immutableComponents = new ArrayList<ILSMComponent>(index.getImmutableComponents());
        if (!areComponentsReadableWritableState(immutableComponents)) {
            return;
        }
        if (fullMergeIsRequested) {
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

    private boolean scheduleMerge(final ILSMIndex index) throws HyracksDataException, IndexException {
        List<ILSMComponent> immutableComponents = new ArrayList<ILSMComponent>(index.getImmutableComponents());
        Collections.reverse(immutableComponents);
        int size = immutableComponents.size();
        int depth = 0;
        while ((tree(depth) < numFlushes)) {
            depth++;
        }
        int mergedIndex =
                binomial_index(depth, Math.min(depth, numComponents) - 1, (int) (numFlushes - tree(depth - 1) - 1));
        LOGGER.severe("Binomial Index: " + numFlushes + "," + (mergedIndex + 1) + "," + size);
        if (mergedIndex == size - 1) {
            return false;
        }
        LOGGER.severe("Binomial Compaction: " + numFlushes + "," + (mergedIndex + 1) + "," + size);
        long mergeSize = 0;
        List<ILSMComponent> mergableComponents = new ArrayList<ILSMComponent>();
        for (int i = mergedIndex; i < immutableComponents.size(); i++) {
            mergeSize = mergeSize + ((AbstractDiskLSMComponent) immutableComponents.get(i)).getComponentSize();
            mergableComponents.add(immutableComponents.get(i));
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

    private int binomial_index(int d, int h, int t) {
        if (t < 0 || t > binomial_choose(d + h, h)) {
            LOGGER.severe("Binomial Exception");

        }
        if (t == 0) {
            return 0;
        } else if (t < binomial_choose(d + h - 1, h)) {
            return binomial_index(d - 1, h, t);
        }
        return binomial_index(d, h - 1, t - binomial_choose(d + h - 1, h)) + 1;

    }

    private int tree(int d) {
        if (d < 0) {
            return 0;
        }
        return tree(d - 1) + binomial_choose(d + Math.min(d, numComponents) - 1, d);
    }

    private int binomial_choose(int n, int k) {
        if (k < 0 || k > n) {
            return 0;
        }
        if (k == 0 || k == n) {
            return 1;
        } else {
            int bin[][] = new int[n + 1][n + 1];

            for (int r = 0; r <= n; r++) {
                for (int c = 0; c <= r && c <= k; c++) {
                    if (c == 0 || c == r) {
                        bin[r][c] = 1;
                    } else {
                        bin[r][c] = bin[r - 1][c - 1] + bin[r - 1][c];
                    }
                }
            }
            return bin[n][k];
        }
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

    @Override
    public void configure(Map<String, String> properties) {
        numComponents = Integer.parseInt(properties.get("num-components"));
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

    private long getMergeSize(List<ILSMComponent> immutableComponents) {
        long mergeSize = 0;
        for (int j = 0; j < immutableComponents.size(); j++) {
            mergeSize = mergeSize + ((AbstractDiskLSMComponent) immutableComponents.get(j)).getComponentSize();
        }
        return mergeSize;
    }

    private void logDiskComponentsSnapshot(List<ILSMComponent> immutableComponents) {

        if (LOGGER.isLoggable(Level.SEVERE)) {
            String snapshotStr = "";
            for (int j = 0; j < immutableComponents.size(); j++) {

                snapshotStr =
                        snapshotStr + "," + ((AbstractDiskLSMComponent) immutableComponents.get(j)).getComponentSize();
            }
            if (snapshotStr.length() > 1) {
                snapshotStr = snapshotStr.substring(1);
            }
            LOGGER.severe("Merge Snapshot: " + snapshotStr);
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