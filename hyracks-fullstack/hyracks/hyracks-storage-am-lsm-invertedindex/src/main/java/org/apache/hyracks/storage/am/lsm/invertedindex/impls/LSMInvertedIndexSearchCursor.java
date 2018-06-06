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
package org.apache.hyracks.storage.am.lsm.invertedindex.impls;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ILSMIndexCursor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

/**
 * Searches the components one-by-one, completely consuming a cursor before moving on to the next one.
 * Therefore, the are no guarantees about sort order of the results.
 */
public class LSMInvertedIndexSearchCursor extends EnforcedIndexCursor implements ILSMIndexCursor {

    private IIndexAccessor currentAccessor;
    private IIndexCursor currentCursor;
    private int accessorIndex = -1;
    private boolean tupleConsumed = true;
    private ILSMHarness harness;
    private List<IIndexAccessor> indexAccessors;
    private ISearchPredicate searchPred;
    private ISearchOperationCallback searchCallback;

    // Assuming the cursor for all deleted-keys indexes are of the same type.
    private IIndexCursor[] deletedKeysBTreeCursors;
    private BloomFilter[] deletedKeysBTreeBloomFilters;
    private List<IIndexAccessor> deletedKeysBTreeAccessors;
    private RangePredicate keySearchPred;
    private ILSMIndexOperationContext opCtx;
    private boolean includeMemoryComponents;

    private List<ILSMComponent> operationalComponents;
    private ITupleReference currentTuple = null;
    private boolean resultOfSearchCallBackProceed = false;

    private final long[] hashes = BloomFilter.createHashArray();

    @Override
    public void doOpen(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        LSMInvertedIndexSearchCursorInitialState lsmInitState = (LSMInvertedIndexSearchCursorInitialState) initialState;
        harness = lsmInitState.getLSMHarness();
        operationalComponents = lsmInitState.getOperationalComponents();
        indexAccessors = lsmInitState.getIndexAccessors();
        opCtx = lsmInitState.getOpContext();
        accessorIndex = 0;
        this.searchPred = searchPred;
        this.searchCallback = lsmInitState.getSearchOperationCallback();
        includeMemoryComponents = false;
        // For searching the deleted-keys BTrees.
        deletedKeysBTreeAccessors = lsmInitState.getDeletedKeysBTreeAccessors();
        deletedKeysBTreeCursors = new IIndexCursor[deletedKeysBTreeAccessors.size()];
        deletedKeysBTreeBloomFilters = new BloomFilter[deletedKeysBTreeAccessors.size()];
        for (int i = 0; i < operationalComponents.size(); i++) {
            ILSMComponent component = operationalComponents.get(i);
            deletedKeysBTreeCursors[i] = deletedKeysBTreeAccessors.get(i).createSearchCursor(false);
            if (component.getType() == LSMComponentType.MEMORY) {
                // No need for a bloom filter for the in-memory BTree.
                deletedKeysBTreeBloomFilters[i] = null;
                includeMemoryComponents = true;
            } else {
                deletedKeysBTreeBloomFilters[i] = ((LSMInvertedIndexDiskComponent) component).getBloomFilter();
            }
        }

        MultiComparator keyCmp = lsmInitState.getKeyComparator();
        keySearchPred = new RangePredicate(null, null, true, true, keyCmp, keyCmp);
    }

    protected boolean isDeleted(ITupleReference key) throws HyracksDataException {
        keySearchPred.setLowKey(key, true);
        keySearchPred.setHighKey(key, true);
        for (int i = 0; i < accessorIndex; i++) {
            deletedKeysBTreeCursors[i].close();
            if (deletedKeysBTreeBloomFilters[i] != null && !deletedKeysBTreeBloomFilters[i].contains(key, hashes)) {
                continue;
            }
            try {
                deletedKeysBTreeAccessors.get(i).search(deletedKeysBTreeCursors[i], keySearchPred);
                if (deletedKeysBTreeCursors[i].hasNext()) {
                    return true;
                }
            } finally {
                deletedKeysBTreeCursors[i].close();
            }
        }
        return false;
    }

    // Move to the next tuple that has not been deleted.
    private boolean nextValidTuple() throws HyracksDataException {
        while (currentCursor.hasNext()) {
            currentCursor.next();
            currentTuple = currentCursor.getTuple();
            resultOfSearchCallBackProceed =
                    includeMemoryComponents && accessorIndex == 0 ? searchCallback.proceed(currentTuple) : true;

            if (!resultOfSearchCallBackProceed) {
                // We assume that the underlying cursors materialize their results such that
                // there is no need to reposition the result cursor after reconciliation.
                searchCallback.reconcile(currentTuple);
            }

            if (!isDeleted(currentTuple)) {
                tupleConsumed = false;
                return true;
            } else if (!resultOfSearchCallBackProceed) {
                // reconcile & tuple deleted case: needs to cancel the effect of reconcile().
                searchCallback.cancel(currentTuple);
            }
        }
        return false;
    }

    @Override
    public boolean doHasNext() throws HyracksDataException {
        if (!tupleConsumed) {
            return true;
        }
        if (currentCursor != null) {
            if (nextValidTuple()) {
                return true;
            }
            currentCursor.close();
            accessorIndex++;
        }
        while (accessorIndex < indexAccessors.size()) {
            // Current cursor has been exhausted, switch to next accessor/cursor.
            currentAccessor = indexAccessors.get(accessorIndex);
            currentCursor = currentAccessor.createSearchCursor(false);
            currentAccessor.search(currentCursor, searchPred);
            if (nextValidTuple()) {
                return true;
            }
            // Close as we go to release resources.
            currentCursor.close();
            accessorIndex++;
        }
        return false;
    }

    @Override
    public void doNext() throws HyracksDataException {
        // Mark the tuple as consumed, so hasNext() can move on.
        tupleConsumed = true;
    }

    @Override
    public void doDestroy() throws HyracksDataException {
        doClose();
    }

    @Override
    public void doClose() throws HyracksDataException {
        try {
            if (currentCursor != null) {
                currentCursor.close();
                currentCursor = null;
            }
            accessorIndex = 0;
        } finally {
            if (harness != null) {
                harness.endSearch(opCtx);
            }
        }
    }

    @Override
    public ITupleReference doGetTuple() {
        return currentCursor.getTuple();
    }

    @Override
    public ITupleReference getFilterMinTuple() {
        ILSMComponentFilter filter = getComponentFilter();
        return filter == null ? null : filter.getMinTuple();
    }

    @Override
    public ITupleReference getFilterMaxTuple() {
        ILSMComponentFilter filter = getComponentFilter();
        return filter == null ? null : filter.getMaxTuple();
    }

    private ILSMComponentFilter getComponentFilter() {
        if (accessorIndex < 0) {
            return null;
        }
        return operationalComponents.get(accessorIndex).getLSMComponentFilter();
    }

    @Override
    public boolean getSearchOperationCallbackProceedResult() {
        return false;
    }

}
