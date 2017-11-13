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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.btree.impls.BTree.BTreeAccessor;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.tuples.PermutingTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentFilterHelper;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentBulkLoader;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.freepage.VirtualFreePageManager;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilterManager;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMIndexSearchCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndex;
import org.apache.hyracks.storage.am.lsm.invertedindex.inmemory.InMemoryInvertedIndexAccessor;
import org.apache.hyracks.storage.am.lsm.invertedindex.search.InvertedIndexSearchPredicate;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizerFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexUtils;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.util.trace.ITracer;

public class LSMInvertedIndex extends AbstractLSMIndex implements IInvertedIndex {
    private static final Logger LOGGER = Logger.getLogger(LSMInvertedIndex.class.getName());

    protected final IBinaryTokenizerFactory tokenizerFactory;

    // Type traits and comparators for tokens and inverted-list elements.
    protected final ITypeTraits[] invListTypeTraits;
    protected final IBinaryComparatorFactory[] invListCmpFactories;
    protected final ITypeTraits[] tokenTypeTraits;
    protected final IBinaryComparatorFactory[] tokenCmpFactories;
    private final int[] filterFieldsForNonBulkLoadOps;
    private final int[] invertedIndexFieldsForNonBulkLoadOps;

    public LSMInvertedIndex(IIOManager ioManager, List<IVirtualBufferCache> virtualBufferCaches,
            ILSMDiskComponentFactory componentFactory, IComponentFilterHelper filterHelper,
            ILSMComponentFilterFrameFactory filterFrameFactory, LSMComponentFilterManager filterManager,
            double bloomFilterFalsePositiveRate, IBufferCache diskBufferCache, ILSMIndexFileManager fileManager,
            ITypeTraits[] invListTypeTraits, IBinaryComparatorFactory[] invListCmpFactories,
            ITypeTraits[] tokenTypeTraits, IBinaryComparatorFactory[] tokenCmpFactories,
            IBinaryTokenizerFactory tokenizerFactory, ILSMMergePolicy mergePolicy, ILSMOperationTracker opTracker,
            ILSMIOOperationScheduler ioScheduler, ILSMIOOperationCallbackFactory ioOpCallbackFactory,
            int[] invertedIndexFields, int[] filterFields, int[] filterFieldsForNonBulkLoadOps,
            int[] invertedIndexFieldsForNonBulkLoadOps, boolean durable) throws HyracksDataException {
        super(ioManager, virtualBufferCaches, diskBufferCache, fileManager, bloomFilterFalsePositiveRate, mergePolicy,
                opTracker, ioScheduler, ioOpCallbackFactory, componentFactory, componentFactory, filterFrameFactory,
                filterManager, filterFields, durable, filterHelper, invertedIndexFields, ITracer.NONE);
        this.tokenizerFactory = tokenizerFactory;
        this.invListTypeTraits = invListTypeTraits;
        this.invListCmpFactories = invListCmpFactories;
        this.tokenTypeTraits = tokenTypeTraits;
        this.tokenCmpFactories = tokenCmpFactories;
        this.filterFieldsForNonBulkLoadOps = filterFieldsForNonBulkLoadOps;
        this.invertedIndexFieldsForNonBulkLoadOps = invertedIndexFieldsForNonBulkLoadOps;

        int i = 0;
        for (IVirtualBufferCache virtualBufferCache : virtualBufferCaches) {
            InMemoryInvertedIndex memInvIndex =
                    createInMemoryInvertedIndex(virtualBufferCache, new VirtualFreePageManager(virtualBufferCache), i);
            BTree deleteKeysBTree =
                    BTreeUtils.createBTree(virtualBufferCache, new VirtualFreePageManager(virtualBufferCache),
                            invListTypeTraits, invListCmpFactories, BTreeLeafFrameType.REGULAR_NSM,
                            ioManager.resolveAbsolutePath(fileManager.getBaseDir() + "_virtual_del_" + i), false);
            LSMInvertedIndexMemoryComponent mutableComponent =
                    new LSMInvertedIndexMemoryComponent(this, memInvIndex, deleteKeysBTree, virtualBufferCache,
                            i == 0 ? true : false, filterHelper == null ? null : filterHelper.createFilter());
            memoryComponents.add(mutableComponent);
            ++i;
        }
    }

    /**
     * The keys in the in-memory deleted-keys BTree only refer to on-disk components.
     * We delete documents from the in-memory inverted index by deleting its entries directly,
     * while still adding the deleted key to the deleted-keys BTree.
     * Otherwise, inserts would have to remove keys from the in-memory deleted-keys BTree which
     * may cause incorrect behavior (lost deletes) in the following pathological case:
     * Insert doc 1, flush, delete doc 1, insert doc 1
     * After the sequence above doc 1 will now appear twice because the delete of the on-disk doc 1 has been lost.
     * Insert:
     * - Insert document into in-memory inverted index.
     * Delete:
     * - Delete document from in-memory inverted index (ignore if it does not exist).
     * - Insert key into deleted-keys BTree.
     */
    @Override
    public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {
        LSMInvertedIndexOpContext ctx = (LSMInvertedIndexOpContext) ictx;
        // TODO: This is a hack to support logging properly in ASTERIX.
        // The proper undo operations are only dependent on the after image so
        // it is correct to say we found nothing (null) as the before image (at least
        // in the perspective of ASTERIX). The semantics for the operation callbacks
        // are violated here (and they are somewhat unclear in the first place as to
        // what they should be for an inverted index).

        ITupleReference indexTuple;
        if (ctx.getIndexTuple() != null) {
            ctx.getIndexTuple().reset(tuple);
            indexTuple = ctx.getIndexTuple();
            ((InMemoryInvertedIndexAccessor) (ctx.getCurrentMutableInvIndexAccessors())).resetLogTuple(tuple);
        } else {
            indexTuple = tuple;
        }

        ctx.getModificationCallback().before(tuple);
        ctx.getModificationCallback().found(null, tuple);
        switch (ctx.getOperation()) {
            case INSERT:
                // Insert into the in-memory inverted index.
                ctx.getCurrentMutableInvIndexAccessors().insert(indexTuple);
                break;
            case DELETE:
                // First remove all entries in the in-memory inverted index (if any).
                ctx.getCurrentMutableInvIndexAccessors().delete(indexTuple);
                // Insert key into the deleted-keys BTree.
                ctx.getKeysOnlyTuple().reset(indexTuple);
                try {
                    ctx.getCurrentDeletedKeysBTreeAccessors().insert(ctx.getKeysOnlyTuple());
                } catch (HyracksDataException e) {
                    if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                        // Key has already been deleted.
                        LOGGER.log(Level.WARNING, "Failure during index delete operation", e);
                        throw e;
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException("Operation " + ctx.getOperation() + " not supported.");
        }
        updateFilter(ctx, tuple);
    }

    @Override
    public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
            throws HyracksDataException {
        List<ILSMComponent> operationalComponents = ictx.getComponentHolder();
        int numComponents = operationalComponents.size();
        boolean includeMutableComponent = false;
        ArrayList<IIndexAccessor> indexAccessors = new ArrayList<>(numComponents);
        ArrayList<IIndexAccessor> deletedKeysBTreeAccessors = new ArrayList<>(numComponents);

        for (int i = 0; i < operationalComponents.size(); i++) {
            ILSMComponent component = operationalComponents.get(i);
            if (component.getType() == LSMComponentType.MEMORY) {
                includeMutableComponent = true;
                IIndexAccessor invIndexAccessor =
                        component.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
                indexAccessors.add(invIndexAccessor);
                IIndexAccessor deletedKeysAccessor = ((LSMInvertedIndexMemoryComponent) component).getBuddyIndex()
                        .createAccessor(NoOpIndexAccessParameters.INSTANCE);
                deletedKeysBTreeAccessors.add(deletedKeysAccessor);
            } else {
                IIndexAccessor invIndexAccessor =
                        component.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
                indexAccessors.add(invIndexAccessor);
                IIndexAccessor deletedKeysAccessor = ((LSMInvertedIndexDiskComponent) component).getBuddyIndex()
                        .createAccessor(NoOpIndexAccessParameters.INSTANCE);
                deletedKeysBTreeAccessors.add(deletedKeysAccessor);
            }
        }

        ICursorInitialState initState = createCursorInitialState(pred, ictx, includeMutableComponent, indexAccessors,
                deletedKeysBTreeAccessors, operationalComponents);
        cursor.open(initState, pred);
    }

    private ICursorInitialState createCursorInitialState(ISearchPredicate pred, IIndexOperationContext ictx,
            boolean includeMutableComponent, ArrayList<IIndexAccessor> indexAccessors,
            ArrayList<IIndexAccessor> deletedKeysBTreeAccessors, List<ILSMComponent> operationalComponents) {
        ICursorInitialState initState;
        PermutingTupleReference keysOnlyTuple = createKeysOnlyTupleReference();
        MultiComparator keyCmp = MultiComparator.create(invListCmpFactories);

        // TODO: This check is not pretty, but it does the job. Come up with something more OO in the future.
        // Distinguish between regular searches and range searches (mostly used in merges).
        if (pred instanceof InvertedIndexSearchPredicate) {
            initState = new LSMInvertedIndexSearchCursorInitialState(keyCmp, keysOnlyTuple, indexAccessors,
                    deletedKeysBTreeAccessors,
                    ((LSMInvertedIndexMemoryComponent) memoryComponents.get(currentMutableComponentId.get()))
                            .getBuddyIndex().getLeafFrameFactory(),
                    ictx, includeMutableComponent, getLsmHarness(), operationalComponents);
        } else {
            LSMInvertedIndexMemoryComponent mutableComponent =
                    (LSMInvertedIndexMemoryComponent) memoryComponents.get(currentMutableComponentId.get());
            MultiComparator tokensAndKeysCmp =
                    MultiComparator.create(mutableComponent.getIndex().getBTree().getComparatorFactories());
            initState = new LSMInvertedIndexRangeSearchCursorInitialState(tokensAndKeysCmp, keyCmp, keysOnlyTuple,
                    ((LSMInvertedIndexMemoryComponent) memoryComponents.get(currentMutableComponentId.get()))
                            .getBuddyIndex().getLeafFrameFactory(),
                    includeMutableComponent, getLsmHarness(), indexAccessors, deletedKeysBTreeAccessors, pred,
                    operationalComponents);
        }
        return initState;
    }

    /**
     * Returns a permuting tuple reference that projects away the document field(s) of a tuple, only leaving the key
     * fields.
     */
    private PermutingTupleReference createKeysOnlyTupleReference() {
        // Project away token fields.
        int[] keyFieldPermutation = new int[invListTypeTraits.length];
        int numTokenFields = tokenTypeTraits.length;
        for (int i = 0; i < invListTypeTraits.length; i++) {
            keyFieldPermutation[i] = numTokenFields + i;
        }
        return new PermutingTupleReference(keyFieldPermutation);
    }

    @Override
    public ILSMDiskComponent doFlush(ILSMIOOperation operation) throws HyracksDataException {
        LSMInvertedIndexFlushOperation flushOp = (LSMInvertedIndexFlushOperation) operation;

        // Create an inverted index instance to be bulk loaded.
        ILSMDiskComponent component = createDiskComponent(componentFactory, flushOp.getTarget(),
                flushOp.getDeletedKeysBTreeTarget(), flushOp.getBloomFilterTarget(), true);

        // Create a scan cursor on the BTree underlying the in-memory inverted index.
        LSMInvertedIndexMemoryComponent flushingComponent =
                (LSMInvertedIndexMemoryComponent) flushOp.getFlushingComponent();

        RangePredicate nullPred = new RangePredicate(null, null, true, true, null, null);

        // Search the deleted keys BTree to calculate the number of elements for BloomFilter
        BTreeAccessor deletedKeysBTreeAccessor =
                flushingComponent.getBuddyIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        IIndexCursor btreeCountingCursor = deletedKeysBTreeAccessor.createCountingSearchCursor();
        deletedKeysBTreeAccessor.search(btreeCountingCursor, nullPred);
        long numBTreeTuples = 0L;
        try {
            while (btreeCountingCursor.hasNext()) {
                btreeCountingCursor.next();
                ITupleReference countTuple = btreeCountingCursor.getTuple();
                numBTreeTuples = IntegerPointable.getInteger(countTuple.getFieldData(0), countTuple.getFieldStart(0));
            }
        } finally {
            btreeCountingCursor.close();
        }

        ILSMDiskComponentBulkLoader componentBulkLoader =
                component.createBulkLoader(1.0f, false, numBTreeTuples, false, false, false);

        // Create a scan cursor on the deleted keys BTree underlying the in-memory inverted index.
        IIndexCursor deletedKeysScanCursor = deletedKeysBTreeAccessor.createSearchCursor(false);
        deletedKeysBTreeAccessor.search(deletedKeysScanCursor, nullPred);

        try {
            while (deletedKeysScanCursor.hasNext()) {
                deletedKeysScanCursor.next();
                componentBulkLoader.delete(deletedKeysScanCursor.getTuple());
            }
        } finally {
            deletedKeysScanCursor.close();
        }

        // Scan the in-memory inverted index
        InMemoryInvertedIndexAccessor memInvIndexAccessor =
                flushingComponent.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        BTreeAccessor memBTreeAccessor = memInvIndexAccessor.getBTreeAccessor();
        IIndexCursor scanCursor = memBTreeAccessor.createSearchCursor(false);
        memBTreeAccessor.search(scanCursor, nullPred);

        // Bulk load the disk inverted index from the in-memory inverted index.
        try {
            while (scanCursor.hasNext()) {
                scanCursor.next();
                componentBulkLoader.add(scanCursor.getTuple());
            }
        } finally {
            scanCursor.close();
        }
        if (component.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<>();
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMinTuple());
            filterTuples.add(flushingComponent.getLSMComponentFilter().getMaxTuple());
            filterManager.updateFilter(component.getLSMComponentFilter(), filterTuples);
            filterManager.writeFilter(component.getLSMComponentFilter(), component.getMetadataHolder());
        }
        flushingComponent.getMetadata().copy(component.getMetadata());

        componentBulkLoader.end();

        return component;
    }

    @Override
    public ILSMDiskComponent doMerge(ILSMIOOperation operation) throws HyracksDataException {
        LSMInvertedIndexMergeOperation mergeOp = (LSMInvertedIndexMergeOperation) operation;
        IIndexCursor cursor = mergeOp.getCursor();

        RangePredicate mergePred = new RangePredicate(null, null, true, true, null, null);
        ILSMIndexOperationContext opCtx = ((LSMIndexSearchCursor) cursor).getOpCtx();
        // Scan diskInvertedIndexes ignoring the memoryInvertedIndex.
        search(opCtx, cursor, mergePred);

        // Create an inverted index instance.
        ILSMDiskComponent component = createDiskComponent(componentFactory, mergeOp.getTarget(),
                mergeOp.getDeletedKeysBTreeTarget(), mergeOp.getBloomFilterTarget(), true);

        ILSMDiskComponentBulkLoader componentBulkLoader;

        // In case we must keep the deleted-keys BTrees, then they must be merged *before* merging the inverted indexes so that
        // lsmHarness.endSearch() is called once when the inverted indexes have been merged.
        if (mergeOp.getMergingComponents().get(mergeOp.getMergingComponents().size() - 1) != diskComponents
                .get(diskComponents.size() - 1)) {
            // Keep the deleted tuples since the oldest disk component is not included in the merge operation

            LSMInvertedIndexDeletedKeysBTreeMergeCursor btreeCursor =
                    new LSMInvertedIndexDeletedKeysBTreeMergeCursor(opCtx);
            search(opCtx, btreeCursor, mergePred);

            long numElements = 0L;
            for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                numElements += ((LSMInvertedIndexDiskComponent) mergeOp.getMergingComponents().get(i)).getBloomFilter()
                        .getNumElements();
            }

            componentBulkLoader = component.createBulkLoader(1.0f, false, numElements, false, false, false);
            try {
                while (btreeCursor.hasNext()) {
                    btreeCursor.next();
                    ITupleReference tuple = btreeCursor.getTuple();
                    componentBulkLoader.delete(tuple);
                }
            } finally {
                btreeCursor.close();
            }
        } else {
            componentBulkLoader = component.createBulkLoader(1.0f, false, 0L, false, false, false);
        }

        try {
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();
                componentBulkLoader.add(tuple);
            }
        } finally {
            cursor.close();
        }
        if (component.getLSMComponentFilter() != null) {
            List<ITupleReference> filterTuples = new ArrayList<>();
            for (int i = 0; i < mergeOp.getMergingComponents().size(); ++i) {
                ITupleReference min = mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMinTuple();
                ITupleReference max = mergeOp.getMergingComponents().get(i).getLSMComponentFilter().getMaxTuple();
                if (min != null) {
                    filterTuples.add(min);
                }
                if (max != null) {
                    filterTuples.add(max);
                }
            }
            getFilterManager().updateFilter(component.getLSMComponentFilter(), filterTuples);
            getFilterManager().writeFilter(component.getLSMComponentFilter(), component.getMetadataHolder());
        }

        componentBulkLoader.end();

        return component;
    }

    protected InMemoryInvertedIndex createInMemoryInvertedIndex(IVirtualBufferCache virtualBufferCache,
            VirtualFreePageManager virtualFreePageManager, int id) throws HyracksDataException {
        return InvertedIndexUtils.createInMemoryBTreeInvertedindex(virtualBufferCache, virtualFreePageManager,
                invListTypeTraits, invListCmpFactories, tokenTypeTraits, tokenCmpFactories, tokenizerFactory,
                ioManager.resolveAbsolutePath(fileManager.getBaseDir() + "_virtual_vocab_" + id));
    }

    @Override
    public ILSMIndexAccessor createAccessor(IIndexAccessParameters iap) throws HyracksDataException {
        return new LSMInvertedIndexAccessor(getLsmHarness(),
                createOpContext(iap.getModificationCallback(), iap.getSearchOperationCallback()));
    }

    @Override
    protected LSMInvertedIndexOpContext createOpContext(IModificationOperationCallback modificationCallback,
            ISearchOperationCallback searchCallback) throws HyracksDataException {
        return new LSMInvertedIndexOpContext(memoryComponents, modificationCallback, searchCallback,
                invertedIndexFieldsForNonBulkLoadOps, filterFieldsForNonBulkLoadOps, getFilterCmpFactories());
    }

    @Override
    public ITypeTraits[] getInvListTypeTraits() {
        return invListTypeTraits;
    }

    @Override
    public IBinaryComparatorFactory[] getInvListCmpFactories() {
        return invListCmpFactories;
    }

    @Override
    public ITypeTraits[] getTokenTypeTraits() {
        return tokenTypeTraits;
    }

    @Override
    public IBinaryComparatorFactory[] getTokenCmpFactories() {
        return tokenCmpFactories;
    }

    public IBinaryTokenizerFactory getTokenizerFactory() {
        return tokenizerFactory;
    }

    @Override
    public boolean isPrimaryIndex() {
        return false;
    }

    @Override
    protected LSMComponentFileReferences getMergeFileReferences(ILSMDiskComponent firstComponent,
            ILSMDiskComponent lastComponent) throws HyracksDataException {
        LSMInvertedIndexDiskComponent first = (LSMInvertedIndexDiskComponent) firstComponent;
        String firstFileName = first.getMetadataHolder().getFileReference().getFile().getName();
        LSMInvertedIndexDiskComponent last = (LSMInvertedIndexDiskComponent) lastComponent;
        String lastFileName = last.getMetadataHolder().getFileReference().getFile().getName();
        return fileManager.getRelMergeFileReference(firstFileName, lastFileName);
    }

    @Override
    protected ILSMIOOperation createFlushOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences componentFileRefs, ILSMIOOperationCallback callback)
            throws HyracksDataException {
        return new LSMInvertedIndexFlushOperation(new LSMInvertedIndexAccessor(getLsmHarness(), opCtx),
                componentFileRefs.getInsertIndexFileReference(), componentFileRefs.getDeleteIndexFileReference(),
                componentFileRefs.getBloomFilterFileReference(), callback, fileManager.getBaseDir().getAbsolutePath());
    }

    @Override
    protected ILSMIOOperation createMergeOperation(AbstractLSMIndexOperationContext opCtx,
            LSMComponentFileReferences mergeFileRefs, ILSMIOOperationCallback callback) throws HyracksDataException {
        ILSMIndexAccessor accessor = new LSMInvertedIndexAccessor(getLsmHarness(), opCtx);
        IIndexCursor cursor = new LSMInvertedIndexRangeSearchCursor(opCtx);
        return new LSMInvertedIndexMergeOperation(accessor, cursor, mergeFileRefs.getInsertIndexFileReference(),
                mergeFileRefs.getDeleteIndexFileReference(), mergeFileRefs.getBloomFilterFileReference(), callback,
                fileManager.getBaseDir().getAbsolutePath());
    }
}
