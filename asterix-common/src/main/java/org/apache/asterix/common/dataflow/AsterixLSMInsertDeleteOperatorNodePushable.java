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
package org.apache.asterix.common.dataflow;

import java.nio.ByteBuffer;

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.exceptions.FrameDataException;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.storage.am.common.api.ITupleFilterFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMIndexInsertUpdateDeleteOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;

public class AsterixLSMInsertDeleteOperatorNodePushable extends LSMIndexInsertUpdateDeleteOperatorNodePushable {

    private final boolean isPrimary;
    private AbstractLSMIndex lsmIndex;
    private int i = 0;

    public boolean isPrimary() {
        return isPrimary;
    }

    public AsterixLSMInsertDeleteOperatorNodePushable(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, int[] fieldPermutation, IRecordDescriptorProvider recordDescProvider, IndexOperation op,
            boolean isPrimary) {
        super(opDesc, ctx, partition, fieldPermutation, recordDescProvider, op);
        this.isPrimary = isPrimary;
    }

    @Override
    public void open() throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        accessor = new FrameTupleAccessor(inputRecDesc);
        writeBuffer = new VSizeFrame(ctx);
        indexHelper.open();
        lsmIndex = (AbstractLSMIndex) indexHelper.getIndexInstance();
        try {
            writer.open();
            modCallback = opDesc.getModificationOpCallbackFactory().createModificationOperationCallback(
                    indexHelper.getResourcePath(), indexHelper.getResourceID(), indexHelper.getResourcePartition(),
                    lsmIndex, ctx);
            indexAccessor = lsmIndex.createAccessor(modCallback, NoOpOperationCallback.INSTANCE);
            ITupleFilterFactory tupleFilterFactory = opDesc.getTupleFilterFactory();
            if (tupleFilterFactory != null) {
                tupleFilter = tupleFilterFactory.createTupleFilter(indexHelper.getTaskContext());
                frameTuple = new FrameTupleReference();
            }
            IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                    .getApplicationContext().getApplicationObject();
            AsterixLSMIndexUtil.checkAndSetFirstLSN(lsmIndex, runtimeCtx.getTransactionSubsystem().getLogManager());
        } catch (Throwable th) {
            throw new HyracksDataException(th);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        ILSMIndexAccessor lsmAccessor = (ILSMIndexAccessor) indexAccessor;
        int tupleCount = accessor.getTupleCount();
        try {
            for (; i < tupleCount; i++) {
                if (tupleFilter != null) {
                    frameTuple.reset(accessor, i);
                    if (!tupleFilter.accept(frameTuple)) {
                        continue;
                    }
                }
                tuple.reset(accessor, i);
                switch (op) {
                    case INSERT:
                        if (i == 0 && isPrimary) {
                            lsmAccessor.insert(tuple);
                        } else {
                            lsmAccessor.forceInsert(tuple);
                        }
                        break;
                    case DELETE:
                        if (i == 0 && isPrimary) {
                            lsmAccessor.delete(tuple);
                        } else {
                            lsmAccessor.forceDelete(tuple);
                        }
                        break;
                    default: {
                        throw new HyracksDataException(
                                "Unsupported operation " + op + " in tree index InsertDelete operator");
                    }
                }
            }
        } catch (Throwable th) {
            FrameDataException fde = new FrameDataException(i, th);
            throw fde;
        }
        writeBuffer.ensureFrameSize(buffer.capacity());
        FrameUtils.copyAndFlip(buffer, writeBuffer.getBuffer());
        FrameUtils.flushFrame(writeBuffer.getBuffer(), writer);
        i = 0;
    }

    @Override
    public void close() throws HyracksDataException {
        if (lsmIndex != null) {
            try {
                indexHelper.close();
            } finally {
                writer.close();
            }
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        if (lsmIndex != null) {
            writer.fail();
        }
    }
}
