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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;

public abstract class AbstractLSMMemoryComponent extends AbstractLSMComponent implements ILSMMemoryComponent {

    private final IVirtualBufferCache vbc;
    private final AtomicBoolean isModified;
    private int writerCount;
    private boolean requestedToBeActive;
    private final MemoryComponentMetadata metadata;

    public AbstractLSMMemoryComponent(IVirtualBufferCache vbc, boolean isActive, ILSMComponentFilter filter) {
        super(filter);
        this.vbc = vbc;
        writerCount = 0;
        if (isActive) {
            state = ComponentState.READABLE_WRITABLE;
        } else {
            state = ComponentState.INACTIVE;
        }
        isModified = new AtomicBoolean();
        metadata = new MemoryComponentMetadata();
    }

    @Override
    public boolean threadEnter(LSMOperationType opType, boolean isMutableComponent) throws HyracksDataException {
        if (state == ComponentState.INACTIVE && requestedToBeActive) {
            state = ComponentState.READABLE_WRITABLE;
            requestedToBeActive = false;
        }
        switch (opType) {
            case FORCE_MODIFICATION:
                if (isMutableComponent) {
                    if (state == ComponentState.READABLE_WRITABLE || state == ComponentState.READABLE_UNWRITABLE) {
                        writerCount++;
                    } else {
                        return false;
                    }
                } else {
                    if (state == ComponentState.READABLE_UNWRITABLE
                            || state == ComponentState.READABLE_UNWRITABLE_FLUSHING) {
                        readerCount++;
                    } else {
                        return false;
                    }
                }
                break;
            case MODIFICATION:
                if (isMutableComponent) {
                    if (state == ComponentState.READABLE_WRITABLE) {
                        writerCount++;
                    } else {
                        return false;
                    }
                } else {
                    if (state == ComponentState.READABLE_UNWRITABLE
                            || state == ComponentState.READABLE_UNWRITABLE_FLUSHING) {
                        readerCount++;
                    } else {
                        return false;
                    }
                }
                break;
            case REPLICATE:
            case SEARCH:
                if (state == ComponentState.READABLE_WRITABLE || state == ComponentState.READABLE_UNWRITABLE
                        || state == ComponentState.READABLE_UNWRITABLE_FLUSHING) {
                    readerCount++;
                } else {
                    return false;
                }
                break;
            case FLUSH:
                if (state == ComponentState.READABLE_WRITABLE || state == ComponentState.READABLE_UNWRITABLE) {
                    if (writerCount != 0) {
                        throw new IllegalStateException("Trying to flush when writerCount != 0");
                    }
                    state = ComponentState.READABLE_UNWRITABLE_FLUSHING;
                    readerCount++;
                } else {
                    return false;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }
        return true;
    }

    @Override
    public void threadExit(LSMOperationType opType, boolean failedOperation, boolean isMutableComponent)
            throws HyracksDataException {
        switch (opType) {
            case FORCE_MODIFICATION:
            case MODIFICATION:
                if (isMutableComponent) {
                    writerCount--;
                    // A failed operation should not change the component state since it's better for
                    // the failed operation's effect to be no-op.
                    if (state == ComponentState.READABLE_WRITABLE && !failedOperation && isFull()) {
                        state = ComponentState.READABLE_UNWRITABLE;
                    }
                } else {
                    readerCount--;
                    if (state == ComponentState.UNREADABLE_UNWRITABLE && readerCount == 0) {
                        state = ComponentState.INACTIVE;
                    }
                }
                break;
            case REPLICATE:
            case SEARCH:
                readerCount--;
                if (state == ComponentState.UNREADABLE_UNWRITABLE && readerCount == 0) {
                    state = ComponentState.INACTIVE;
                }
                break;
            case FLUSH:
                if (state != ComponentState.READABLE_UNWRITABLE_FLUSHING) {
                    throw new IllegalStateException("Flush sees an illegal LSM memory compoenent state: " + state);
                }
                readerCount--;
                if (failedOperation) {
                    // if flush failed, return the component state to READABLE_UNWRITABLE
                    state = ComponentState.READABLE_UNWRITABLE;
                    return;
                }
                if (readerCount == 0) {
                    state = ComponentState.INACTIVE;
                } else {
                    state = ComponentState.UNREADABLE_UNWRITABLE;
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported operation " + opType);
        }

        if (readerCount <= -1 || writerCount <= -1) {
            throw new IllegalStateException("Invalid reader or writer count " + readerCount + " - " + writerCount);
        }
    }

    @Override
    public boolean isReadable() {
        if (state == ComponentState.INACTIVE || state == ComponentState.UNREADABLE_UNWRITABLE) {
            return false;
        }
        return true;
    }

    @Override
    public void setState(ComponentState state) {
        this.state = state;
    }

    @Override
    public void activate() {
        requestedToBeActive = true;
    }

    @Override
    public void setModified() {
        isModified.set(true);
    }

    @Override
    public boolean isModified() {
        return isModified.get();
    }

    @Override
    public boolean isFull() {
        return vbc.isFull();
    }

    @Override
    public void reset() throws HyracksDataException {
        isModified.set(false);
        metadata.reset();
        if (filter != null) {
            filter.reset();
        }
    }

    @Override
    public int getWriterCount() {
        return writerCount;
    }

    @Override
    public MemoryComponentMetadata getMetadata() {
        return metadata;
    }
}
