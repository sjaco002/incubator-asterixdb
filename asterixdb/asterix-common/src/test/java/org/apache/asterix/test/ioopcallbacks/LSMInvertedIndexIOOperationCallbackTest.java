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

package org.apache.asterix.test.ioopcallbacks;

import org.apache.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import org.apache.asterix.common.ioopcallbacks.LSMInvertedIndexIOOperationCallback;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentIdGenerator;
import org.mockito.Mockito;

public class LSMInvertedIndexIOOperationCallbackTest extends AbstractLSMIOOperationCallbackTest {

    @Override
    protected AbstractLSMIOOperationCallback getIoCallback() throws HyracksDataException {
        ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
        Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(2);
        return new LSMInvertedIndexIOOperationCallback(mockIndex, new LSMComponentIdGenerator(),
                mockIndexCheckpointManagerProvider());
    }

}
