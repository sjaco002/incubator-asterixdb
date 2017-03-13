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
package org.apache.asterix.app.nc.task;

import org.apache.asterix.app.external.ExternalLibraryUtils;
import org.apache.asterix.common.api.IAppRuntimeContext;
import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.service.IControllerService;

public class ExternalLibrarySetupTask implements INCLifecycleTask {

    private static final long serialVersionUID = 1L;
    private final boolean metadataNode;

    public ExternalLibrarySetupTask(boolean metadataNode) {
        this.metadataNode = metadataNode;
    }

    @Override
    public void perform(IControllerService cs) throws HyracksDataException {
        IAppRuntimeContext appContext = (IAppRuntimeContext) cs.getApplicationContext();
        try {
            ExternalLibraryUtils.setUpExternaLibraries(appContext.getLibraryManager(), metadataNode);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }
}