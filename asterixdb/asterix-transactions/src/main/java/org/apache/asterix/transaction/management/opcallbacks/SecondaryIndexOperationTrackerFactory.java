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
package org.apache.asterix.transaction.management.opcallbacks;

import org.apache.asterix.common.api.IAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.context.BaseOperationTracker;
import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;

public class SecondaryIndexOperationTrackerFactory implements ILSMOperationTrackerFactory {

    private static final long serialVersionUID = 1L;

    private final int datasetID;

    public SecondaryIndexOperationTrackerFactory(int datasetID) {
        this.datasetID = datasetID;
    }

    @Override
    public ILSMOperationTracker getOperationTracker(INCApplicationContext ctx) {
        IDatasetLifecycleManager dslcManager = ((IAppRuntimeContext) ctx.getApplicationObject())
                .getDatasetLifecycleManager();
        return new BaseOperationTracker(datasetID, dslcManager.getDatasetInfo(datasetID));
    }

}
