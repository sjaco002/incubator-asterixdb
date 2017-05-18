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

package org.apache.hyracks.control.nc.work;

import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.nc.NodeControllerService;

/**
 * destroy a pre-distributed job
 *
 */
public class DestroyJobWork extends AbstractWork {

    private final NodeControllerService ncs;
    private final long predistributedId;

    public DestroyJobWork(NodeControllerService ncs, long predistributedId) {
        this.ncs = ncs;
        this.predistributedId = predistributedId;
    }

    @Override
    public void run() {
        try {
            ncs.removeActivityClusterGraph(predistributedId);
        } catch (HyracksException e) {
            try {
                ncs.getClusterController().notifyDistributedJobFailure(predistributedId, ncs.getId());
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }

}
