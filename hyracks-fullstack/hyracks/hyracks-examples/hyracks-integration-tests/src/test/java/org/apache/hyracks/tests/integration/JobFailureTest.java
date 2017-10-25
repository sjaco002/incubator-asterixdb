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
package org.apache.hyracks.tests.integration;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.misc.SinkOperatorDescriptor;
import org.apache.hyracks.tests.util.ExceptionOnCreatePushRuntimeOperatorDescriptor;
import org.junit.Assert;
import org.junit.Test;

public class JobFailureTest extends AbstractMultiNCIntegrationTest {

    @Test
    public void failureOnCreatePushRuntime() throws Exception {
        JobId jobId = new JobId(0); // First job
        for (int i = 0; i < 20; i++) {
            execTest();
            if (i == 0) {
                // passes. read from job archive
                waitForCompletion(jobId, ExceptionOnCreatePushRuntimeOperatorDescriptor.ERROR_MESSAGE);
            }
        }
        // passes. read from job history
        waitForCompletion(jobId, ExceptionOnCreatePushRuntimeOperatorDescriptor.ERROR_MESSAGE);
        for (int i = 0; i < 300; i++) {
            execTest();
        }
        // passes. history has been cleared
        waitForCompletion(jobId, "has been cleared from job history");
    }

    @Test
    public void waitForNonExistingJob() throws Exception {
        JobId jobId = new JobId(Long.MAX_VALUE);
        waitForCompletion(jobId, "has not been created yet");
    }

    private void execTest() throws Exception {
        JobSpecification spec = new JobSpecification();
        AbstractSingleActivityOperatorDescriptor sourceOpDesc =
                new ExceptionOnCreatePushRuntimeOperatorDescriptor(spec, 0, 1, new int[] { 4 }, true);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sourceOpDesc, ASTERIX_IDS);
        SinkOperatorDescriptor sinkOpDesc = new SinkOperatorDescriptor(spec, 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sinkOpDesc, ASTERIX_IDS);
        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn, sourceOpDesc, 0, sinkOpDesc, 0);
        spec.addRoot(sinkOpDesc);
        try {
            runTest(spec, ExceptionOnCreatePushRuntimeOperatorDescriptor.ERROR_MESSAGE);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        Assert.assertTrue(
                ExceptionOnCreatePushRuntimeOperatorDescriptor.stats()
                        + ExceptionOnCreatePushRuntimeOperatorDescriptor.succeed(),
                ExceptionOnCreatePushRuntimeOperatorDescriptor.succeed());
        // should also check the content of the different ncs
    }
}
