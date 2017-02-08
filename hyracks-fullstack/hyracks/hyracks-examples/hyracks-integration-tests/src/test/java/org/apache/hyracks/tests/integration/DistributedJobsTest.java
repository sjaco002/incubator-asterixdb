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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class DistributedJobsTest extends AbstractIntegrationTest {

    @Test
    public void DistributedTest() throws Exception {
            execTest();
    }

    private void execTest() throws Exception {
        JobSpecification spec1 = UnionTest.createUnionJobSpec();
        JobSpecification spec2 = HeapSortMergeTest.createSortMergeJobSpec();

        //distribute both jobs
        JobId jobId1 = hcc.distributeJob(spec1);
        JobId jobId2 = hcc.distributeJob(spec2);

        //make sure it finished
        verify(cc, Mockito.timeout(5000).times(2)).storeActivityClusterGraphConstraints(any(), any());
        verify(nc1, Mockito.timeout(5000).times(2)).storeActivityClusterGraph(any(), any());
        verify(nc2, Mockito.timeout(5000).times(2)).storeActivityClusterGraph(any(), any());

        //confirm that both jobs are distributed
        Assert.assertTrue(nc1.getActivityClusterGraph(jobId1) != null && nc2.getActivityClusterGraph(jobId1) != null);
        Assert.assertTrue(nc1.getActivityClusterGraph(jobId2) != null && nc2.getActivityClusterGraph(jobId2) != null);
        Assert.assertTrue(cc.getActivityClusterGraph(jobId1) != null
                && cc.getActivityClusterGraphConstraints(jobId1) != null && cc.getJobSpecification(jobId1) != null);
        Assert.assertTrue(cc.getActivityClusterGraph(jobId2) != null
                && cc.getActivityClusterGraphConstraints(jobId2) != null && cc.getJobSpecification(jobId2) != null);

        //run the first job
        hcc.startJob(jobId1);
        hcc.waitForCompletion(jobId1);

        //destroy the first job
        hcc.destroyJob(jobId1);

        //make sure it finished
        verify(cc, Mockito.timeout(5000).times(1)).removeActivityClusterGraphConstraints(any());
        verify(nc1, Mockito.timeout(5000).times(1)).removeActivityClusterGraph(any());
        verify(nc2, Mockito.timeout(5000).times(1)).removeActivityClusterGraph(any());

        //confirm the first job is destroyed
        Assert.assertTrue(nc1.getActivityClusterGraph(jobId1) == null && nc2.getActivityClusterGraph(jobId1) == null);
        Assert.assertTrue(
                cc.getActivityClusterGraph(jobId1) == null && cc.getActivityClusterGraphConstraints(jobId1) == null);
        Assert.assertTrue(cc.getJobSpecification(jobId1) == null);

        //run the second job
        hcc.startJob(jobId2);
        hcc.waitForCompletion(jobId2);

        //run the second job again
        hcc.startJob(jobId2);
        hcc.waitForCompletion(jobId2);

        //destroy the second job
        hcc.destroyJob(jobId2);

        //make sure it finished
        verify(cc, Mockito.timeout(5000).times(2)).removeActivityClusterGraphConstraints(any());
        verify(nc1, Mockito.timeout(5000).times(2)).removeActivityClusterGraph(any());
        verify(nc2, Mockito.timeout(5000).times(2)).removeActivityClusterGraph(any());

        //confirm the second job is destroyed
        Assert.assertTrue(nc1.getActivityClusterGraph(jobId2) == null && nc2.getActivityClusterGraph(jobId2) == null);
        Assert.assertTrue(
                cc.getActivityClusterGraph(jobId2) == null && cc.getActivityClusterGraphConstraints(jobId2) == null);
        Assert.assertTrue(cc.getJobSpecification(jobId2) == null);

    }
}
