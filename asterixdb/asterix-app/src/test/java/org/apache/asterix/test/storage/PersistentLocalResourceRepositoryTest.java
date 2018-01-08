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
package org.apache.asterix.test.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.asterix.common.TestDataUtil;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.io.FileReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PersistentLocalResourceRepositoryTest {

    protected static final String TEST_CONFIG_FILE_NAME = "src/main/resources/cc.conf";
    private static final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();

    @Before
    public void setUp() throws Exception {
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
        integrationUtil.init(true, TEST_CONFIG_FILE_NAME);
    }

    @After
    public void tearDown() throws Exception {
        integrationUtil.deinit(true);
    }

    @Test
    public void deleteMaskedFiles() throws Exception {
        final INcApplicationContext ncAppCtx = (INcApplicationContext) integrationUtil.ncs[0].getApplicationContext();
        final String nodeId = ncAppCtx.getServiceContext().getNodeId();
        final String datasetName = "ds";
        TestDataUtil.createIdOnlyDataset(datasetName);
        final Dataset dataset = TestDataUtil.getDataset(integrationUtil, datasetName);
        final String indexPath = TestDataUtil.getIndexPath(integrationUtil, dataset, nodeId);
        FileReference indexDirRef = ncAppCtx.getIoManager().resolve(indexPath);
        // create masked component files
        String indexDir = indexDirRef.getFile().getAbsolutePath();
        String componentId = "12345_12345";
        String btree = componentId + "_b";
        String filter = componentId + "_f";
        Path maskPath = Paths.get(indexDir, StorageConstants.COMPONENT_MASK_FILE_PREFIX + componentId);
        Path btreePath = Paths.get(indexDir, btree);
        Path filterPath = Paths.get(indexDir, filter);
        Files.createFile(maskPath);
        Files.createFile(btreePath);
        Files.createFile(filterPath);
        // clean up the dataset partition
        PersistentLocalResourceRepository localResourceRepository =
                (PersistentLocalResourceRepository) ncAppCtx.getLocalResourceRepository();
        DatasetLocalResource lr = (DatasetLocalResource) localResourceRepository.get(indexPath).getResource();
        int partition = lr.getPartition();
        localResourceRepository.cleanup(partition);

        // ensure all masked files and the mask were deleted
        Assert.assertFalse(maskPath.toFile().exists());
        Assert.assertFalse(btreePath.toFile().exists());
        Assert.assertFalse(filterPath.toFile().exists());

        // create single masked file
        String fileName = "someFile";
        maskPath = Paths.get(indexDir, StorageConstants.MASK_FILE_PREFIX + fileName);
        Path filePath = Paths.get(indexDir, fileName);
        Files.createFile(maskPath);
        Files.createFile(filePath);
        localResourceRepository.cleanup(partition);

        // ensure the masked file and the mask were deleted
        Assert.assertFalse(maskPath.toFile().exists());
        Assert.assertFalse(filePath.toFile().exists());
    }
}
