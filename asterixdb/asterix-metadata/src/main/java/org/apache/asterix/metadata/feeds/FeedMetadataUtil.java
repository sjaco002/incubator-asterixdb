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
package org.apache.asterix.metadata.feeds;

import java.rmi.RemoteException;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.api.IAdapterFactory;
import org.apache.asterix.external.api.IDataSourceAdapter;
import org.apache.asterix.external.api.IDataSourceAdapter.AdapterType;
import org.apache.asterix.external.feed.api.IFeed;
import org.apache.asterix.external.feed.policy.FeedPolicyAccessor;
import org.apache.asterix.external.provider.AdapterFactoryProvider;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Datatype;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.entities.FeedPolicyEntity;
import org.apache.asterix.metadata.utils.MetadataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;

/**
 * A utility class for providing helper functions for feeds
 * TODO: Refactor this class.
 */
public class FeedMetadataUtil {

    public static Dataset validateIfDatasetExists(String dataverse, String datasetName, MetadataTransactionContext ctx)
            throws CompilationException {
        Dataset dataset = MetadataManager.INSTANCE.getDataset(ctx, dataverse, datasetName);
        if (dataset == null) {
            throw new CompilationException("Unknown target dataset :" + datasetName);
        }

        if (!dataset.getDatasetType().equals(DatasetType.INTERNAL)) {
            throw new CompilationException("Statement not applicable. Dataset " + datasetName
                    + " is not of required type "
                    + DatasetType.INTERNAL);
        }
        return dataset;
    }

    public static Feed validateIfFeedExists(String dataverse, String feedName, MetadataTransactionContext ctx)
            throws CompilationException {
        Feed feed = MetadataManager.INSTANCE.getFeed(ctx, dataverse, feedName);
        if (feed == null) {
            throw new CompilationException("Unknown source feed: " + feedName);
        }
        return feed;
    }

    public static FeedPolicyEntity validateIfPolicyExists(String dataverse, String policyName,
            MetadataTransactionContext ctx) throws CompilationException {
        FeedPolicyEntity feedPolicy = MetadataManager.INSTANCE.getFeedPolicy(ctx, dataverse, policyName);
        if (feedPolicy == null) {
            feedPolicy = MetadataManager.INSTANCE.getFeedPolicy(ctx, MetadataConstants.METADATA_DATAVERSE_NAME,
                    policyName);
            if (feedPolicy == null) {
                throw new CompilationException("Unknown feed policy" + policyName);
            }
        }
        return feedPolicy;
    }

    public static void validateFeed(Feed feed, MetadataTransactionContext mdTxnCtx, ILibraryManager libraryManager)
            throws AsterixException {
        try {
            String adapterName = feed.getAdapterName();
            Map<String, String> configuration = feed.getAdapterConfiguration();
            ARecordType adapterOutputType = getOutputType(feed, configuration, ExternalDataConstants.KEY_TYPE_NAME);
            ARecordType metaType = getOutputType(feed, configuration, ExternalDataConstants.KEY_META_TYPE_NAME);
            ExternalDataUtils.prepareFeed(configuration, feed.getDataverseName(), feed.getFeedName());
            ExternalDataUtils.prepareFeed(configuration, feed.getDataverseName(), feed.getFeedName());
            // Get adapter from metadata dataset <Metadata dataverse>
            DatasourceAdapter adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx,
                    MetadataConstants.METADATA_DATAVERSE_NAME, adapterName);
            // Get adapter from metadata dataset <The feed dataverse>
            if (adapterEntity == null) {
                adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, feed.getDataverseName(), adapterName);
            }
            AdapterType adapterType;
            IAdapterFactory adapterFactory;
            if (adapterEntity != null) {
                adapterType = adapterEntity.getType();
                String adapterFactoryClassname = adapterEntity.getClassname();
                switch (adapterType) {
                    case INTERNAL:
                        adapterFactory = (IAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
                        break;
                    case EXTERNAL:
                        String[] anameComponents = adapterName.split("#");
                        String libraryName = anameComponents[0];
                        ClassLoader cl = libraryManager.getLibraryClassLoader(feed.getDataverseName(), libraryName);
                        adapterFactory = (IAdapterFactory) cl.loadClass(adapterFactoryClassname).newInstance();
                        break;
                    default:
                        throw new AsterixException("Unknown Adapter type " + adapterType);
                }
                adapterFactory.setOutputType(adapterOutputType);
                adapterFactory.setMetaType(metaType);
                adapterFactory.configure(null, configuration);
            } else {
                AdapterFactoryProvider.getAdapterFactory(libraryManager, adapterName, configuration, adapterOutputType,
                        metaType);
            }
            if (metaType == null && configuration.containsKey(ExternalDataConstants.KEY_META_TYPE_NAME)) {
                metaType = getOutputType(feed, configuration, ExternalDataConstants.KEY_META_TYPE_NAME);
                if (metaType == null) {
                    throw new AsterixException("Unknown specified feed meta output data type "
                            + configuration.get(ExternalDataConstants.KEY_META_TYPE_NAME));
                }
            }
            if (adapterOutputType == null) {
                if (!configuration.containsKey(ExternalDataConstants.KEY_TYPE_NAME)) {
                    throw new AsterixException("Unspecified feed output data type");
                }
                adapterOutputType = getOutputType(feed, configuration, ExternalDataConstants.KEY_TYPE_NAME);
                if (adapterOutputType == null) {
                    throw new AsterixException("Unknown specified feed output data type "
                            + configuration.get(ExternalDataConstants.KEY_TYPE_NAME));
                }
            }
        } catch (Exception e) {
            throw new AsterixException("Invalid feed parameters", e);
        }
    }

    @SuppressWarnings("rawtypes")
    public static Triple<IAdapterFactory, RecordDescriptor, AdapterType> getPrimaryFeedFactoryAndOutput(Feed feed,
            FeedPolicyAccessor policyAccessor, MetadataTransactionContext mdTxnCtx, ILibraryManager libraryManager)
            throws AlgebricksException {
        // This method needs to be re-visited
        String adapterName = null;
        DatasourceAdapter adapterEntity = null;
        String adapterFactoryClassname = null;
        IAdapterFactory adapterFactory = null;
        ARecordType adapterOutputType = null;
        ARecordType metaType = null;
        Triple<IAdapterFactory, RecordDescriptor, IDataSourceAdapter.AdapterType> feedProps = null;
        IDataSourceAdapter.AdapterType adapterType = null;
        try {
            adapterName = feed.getAdapterName();
            Map<String, String> configuration = feed.getAdapterConfiguration();
            configuration.putAll(policyAccessor.getFeedPolicy());
            adapterOutputType = getOutputType(feed, configuration, ExternalDataConstants.KEY_TYPE_NAME);
            metaType = getOutputType(feed, configuration, ExternalDataConstants.KEY_META_TYPE_NAME);
            ExternalDataUtils.prepareFeed(configuration, feed.getDataverseName(), feed.getFeedName());
            // Get adapter from metadata dataset <Metadata dataverse>
            adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, MetadataConstants.METADATA_DATAVERSE_NAME,
                    adapterName);
            // Get adapter from metadata dataset <The feed dataverse>
            if (adapterEntity == null) {
                adapterEntity = MetadataManager.INSTANCE.getAdapter(mdTxnCtx, feed.getDataverseName(), adapterName);
            }
            if (adapterEntity != null) {
                adapterType = adapterEntity.getType();
                adapterFactoryClassname = adapterEntity.getClassname();
                switch (adapterType) {
                    case INTERNAL:
                        adapterFactory = (IAdapterFactory) Class.forName(adapterFactoryClassname).newInstance();
                        break;
                    case EXTERNAL:
                        String[] anameComponents = adapterName.split("#");
                        String libraryName = anameComponents[0];
                        ClassLoader cl = libraryManager.getLibraryClassLoader(feed.getDataverseName(), libraryName);
                        adapterFactory = (IAdapterFactory) cl.loadClass(adapterFactoryClassname).newInstance();
                        break;
                    default:
                        throw new AsterixException("Unknown Adapter type " + adapterType);
                }
                adapterFactory.setOutputType(adapterOutputType);
                adapterFactory.setMetaType(metaType);
                adapterFactory.configure(null, configuration);
            } else {
                adapterFactory = AdapterFactoryProvider.getAdapterFactory(libraryManager, adapterName, configuration,
                        adapterOutputType, metaType);
                adapterType = IDataSourceAdapter.AdapterType.INTERNAL;
            }
            if (metaType == null) {
                metaType = getOutputType(feed, configuration, ExternalDataConstants.KEY_META_TYPE_NAME);
            }
            if (adapterOutputType == null) {
                if (!configuration.containsKey(ExternalDataConstants.KEY_TYPE_NAME)) {
                    throw new AsterixException("Unspecified feed output data type");
                }
                adapterOutputType = getOutputType(feed, configuration, ExternalDataConstants.KEY_TYPE_NAME);
            }
            int numOfOutputs = 1;
            if (metaType != null) {
                numOfOutputs++;
            }
            if (ExternalDataUtils.isChangeFeed(configuration)) {
                // get number of PKs
                numOfOutputs += ExternalDataUtils.getNumberOfKeys(configuration);
            }
            ISerializerDeserializer[] serdes = new ISerializerDeserializer[numOfOutputs];
            int i = 0;
            serdes[i++] = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(adapterOutputType);
            if (metaType != null) {
                serdes[i++] = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(metaType);
            }
            if (ExternalDataUtils.isChangeFeed(configuration)) {
                getSerdesForPKs(serdes, configuration, metaType, adapterOutputType, i);
            }
            feedProps = new Triple<>(adapterFactory, new RecordDescriptor(serdes), adapterType);
        } catch (Exception e) {
            throw new AlgebricksException("unable to create adapter", e);
        }
        return feedProps;
    }

    @SuppressWarnings("rawtypes")
    private static void getSerdesForPKs(ISerializerDeserializer[] serdes, Map<String, String> configuration,
            ARecordType metaType, ARecordType adapterOutputType, int index) throws AlgebricksException {
        int[] pkIndexes = ExternalDataUtils.getPKIndexes(configuration);
        if (metaType != null) {
            int[] pkIndicators = ExternalDataUtils.getPKSourceIndicators(configuration);
            for (int j = 0; j < pkIndexes.length; j++) {
                int aInt = pkIndexes[j];
                if (pkIndicators[j] == 0) {
                    serdes[index++] = SerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(adapterOutputType.getFieldTypes()[aInt]);
                } else if (pkIndicators[j] == 1) {
                    serdes[index++] = SerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(metaType.getFieldTypes()[aInt]);
                } else {
                    throw new AlgebricksException("a key source indicator can only be 0 or 1");
                }
            }
        } else {
            for (int aInt : pkIndexes) {
                serdes[index++] = SerializerDeserializerProvider.INSTANCE
                        .getSerializerDeserializer(adapterOutputType.getFieldTypes()[aInt]);
            }
        }
    }

    public static ARecordType getOutputType(IFeed feed, Map<String, String> configuration, String key)
            throws RemoteException, ACIDException, MetadataException {
        ARecordType outputType = null;
        String fqOutputType = configuration.get(key);

        if (fqOutputType == null) {
            return null;
        }
        String[] dataverseAndType = fqOutputType.split("[.]");
        String dataverseName;
        String datatypeName;

        if (dataverseAndType.length == 1) {
            datatypeName = dataverseAndType[0];
            dataverseName = feed.getDataverseName();
        } else if (dataverseAndType.length == 2) {
            dataverseName = dataverseAndType[0];
            datatypeName = dataverseAndType[1];
        } else {
            throw new IllegalArgumentException("Invalid value for the parameter " + key);
        }

        MetadataTransactionContext ctx = null;
        MetadataManager.INSTANCE.acquireReadLatch();
        try {
            ctx = MetadataManager.INSTANCE.beginTransaction();
            Datatype t = MetadataManager.INSTANCE.getDatatype(ctx, dataverseName, datatypeName);
            IAType type = t.getDatatype();
            if (type.getTypeTag() != ATypeTag.RECORD) {
                throw new IllegalStateException();
            }
            outputType = (ARecordType) t.getDatatype();
            MetadataManager.INSTANCE.commitTransaction(ctx);
        } catch (ACIDException | RemoteException | MetadataException e) {
            if (ctx != null) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                } catch (ACIDException | RemoteException e2) {
                    e.addSuppressed(e2);
                }
                throw e;
            }
        } finally {
            MetadataManager.INSTANCE.releaseReadLatch();
        }
        return outputType;
    }
}
