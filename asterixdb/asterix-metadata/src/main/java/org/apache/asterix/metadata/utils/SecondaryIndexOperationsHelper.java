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

package org.apache.asterix.metadata.utils;

import java.io.DataOutput;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;
import org.apache.asterix.common.config.IPropertiesProvider;
import org.apache.asterix.common.context.ITransactionSubsystemProvider;
import org.apache.asterix.common.context.TransactionSubsystemProvider;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.transactions.IRecoveryManager.ResourceType;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.external.indexing.ExternalFile;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.external.operators.ExternalIndexBulkModifyOperatorDescriptor;
import org.apache.asterix.external.operators.ExternalScanOperatorDescriptor;
import org.apache.asterix.formats.nontagged.BinaryBooleanInspector;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.AndDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CastTypeDescriptor;
import org.apache.asterix.runtime.evaluators.functions.IsUnknownDescriptor;
import org.apache.asterix.runtime.evaluators.functions.NotDescriptor;
import org.apache.asterix.runtime.job.listener.JobEventListenerFactory;
import org.apache.asterix.runtime.utils.AppContextInfo;
import org.apache.asterix.runtime.utils.RuntimeComponentsProvider;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexInstantSearchOperationCallbackFactory;
import org.apache.asterix.transaction.management.service.transaction.JobIdFactory;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.StreamSelectRuntimeFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IJobletEventListenerFactory;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

@SuppressWarnings("rawtypes")
// TODO: We should eventually have a hierarchy of classes that can create all
// possible index job specs,
// not just for creation.
public abstract class SecondaryIndexOperationsHelper {
    protected final PhysicalOptimizationConfig physOptConf;
    protected final MetadataProvider metadataProvider;
    protected final Dataset dataset;
    protected final Index index;
    protected final ARecordType itemType;
    protected final ARecordType metaType;
    protected final ARecordType enforcedItemType;
    protected final ARecordType enforcedMetaType;
    protected ISerializerDeserializer metaSerde;
    protected ISerializerDeserializer payloadSerde;
    protected IFileSplitProvider primaryFileSplitProvider;
    protected AlgebricksPartitionConstraint primaryPartitionConstraint;
    protected IFileSplitProvider secondaryFileSplitProvider;
    protected AlgebricksPartitionConstraint secondaryPartitionConstraint;
    protected boolean anySecondaryKeyIsNullable = false;
    protected long numElementsHint;
    protected IBinaryComparatorFactory[] primaryComparatorFactories;
    protected int[] primaryBloomFilterKeyFields;
    protected RecordDescriptor primaryRecDesc;
    protected IBinaryComparatorFactory[] secondaryComparatorFactories;
    protected ITypeTraits[] secondaryTypeTraits;
    protected int[] secondaryBloomFilterKeyFields;
    protected RecordDescriptor secondaryRecDesc;
    protected IScalarEvaluatorFactory[] secondaryFieldAccessEvalFactories;
    protected IPropertiesProvider propertiesProvider;
    protected ILSMMergePolicyFactory mergePolicyFactory;
    protected Map<String, String> mergePolicyFactoryProperties;
    protected RecordDescriptor enforcedRecDesc;
    protected int numFilterFields;
    protected List<String> filterFieldName;
    protected ITypeTraits[] filterTypeTraits;
    protected IBinaryComparatorFactory[] filterCmpFactories;
    protected int[] secondaryFilterFields;
    protected int[] primaryFilterFields;
    protected int[] primaryBTreeFields;
    protected int[] secondaryBTreeFields;
    protected List<ExternalFile> externalFiles;
    protected int numPrimaryKeys;

    // Prevent public construction. Should be created via createIndexCreator().
    protected SecondaryIndexOperationsHelper(Dataset dataset, Index index, PhysicalOptimizationConfig physOptConf,
            IPropertiesProvider propertiesProvider, MetadataProvider metadataProvider, ARecordType recType,
            ARecordType metaType, ARecordType enforcedType, ARecordType enforcedMetaType) {
        this.dataset = dataset;
        this.index = index;
        this.physOptConf = physOptConf;
        this.propertiesProvider = propertiesProvider;
        this.metadataProvider = metadataProvider;
        this.itemType = recType;
        this.metaType = metaType;
        this.enforcedItemType = enforcedType;
        this.enforcedMetaType = enforcedMetaType;
    }

    public static SecondaryIndexOperationsHelper createIndexOperationsHelper(Dataset dataset, Index index,
            MetadataProvider metadataProvider, PhysicalOptimizationConfig physOptConf, ARecordType recType,
            ARecordType metaType, ARecordType enforcedType, ARecordType enforcedMetaType) throws AlgebricksException {
        IPropertiesProvider asterixPropertiesProvider = AppContextInfo.INSTANCE;
        SecondaryIndexOperationsHelper indexOperationsHelper;
        switch (index.getIndexType()) {
            case BTREE:
                indexOperationsHelper =
                        new SecondaryBTreeOperationsHelper(dataset, index, physOptConf, asterixPropertiesProvider,
                                metadataProvider, recType, metaType, enforcedType, enforcedMetaType);
                break;
            case RTREE:
                indexOperationsHelper =
                        new SecondaryRTreeOperationsHelper(dataset, index, physOptConf, asterixPropertiesProvider,
                                metadataProvider, recType, metaType, enforcedType, enforcedMetaType);
                break;
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX:
                indexOperationsHelper = new SecondaryInvertedIndexOperationsHelper(dataset, index, physOptConf,
                        asterixPropertiesProvider, metadataProvider, recType, metaType, enforcedType,
                        enforcedMetaType);
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE, index.getIndexType());
        }
        indexOperationsHelper.init();
        return indexOperationsHelper;
    }

    public abstract JobSpecification buildCreationJobSpec() throws AlgebricksException;

    public abstract JobSpecification buildLoadingJobSpec() throws AlgebricksException;

    public abstract JobSpecification buildCompactJobSpec() throws AlgebricksException;

    protected void init() throws AlgebricksException {
        payloadSerde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(itemType);
        metaSerde =
                metaType == null ? null : SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(metaType);
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> secondarySplitsAndConstraint =
                metadataProvider.getSplitProviderAndConstraints(index.getDataverseName(), index.getDatasetName(),
                        index.getIndexName(), dataset.getDatasetDetails().isTemp());
        secondaryFileSplitProvider = secondarySplitsAndConstraint.first;
        secondaryPartitionConstraint = secondarySplitsAndConstraint.second;
        numPrimaryKeys = DatasetUtil.getPartitioningKeys(dataset).size();
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            filterFieldName = DatasetUtil.getFilterField(dataset);
            if (filterFieldName != null) {
                numFilterFields = 1;
            } else {
                numFilterFields = 0;
            }
            Pair<IFileSplitProvider, AlgebricksPartitionConstraint> primarySplitsAndConstraint =
                    metadataProvider.getSplitProviderAndConstraints(dataset.getDataverseName(),
                            dataset.getDatasetName(), dataset.getDatasetName(), dataset.getDatasetDetails().isTemp());
            primaryFileSplitProvider = primarySplitsAndConstraint.first;
            primaryPartitionConstraint = primarySplitsAndConstraint.second;
            setPrimaryRecDescAndComparators();
        }
        setSecondaryRecDescAndComparators();
        numElementsHint = metadataProvider.getCardinalityPerPartitionHint(dataset);
        Pair<ILSMMergePolicyFactory, Map<String, String>> compactionInfo =
                DatasetUtil.getMergePolicyFactory(dataset, metadataProvider.getMetadataTxnContext());
        mergePolicyFactory = compactionInfo.first;
        mergePolicyFactoryProperties = compactionInfo.second;
        if (numFilterFields > 0) {
            setFilterTypeTraitsAndComparators();
        }
    }

    protected void setFilterTypeTraitsAndComparators() throws AlgebricksException {
        filterTypeTraits = new ITypeTraits[numFilterFields];
        filterCmpFactories = new IBinaryComparatorFactory[numFilterFields];
        secondaryFilterFields = new int[numFilterFields];
        primaryFilterFields = new int[numFilterFields];
        primaryBTreeFields = new int[numPrimaryKeys + 1];
        secondaryBTreeFields = new int[index.getKeyFieldNames().size() + numPrimaryKeys];
        for (int i = 0; i < primaryBTreeFields.length; i++) {
            primaryBTreeFields[i] = i;
        }
        for (int i = 0; i < secondaryBTreeFields.length; i++) {
            secondaryBTreeFields[i] = i;
        }

        IAType type = itemType.getSubFieldType(filterFieldName);
        filterCmpFactories[0] = BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(type, true);
        filterTypeTraits[0] = TypeTraitProvider.INSTANCE.getTypeTrait(type);
        secondaryFilterFields[0] = getNumSecondaryKeys() + numPrimaryKeys;
        primaryFilterFields[0] = numPrimaryKeys + 1;
    }

    protected abstract int getNumSecondaryKeys();

    protected void setPrimaryRecDescAndComparators() throws AlgebricksException {
        List<List<String>> partitioningKeys = DatasetUtil.getPartitioningKeys(dataset);
        ISerializerDeserializer[] primaryRecFields =
                new ISerializerDeserializer[numPrimaryKeys + 1 + (dataset.hasMetaPart() ? 1 : 0)];
        ITypeTraits[] primaryTypeTraits = new ITypeTraits[numPrimaryKeys + 1 + (dataset.hasMetaPart() ? 1 : 0)];
        primaryComparatorFactories = new IBinaryComparatorFactory[numPrimaryKeys];
        primaryBloomFilterKeyFields = new int[numPrimaryKeys];
        ISerializerDeserializerProvider serdeProvider = metadataProvider.getFormat().getSerdeProvider();
        List<Integer> indicators = null;
        if (dataset.hasMetaPart()) {
            indicators = ((InternalDatasetDetails) dataset.getDatasetDetails()).getKeySourceIndicator();
        }
        for (int i = 0; i < numPrimaryKeys; i++) {
            IAType keyType =
                    (indicators == null || indicators.get(i) == 0) ? itemType.getSubFieldType(partitioningKeys.get(i))
                            : metaType.getSubFieldType(partitioningKeys.get(i));
            primaryRecFields[i] = serdeProvider.getSerializerDeserializer(keyType);
            primaryComparatorFactories[i] =
                    BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(keyType, true);
            primaryTypeTraits[i] = TypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            primaryBloomFilterKeyFields[i] = i;
        }
        primaryRecFields[numPrimaryKeys] = payloadSerde;
        primaryTypeTraits[numPrimaryKeys] = TypeTraitProvider.INSTANCE.getTypeTrait(itemType);
        if (dataset.hasMetaPart()) {
            primaryRecFields[numPrimaryKeys + 1] = payloadSerde;
            primaryTypeTraits[numPrimaryKeys + 1] = TypeTraitProvider.INSTANCE.getTypeTrait(itemType);
        }
        primaryRecDesc = new RecordDescriptor(primaryRecFields, primaryTypeTraits);
    }

    protected abstract void setSecondaryRecDescAndComparators() throws AlgebricksException;

    protected AbstractOperatorDescriptor createDummyKeyProviderOp(JobSpecification spec) throws AlgebricksException {
        // Build dummy tuple containing one field with a dummy value inside.
        ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
        DataOutput dos = tb.getDataOutput();
        tb.reset();
        try {
            // Serialize dummy value into a field.
            IntegerSerializerDeserializer.INSTANCE.serialize(0, dos);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
        // Add dummy field.
        tb.addFieldEndOffset();
        ISerializerDeserializer[] keyRecDescSers = { IntegerSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);
        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, keyProviderOp,
                primaryPartitionConstraint);
        return keyProviderOp;
    }

    protected BTreeSearchOperatorDescriptor createPrimaryIndexScanOp(JobSpecification spec)
            throws AlgebricksException {
        // -Infinity
        int[] lowKeyFields = null;
        // +Infinity
        int[] highKeyFields = null;
        ITransactionSubsystemProvider txnSubsystemProvider = TransactionSubsystemProvider.INSTANCE;
        JobId jobId = JobIdFactory.generateJobId();
        metadataProvider.setJobId(jobId);
        boolean isWriteTransaction = metadataProvider.isWriteTransaction();
        IJobletEventListenerFactory jobEventListenerFactory = new JobEventListenerFactory(jobId, isWriteTransaction);
        spec.setJobletEventListenerFactory(jobEventListenerFactory);
        Index primaryIndex = MetadataManager.INSTANCE.getIndex(metadataProvider.getMetadataTxnContext(),
                dataset.getDataverseName(), dataset.getDatasetName(), dataset.getDatasetName());

        boolean temp = dataset.getDatasetDetails().isTemp();
        ISearchOperationCallbackFactory searchCallbackFactory = temp ? NoOpOperationCallbackFactory.INSTANCE
                : new PrimaryIndexInstantSearchOperationCallbackFactory(jobId, dataset.getDatasetId(),
                        primaryBloomFilterKeyFields, txnSubsystemProvider, ResourceType.LSM_BTREE);
        BTreeSearchOperatorDescriptor primarySearchOp = new BTreeSearchOperatorDescriptor(spec, primaryRecDesc,
                RuntimeComponentsProvider.RUNTIME_PROVIDER, RuntimeComponentsProvider.RUNTIME_PROVIDER,
                primaryFileSplitProvider, primaryRecDesc.getTypeTraits(), primaryComparatorFactories,
                primaryBloomFilterKeyFields, lowKeyFields, highKeyFields, true, true,
                dataset.getIndexDataflowHelperFactory(metadataProvider, primaryIndex, itemType, metaType,
                        mergePolicyFactory, mergePolicyFactoryProperties),
                false, false, null, searchCallbackFactory, null, null,
                metadataProvider.getStorageComponentProvider().getMetadataPageManagerFactory());

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, primarySearchOp,
                primaryPartitionConstraint);
        return primarySearchOp;
    }

    protected AlgebricksMetaOperatorDescriptor createAssignOp(JobSpecification spec, int numSecondaryKeyFields,
            RecordDescriptor secondaryRecDesc) throws AlgebricksException {
        int[] outColumns = new int[numSecondaryKeyFields + numFilterFields];
        int[] projectionList = new int[numSecondaryKeyFields + numPrimaryKeys + numFilterFields];
        for (int i = 0; i < numSecondaryKeyFields + numFilterFields; i++) {
            outColumns[i] = numPrimaryKeys + i;
        }
        int projCount = 0;
        for (int i = 0; i < numSecondaryKeyFields; i++) {
            projectionList[projCount++] = numPrimaryKeys + i;
        }
        for (int i = 0; i < numPrimaryKeys; i++) {
            projectionList[projCount++] = i;
        }
        if (numFilterFields > 0) {
            projectionList[projCount] = numPrimaryKeys + numSecondaryKeyFields;
        }

        IScalarEvaluatorFactory[] sefs = new IScalarEvaluatorFactory[secondaryFieldAccessEvalFactories.length];
        for (int i = 0; i < secondaryFieldAccessEvalFactories.length; ++i) {
            sefs[i] = secondaryFieldAccessEvalFactories[i];
        }
        AssignRuntimeFactory assign = new AssignRuntimeFactory(outColumns, sefs, projectionList);
        AlgebricksMetaOperatorDescriptor asterixAssignOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { secondaryRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixAssignOp,
                primaryPartitionConstraint);
        return asterixAssignOp;
    }

    protected AlgebricksMetaOperatorDescriptor createCastOp(JobSpecification spec, DatasetType dsType) {
        CastTypeDescriptor castFuncDesc = (CastTypeDescriptor) CastTypeDescriptor.FACTORY.createFunctionDescriptor();
        castFuncDesc.setImmutableStates(enforcedItemType, itemType);

        int[] outColumns = new int[1];
        int[] projectionList = new int[(dataset.hasMetaPart() ? 2 : 1) + numPrimaryKeys];
        int recordIdx;
        //external datascan operator returns a record as the first field, instead of the last in internal case
        if (dsType == DatasetType.EXTERNAL) {
            recordIdx = 0;
            outColumns[0] = 0;
        } else {
            recordIdx = numPrimaryKeys;
            outColumns[0] = numPrimaryKeys;
        }
        for (int i = 0; i <= numPrimaryKeys; i++) {
            projectionList[i] = i;
        }
        if (dataset.hasMetaPart()) {
            projectionList[numPrimaryKeys + 1] = numPrimaryKeys + 1;
        }
        IScalarEvaluatorFactory[] castEvalFact =
                new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(recordIdx) };
        IScalarEvaluatorFactory[] sefs = new IScalarEvaluatorFactory[1];
        sefs[0] = castFuncDesc.createEvaluatorFactory(castEvalFact);
        AssignRuntimeFactory castAssign = new AssignRuntimeFactory(outColumns, sefs, projectionList);
        return new AlgebricksMetaOperatorDescriptor(spec, 1, 1, new IPushRuntimeFactory[] { castAssign },
                new RecordDescriptor[] { enforcedRecDesc });
    }

    protected ExternalSortOperatorDescriptor createSortOp(JobSpecification spec,
            IBinaryComparatorFactory[] secondaryComparatorFactories, RecordDescriptor secondaryRecDesc) {
        int[] sortFields = new int[secondaryComparatorFactories.length];
        for (int i = 0; i < secondaryComparatorFactories.length; i++) {
            sortFields[i] = i;
        }
        ExternalSortOperatorDescriptor sortOp = new ExternalSortOperatorDescriptor(spec,
                physOptConf.getMaxFramesExternalSort(), sortFields, secondaryComparatorFactories, secondaryRecDesc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortOp, primaryPartitionConstraint);
        return sortOp;
    }

    protected TreeIndexBulkLoadOperatorDescriptor createTreeIndexBulkLoadOp(JobSpecification spec,
            int[] fieldPermutation, IIndexDataflowHelperFactory dataflowHelperFactory, float fillFactor)
            throws AlgebricksException {
        TreeIndexBulkLoadOperatorDescriptor treeIndexBulkLoadOp = new TreeIndexBulkLoadOperatorDescriptor(spec,
                secondaryRecDesc, RuntimeComponentsProvider.RUNTIME_PROVIDER,
                RuntimeComponentsProvider.RUNTIME_PROVIDER, secondaryFileSplitProvider,
                secondaryRecDesc.getTypeTraits(), secondaryComparatorFactories, secondaryBloomFilterKeyFields,
                fieldPermutation, fillFactor, false, numElementsHint, false, dataflowHelperFactory,
                metadataProvider.getStorageComponentProvider().getMetadataPageManagerFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, treeIndexBulkLoadOp,
                secondaryPartitionConstraint);
        return treeIndexBulkLoadOp;
    }

    public AlgebricksMetaOperatorDescriptor createFilterNullsSelectOp(JobSpecification spec, int numSecondaryKeyFields,
            RecordDescriptor secondaryRecDesc) throws AlgebricksException {
        IScalarEvaluatorFactory[] andArgsEvalFactories = new IScalarEvaluatorFactory[numSecondaryKeyFields];
        NotDescriptor notDesc = new NotDescriptor();
        IsUnknownDescriptor isUnknownDesc = new IsUnknownDescriptor();
        for (int i = 0; i < numSecondaryKeyFields; i++) {
            // Access column i, and apply 'is not null'.
            ColumnAccessEvalFactory columnAccessEvalFactory = new ColumnAccessEvalFactory(i);
            IScalarEvaluatorFactory isUnknownEvalFactory =
                    isUnknownDesc.createEvaluatorFactory(new IScalarEvaluatorFactory[] { columnAccessEvalFactory });
            IScalarEvaluatorFactory notEvalFactory =
                    notDesc.createEvaluatorFactory(new IScalarEvaluatorFactory[] { isUnknownEvalFactory });
            andArgsEvalFactories[i] = notEvalFactory;
        }
        IScalarEvaluatorFactory selectCond;
        if (numSecondaryKeyFields > 1) {
            // Create conjunctive condition where all secondary index keys must
            // satisfy 'is not null'.
            AndDescriptor andDesc = new AndDescriptor();
            selectCond = andDesc.createEvaluatorFactory(andArgsEvalFactories);
        } else {
            selectCond = andArgsEvalFactories[0];
        }
        StreamSelectRuntimeFactory select =
                new StreamSelectRuntimeFactory(selectCond, null, BinaryBooleanInspector.FACTORY, false, -1, null);
        AlgebricksMetaOperatorDescriptor asterixSelectOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { select }, new RecordDescriptor[] { secondaryRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixSelectOp,
                primaryPartitionConstraint);
        return asterixSelectOp;
    }

    // This method creates a source indexing operator for external data
    protected ExternalScanOperatorDescriptor createExternalIndexingOp(JobSpecification spec)
            throws AlgebricksException {
        // A record + primary keys
        ISerializerDeserializer[] serdes = new ISerializerDeserializer[1 + numPrimaryKeys];
        ITypeTraits[] typeTraits = new ITypeTraits[1 + numPrimaryKeys];
        // payload serde and type traits for the record slot
        serdes[0] = payloadSerde;
        typeTraits[0] = TypeTraitProvider.INSTANCE.getTypeTrait(itemType);
        //  serdes and type traits for rid fields
        for (int i = 1; i < serdes.length; i++) {
            serdes[i] = IndexingConstants.getSerializerDeserializer(i - 1);
            typeTraits[i] = IndexingConstants.getTypeTraits(i - 1);
        }
        // output record desc
        RecordDescriptor indexerDesc = new RecordDescriptor(serdes, typeTraits);

        // Create the operator and its partition constraits
        Pair<ExternalScanOperatorDescriptor, AlgebricksPartitionConstraint> indexingOpAndConstraints;
        try {
            indexingOpAndConstraints = ExternalIndexingOperations.createExternalIndexingOp(spec, metadataProvider,
                    dataset, itemType, indexerDesc, externalFiles);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, indexingOpAndConstraints.first,
                indexingOpAndConstraints.second);

        // Set the primary partition constraints to this partition constraints
        primaryPartitionConstraint = indexingOpAndConstraints.second;
        return indexingOpAndConstraints.first;
    }

    protected AlgebricksMetaOperatorDescriptor createExternalAssignOp(JobSpecification spec, int numSecondaryKeys,
            RecordDescriptor secondaryRecDesc) throws AlgebricksException {
        int[] outColumns = new int[numSecondaryKeys];
        int[] projectionList = new int[numSecondaryKeys + numPrimaryKeys];
        for (int i = 0; i < numSecondaryKeys; i++) {
            outColumns[i] = i + numPrimaryKeys + 1;
            projectionList[i] = i + numPrimaryKeys + 1;
        }

        IScalarEvaluatorFactory[] sefs = new IScalarEvaluatorFactory[secondaryFieldAccessEvalFactories.length];
        for (int i = 0; i < secondaryFieldAccessEvalFactories.length; ++i) {
            sefs[i] = secondaryFieldAccessEvalFactories[i];
        }
        //add External RIDs to the projection list
        for (int i = 0; i < numPrimaryKeys; i++) {
            projectionList[numSecondaryKeys + i] = i + 1;
        }

        AssignRuntimeFactory assign = new AssignRuntimeFactory(outColumns, sefs, projectionList);
        return new AlgebricksMetaOperatorDescriptor(spec, 1, 1, new IPushRuntimeFactory[] { assign },
                new RecordDescriptor[] { secondaryRecDesc });
    }

    protected ExternalIndexBulkModifyOperatorDescriptor createExternalIndexBulkModifyOp(JobSpecification spec,
            int[] fieldPermutation, IIndexDataflowHelperFactory dataflowHelperFactory, float fillFactor)
            throws AlgebricksException {
        // create a list of file ids
        int numOfDeletedFiles = 0;
        for (ExternalFile file : externalFiles) {
            if (file.getPendingOp() == ExternalFilePendingOp.DROP_OP) {
                numOfDeletedFiles++;
            }
        }
        int[] deletedFiles = new int[numOfDeletedFiles];
        int i = 0;
        for (ExternalFile file : externalFiles) {
            if (file.getPendingOp() == ExternalFilePendingOp.DROP_OP) {
                deletedFiles[i] = file.getFileNumber();
            }
        }
        ExternalIndexBulkModifyOperatorDescriptor treeIndexBulkLoadOp = new ExternalIndexBulkModifyOperatorDescriptor(
                spec, RuntimeComponentsProvider.RUNTIME_PROVIDER, RuntimeComponentsProvider.RUNTIME_PROVIDER,
                secondaryFileSplitProvider, secondaryTypeTraits, secondaryComparatorFactories,
                secondaryBloomFilterKeyFields, dataflowHelperFactory, NoOpOperationCallbackFactory.INSTANCE,
                deletedFiles, fieldPermutation, fillFactor, numElementsHint,
                metadataProvider.getStorageComponentProvider().getMetadataPageManagerFactory());
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, treeIndexBulkLoadOp,
                secondaryPartitionConstraint);
        return treeIndexBulkLoadOp;
    }

    public List<ExternalFile> getExternalFiles() {
        return externalFiles;
    }

    public void setExternalFiles(List<ExternalFile> externalFiles) {
        this.externalFiles = externalFiles;
    }
}
