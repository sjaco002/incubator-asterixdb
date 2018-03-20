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
package org.apache.asterix.optimizer.rules;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.algebra.operators.CommitOperator;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.DataSource.Type;
import org.apache.asterix.metadata.declared.DatasetDataSource;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DelegateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InsertDeleteUpsertOperator.Kind;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class IntroduceAutogenerateIDRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        // match: commit OR distribute-result OR SINK - ... followed by:
        // [insert to internal dataset with autogenerated id] - assign - assign? - project
        // produce: insert - assign - assign? - assign* - project
        // **
        // OR [insert to internal dataset with autogenerated id] - assign - assign? - [datasource scan]
        // produce: insert - assign - assign? - assign* - datasource scan
        // **
        // where assign* is the newly added assign that adds the autogenerated id
        // and assign? is an assign that may exist when a filter is used

        AbstractLogicalOperator currentOp = (AbstractLogicalOperator) opRef.getValue();
        if (currentOp.getOperatorTag() == LogicalOperatorTag.DELEGATE_OPERATOR) {
            DelegateOperator dOp = (DelegateOperator) currentOp;
            if (!(dOp.getDelegate() instanceof CommitOperator)) {
                return false;
            } else if (!((CommitOperator) dOp.getDelegate()).isSink()) {
                return false;
            }

        } else if (currentOp.getOperatorTag() != LogicalOperatorTag.DISTRIBUTE_RESULT
                && currentOp.getOperatorTag() != LogicalOperatorTag.SINK) {
            return false;
        }
        ArrayDeque<AbstractLogicalOperator> opStack = new ArrayDeque<>();
        opStack.push(currentOp);
        while (currentOp.getInputs().size() == 1) {
            currentOp = (AbstractLogicalOperator) currentOp.getInputs().get(0).getValue();
            if (currentOp.getOperatorTag() == LogicalOperatorTag.INSERT_DELETE_UPSERT) {
                break;
            }
            opStack.push(currentOp);
        }
        if (currentOp.getOperatorTag() != LogicalOperatorTag.INSERT_DELETE_UPSERT) {
            return false;
        }

        InsertDeleteUpsertOperator insertOp = (InsertDeleteUpsertOperator) currentOp;
        if (insertOp.getOperation() != Kind.INSERT && insertOp.getOperation() != Kind.UPSERT) {
            return false;
        }

        DatasetDataSource dds = (DatasetDataSource) insertOp.getDataSource();
        boolean autogenerated = ((InternalDatasetDetails) dds.getDataset().getDatasetDetails()).isAutogenerated();
        if (!autogenerated) {
            return false;
        }

        if (((DataSource) insertOp.getDataSource()).getDatasourceType() != Type.INTERNAL_DATASET) {
            return false;
        }

        AbstractLogicalOperator parentOp = (AbstractLogicalOperator) currentOp.getInputs().get(0).getValue();
        if (parentOp.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assignOp = (AssignOperator) parentOp;
        LogicalVariable inputRecord;

        boolean hasFilter = false;
        AbstractLogicalOperator grandparentOp = (AbstractLogicalOperator) parentOp.getInputs().get(0).getValue();
        AbstractLogicalOperator newAssignParentOp = grandparentOp;
        AbstractLogicalOperator newAssignChildOp = assignOp;
        if (grandparentOp.getOperatorTag() == LogicalOperatorTag.PROJECT) {
            ProjectOperator projectOp = (ProjectOperator) grandparentOp;
            inputRecord = projectOp.getVariables().get(0);
        } else if (grandparentOp.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
            DataSourceScanOperator dssOp = (DataSourceScanOperator) grandparentOp;
            inputRecord = dssOp.getVariables().get(0);
        } else {
            AbstractLogicalOperator greatgrandparentOp =
                    (AbstractLogicalOperator) grandparentOp.getInputs().get(0).getValue();
            if (grandparentOp.getOperatorTag() == LogicalOperatorTag.ASSIGN
                    && greatgrandparentOp.getOperatorTag() == LogicalOperatorTag.PROJECT) {
                //filter case
                ProjectOperator projectOp = (ProjectOperator) greatgrandparentOp;
                inputRecord = projectOp.getVariables().get(0);
                newAssignParentOp = greatgrandparentOp;
                newAssignChildOp = grandparentOp;
                hasFilter = true;
            } else {
                return false;
            }
        }

        List<String> pkFieldName =
                ((InternalDatasetDetails) dds.getDataset().getDatasetDetails()).getPrimaryKey().get(0);
        ILogicalExpression rec0 = new VariableReferenceExpression(inputRecord);
        ILogicalExpression rec1 = createPrimaryKeyRecordExpression(pkFieldName);
        ILogicalExpression mergedRec = createRecordMergeFunction(rec0, rec1);
        ILogicalExpression nonNullMergedRec = createNotNullFunction(mergedRec);

        LogicalVariable v = context.newVar();
        AssignOperator newAssign = new AssignOperator(v, new MutableObject<ILogicalExpression>(nonNullMergedRec));
        newAssign.getInputs().add(new MutableObject<ILogicalOperator>(newAssignParentOp));
        newAssignChildOp.getInputs().set(0, new MutableObject<ILogicalOperator>(newAssign));
        if (hasFilter) {
            VariableUtilities.substituteVariables(newAssignChildOp, inputRecord, v, context);
        }
        VariableUtilities.substituteVariables(assignOp, inputRecord, v, context);
        VariableUtilities.substituteVariables(insertOp, inputRecord, v, context);
        context.computeAndSetTypeEnvironmentForOperator(newAssign);
        if (hasFilter) {
            context.computeAndSetTypeEnvironmentForOperator(newAssignChildOp);
        }
        context.computeAndSetTypeEnvironmentForOperator(assignOp);
        context.computeAndSetTypeEnvironmentForOperator(insertOp);
        for (AbstractLogicalOperator op : opStack) {
            VariableUtilities.substituteVariables(op, inputRecord, v, context);
            context.computeAndSetTypeEnvironmentForOperator(op);
        }

        return true;
    }

    private ILogicalExpression createNotNullFunction(ILogicalExpression mergedRec) {
        List<Mutable<ILogicalExpression>> args = new ArrayList<>();
        args.add(new MutableObject<ILogicalExpression>(mergedRec));
        AbstractFunctionCallExpression notNullFn =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.CHECK_UNKNOWN), args);
        return notNullFn;
    }

    private AbstractFunctionCallExpression createPrimaryKeyRecordExpression(List<String> pkFieldName) {
        //Create lowest level of nested uuid
        AbstractFunctionCallExpression uuidFn =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.CREATE_UUID));
        List<Mutable<ILogicalExpression>> openRecordConsArgs = new ArrayList<>();
        Mutable<ILogicalExpression> pkFieldNameExpression = new MutableObject<ILogicalExpression>(
                new ConstantExpression(new AsterixConstantValue(new AString(pkFieldName.get(pkFieldName.size() - 1)))));
        openRecordConsArgs.add(pkFieldNameExpression);
        Mutable<ILogicalExpression> pkFieldValueExpression = new MutableObject<ILogicalExpression>(uuidFn);
        openRecordConsArgs.add(pkFieldValueExpression);
        AbstractFunctionCallExpression openRecFn = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR), openRecordConsArgs);

        //Create higher levels
        for (int i = pkFieldName.size() - 2; i > -1; i--) {
            AString fieldName = new AString(pkFieldName.get(i));
            openRecordConsArgs = new ArrayList<>();
            openRecordConsArgs.add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(fieldName))));
            openRecordConsArgs.add(new MutableObject<ILogicalExpression>(openRecFn));
            openRecFn = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR), openRecordConsArgs);
        }

        return openRecFn;
    }

    private AbstractFunctionCallExpression createRecordMergeFunction(ILogicalExpression rec0, ILogicalExpression rec1) {
        List<Mutable<ILogicalExpression>> recordMergeFnArgs = new ArrayList<>();
        recordMergeFnArgs.add(new MutableObject<>(rec0));
        recordMergeFnArgs.add(new MutableObject<>(rec1));
        AbstractFunctionCallExpression recordMergeFn = new ScalarFunctionCallExpression(
                FunctionUtil.getFunctionInfo(BuiltinFunctions.RECORD_MERGE), recordMergeFnArgs);
        return recordMergeFn;
    }
}
