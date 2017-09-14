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

import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * This rule removes redundant select operator, e.g., select operators
 * in which the condition is TRUE.
 * Note that the ConstantFoldingRule will evaluate the condition expression
 * during compile time if it is possible.
 *
 * @author yingyib
 */
public class RemoveRedundantSelectRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator select = (SelectOperator) op;
        ILogicalExpression cond = select.getCondition().getValue();
        if (alwaysHold(cond, context)) {
            opRef.setValue(select.getInputs().get(0).getValue());
            return true;
        }
        return false;
    }

    /**
     * Whether the condition expression always returns true.
     *
     * @param cond
     * @return true if the condition always holds; false otherwise.
     */
    private boolean alwaysHold(ILogicalExpression cond, IOptimizationContext context) {
        if (cond.equals(ConstantExpression.TRUE)) {
            return true;
        }
        if (cond.equals(new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE)))) {
            return true;
        }
        /* is-missing is always false for constant values
         * so not(is-missing(CONST)) is always true
         * this situation arises when a left outer join with an "is-missing" filtering function call
         * is converted to an inner join. See PushSelectIntoJoinRule
         */
        if (cond.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL
                && ((AbstractFunctionCallExpression) cond).getFunctionIdentifier() == BuiltinFunctions.NOT) {
            ILogicalExpression subExpr = ((AbstractFunctionCallExpression) cond).getArguments().get(0).getValue();
            if (subExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL
                    && ((AbstractFunctionCallExpression) subExpr)
                            .getFunctionIdentifier() == BuiltinFunctions.IS_MISSING) {
                subExpr = ((AbstractFunctionCallExpression) subExpr).getArguments().get(0).getValue();
                if (subExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    return true;
                }
            }

        }
        return false;
    }
}
