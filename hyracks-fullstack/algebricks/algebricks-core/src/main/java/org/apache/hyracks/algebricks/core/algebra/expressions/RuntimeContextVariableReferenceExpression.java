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
package org.apache.hyracks.algebricks.core.algebra.expressions;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

public class RuntimeContextVariableReferenceExpression extends AbstractLogicalExpression {
    private String name;

    public RuntimeContextVariableReferenceExpression(String name) {
        this.name = name;
    }

    public String getValue() {
        return name;
    }

    public void setValue(String name) {
        this.name = name;
    }

    @Override
    public LogicalExpressionTag getExpressionTag() {
        return LogicalExpressionTag.RUNTIME_CONTEXT_VAR;
    }

    @Override
    public String toString() {
        return "Runtime Context Variable {" + name + "}";
    }

    @Override
    public void getUsedVariables(Collection<LogicalVariable> vars) {
        // do nothing
    }

    @Override
    public void substituteVar(LogicalVariable v1, LogicalVariable v2) {
        // do nothing
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RuntimeContextVariableReferenceExpression)) {
            return false;
        } else {
            return name.equals(((RuntimeContextVariableReferenceExpression) obj).getValue());
        }
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public <R, T> R accept(ILogicalExpressionVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitRuntimeContextVariableExpression(this, arg);
    }

    @Override
    public AbstractLogicalExpression cloneExpression() {
        RuntimeContextVariableReferenceExpression c = new RuntimeContextVariableReferenceExpression(name);
        return c;
    }

    @Override
    public boolean splitIntoConjuncts(List<Mutable<ILogicalExpression>> conjs) {
        return false;
    }
}
