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

package org.apache.asterix.runtime.evaluators.functions;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.exceptions.OverflowException;
import org.apache.asterix.runtime.exceptions.UnsupportedTypeException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class NumericDivDescriptor extends AbstractNumericArithmeticEval {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new NumericDivDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.NUMERIC_DIV;
    }

    @Override
    protected double evaluateDouble(double lhs, double rhs) {
        return lhs / rhs;
    }

    @Override
    protected long evaluateInteger(long lhs, long rhs) throws HyracksDataException {
        if (rhs == 0) {
            throw new RuntimeDataException(ErrorCode.DIVISION_BY_ZERO);
        }
        if ((lhs == Long.MIN_VALUE) && (rhs == -1L)) {
            throw new OverflowException(sourceLoc, getIdentifier());
        }
        return lhs / rhs;
    }

    @Override
    protected long evaluateTimeDurationArithmetic(long chronon, int yearMonth, long dayTime, boolean isTimeOnly) {
        throw new NotImplementedException("Divide operation is not defined for temporal types");
    }

    @Override
    protected long evaluateTimeInstanceArithmetic(long chronon0, long chronon1) throws HyracksDataException {
        throw new UnsupportedTypeException(sourceLoc, getIdentifier(), ATypeTag.SERIALIZED_TIME_TYPE_TAG);
    }
}
