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
package org.apache.asterix.lang.common.literal;

import org.apache.asterix.lang.common.base.Literal;
import org.apache.commons.lang.ObjectUtils;

public class DoubleLiteral extends Literal {
    private static final long serialVersionUID = -5685491458356989250L;
    private double value;

    public DoubleLiteral(double value) {
        super();
        this.value = value;
    }

    @Override
    public Double getValue() {
        return value;
    }

    public double getDoubleValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    @Override
    public Type getLiteralType() {
        return Type.DOUBLE;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(value);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof DoubleLiteral)) {
            return false;
        }
        DoubleLiteral target = (DoubleLiteral) object;
        return ObjectUtils.equals(value, target.value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
