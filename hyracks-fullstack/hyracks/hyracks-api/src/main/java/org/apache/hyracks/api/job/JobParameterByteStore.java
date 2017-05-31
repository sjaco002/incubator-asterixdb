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
package org.apache.hyracks.api.job;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class JobParameterByteStore implements Serializable {

    private static final long serialVersionUID = 1L;

    private Map<byte[], byte[]> vars;
    private Map<String, byte[]> nonpureSingletonValues;
    private final byte[] empty = new byte[0];

    public JobParameterByteStore() {
        vars = new HashMap<>();
    }

    public Map<byte[], byte[]> getParameterMap() {
        return vars;
    }

    public void setParameters(Map<byte[], byte[]> map) {
        vars = map;
    }

    public byte[] getParameterValue(byte[] name, int start, int length) {
        for (Entry<byte[], byte[]> entry : vars.entrySet()) {
            byte[] key = entry.getKey();
            if (key.length == length) {
                boolean matched = true;
                for (int j = 0; j < length; j++) {
                    if (key[j] != name[j + start]) {
                        matched = false;
                        break;
                    }
                }
                if (matched) {
                    return entry.getValue();
                }
            }
        }
        return empty;
    }

    public synchronized byte[] getNonpureSingletonValue(String functionName) {
        byte[] value = nonpureSingletonValues.get(functionName);
        if (value == null) {
            nonpureSingletonValues.put(functionName, value);
        }
        return value;
    }

}
