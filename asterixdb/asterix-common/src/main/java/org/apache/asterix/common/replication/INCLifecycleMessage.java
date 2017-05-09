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
package org.apache.asterix.common.replication;

import org.apache.hyracks.api.messages.IMessage;

public interface INCLifecycleMessage extends IMessage {

    public enum MessageType {
        REPLAY_LOGS_REQUEST,
        REPLAY_LOGS_RESPONSE,
        PREPARE_FAILBACK_REQUEST,
        PREPARE_FAILBACK_RESPONSE,
        COMPLETE_FAILBACK_REQUEST,
        COMPLETE_FAILBACK_RESPONSE,
        STARTUP_TASK_REQUEST,
        STARTUP_TASK_RESPONSE,
        STARTUP_TASK_RESULT,
        TAKEOVER_PARTITION_REQUEST,
        TAKEOVER_PARTITION_RESPONSE,
        TAKEOVER_METADATA_NODE_REQUEST,
        TAKEOVER_METADATA_NODE_RESPONSE
    }

    /**
     * @return The message type.
     */
    MessageType getType();
}
