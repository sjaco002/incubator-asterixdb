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
package org.apache.asterix.active;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.active.ActiveEvent.Kind;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IJobLifecycleListener;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class ActiveLifecycleListener implements IJobLifecycleListener {

    private static final Logger LOGGER = Logger.getLogger(ActiveLifecycleListener.class.getName());

    private final ActiveJobNotificationHandler notificationHandler;
    private final LinkedBlockingQueue<ActiveEvent> jobEventInbox;
    private final ExecutorService executorService;

    public ActiveLifecycleListener() {
        notificationHandler = new ActiveJobNotificationHandler();
        jobEventInbox = notificationHandler.getEventInbox();
        executorService = Executors.newSingleThreadExecutor();
        executorService.execute(notificationHandler);
    }

    @Override
    public synchronized void notifyJobStart(JobId jobId) throws HyracksException {
        EntityId entityId = notificationHandler.getEntity(jobId);
        if (entityId != null) {
            jobEventInbox.add(new ActiveEvent(jobId, Kind.JOB_STARTED, entityId));
        }
    }

    @Override
    public synchronized void notifyJobFinish(JobId jobId) throws HyracksException {
        EntityId entityId = notificationHandler.getEntity(jobId);
        if (entityId != null) {
            jobEventInbox.add(new ActiveEvent(jobId, Kind.JOB_FINISHED, entityId));
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("NO NEED TO NOTIFY JOB FINISH!");
            }
        }
    }

    @Override
    public void notifyJobCreation(JobId jobId, JobSpecification spec) throws HyracksException {
        notificationHandler.notifyJobCreation(jobId, spec);
    }

    public void receive(ActivePartitionMessage message) {
        jobEventInbox.add(new ActiveEvent(message.getJobId(), Kind.PARTITION_EVENT,
                message.getActiveRuntimeId().getEntityId(), message));
    }

    public void stop() {
        executorService.shutdown();
    }

    public ActiveJobNotificationHandler getNotificationHandler() {
        return notificationHandler;
    }
}
