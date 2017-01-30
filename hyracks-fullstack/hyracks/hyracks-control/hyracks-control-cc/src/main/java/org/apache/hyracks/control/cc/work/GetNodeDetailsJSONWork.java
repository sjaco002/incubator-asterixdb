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

package org.apache.hyracks.control.cc.work;

import java.io.File;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.utils.PidHelper;
import org.apache.hyracks.control.common.work.IPCResponder;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class GetNodeDetailsJSONWork extends SynchronizableWork {
    private static final Logger LOGGER = Logger.getLogger(GetNodeDetailsJSONWork.class.getName());
    private final INodeManager nodeManager;
    private final CCConfig ccConfig;
    private final String nodeId;
    private final boolean includeStats;
    private final boolean includeConfig;
    private final IPCResponder<String> callback;
    private ObjectNode detail;
    private ObjectMapper om = new ObjectMapper();

    public GetNodeDetailsJSONWork(INodeManager nodeManager, CCConfig ccConfig, String nodeId, boolean includeStats,
                                  boolean includeConfig, IPCResponder<String> callback) {
        this.nodeManager = nodeManager;
        this.ccConfig = ccConfig;
        this.nodeId = nodeId;
        this.includeStats = includeStats;
        this.includeConfig = includeConfig;
        this.callback = callback;
    }

    public GetNodeDetailsJSONWork(INodeManager nodeManager, CCConfig ccConfig, String nodeId, boolean includeStats,
                                  boolean includeConfig) {
        this(nodeManager, ccConfig, nodeId, includeStats, includeConfig, null);
    }

    @Override
    protected void doRun() throws Exception {
        if (nodeId == null) {
            // null nodeId is a request for CC
            detail = getCCDetails();
            if (includeConfig) {
                addIni(detail, ccConfig);
            }
        } else {
            NodeControllerState ncs = nodeManager.getNodeControllerState(nodeId);
            if (ncs != null) {
                detail = ncs.toDetailedJSON(includeStats, includeConfig);
                if (includeConfig) {
                    addIni(detail, ncs.getNCConfig());
                }
            }
        }

        if (callback != null) {
            callback.setValue(detail == null ? null : om.writeValueAsString(detail));
        }
    }

    private ObjectNode getCCDetails()  {
        ObjectNode o = om.createObjectNode();
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        List<GarbageCollectorMXBean> gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

        if (includeConfig) {
            o.put("os_name", osMXBean.getName());
            o.put("arch", osMXBean.getArch());
            o.put("os_version", osMXBean.getVersion());
            o.put("num_processors", osMXBean.getAvailableProcessors());
            o.put("vm_name", runtimeMXBean.getVmName());
            o.put("vm_version", runtimeMXBean.getVmVersion());
            o.put("vm_vendor", runtimeMXBean.getVmVendor());
            o.putPOJO("classpath", runtimeMXBean.getClassPath().split(File.pathSeparator));
            o.putPOJO("library_path", runtimeMXBean.getLibraryPath().split(File.pathSeparator));
            o.putPOJO("boot_classpath", runtimeMXBean.getBootClassPath().split(File.pathSeparator));
            o.putPOJO("input_arguments", runtimeMXBean.getInputArguments());
            o.putPOJO("system_properties", runtimeMXBean.getSystemProperties());
            o.put("pid", PidHelper.getPid());
        }
        if (includeStats) {
            MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
            MemoryUsage nonheapUsage = memoryMXBean.getNonHeapMemoryUsage();

            List<ObjectNode> gcs = new ArrayList<>();

            for (GarbageCollectorMXBean gcMXBean : gcMXBeans) {
                ObjectNode gc = om.createObjectNode();
                gc.put("name", gcMXBean.getName());
                gc.put("collection-time", gcMXBean.getCollectionTime());
                gc.put("collection-count", gcMXBean.getCollectionCount());
                gcs.add(gc);
            }
            o.putPOJO("gcs", gcs);

            o.put("date", new Date().toString());
            o.put("heap_init_size", heapUsage.getInit());
            o.put("heap_used_size", heapUsage.getUsed());
            o.put("heap_committed_size", heapUsage.getCommitted());
            o.put("heap_max_size", heapUsage.getMax());
            o.put("nonheap_init_size", nonheapUsage.getInit());
            o.put("nonheap_used_size", nonheapUsage.getUsed());
            o.put("nonheap_committed_size", nonheapUsage.getCommitted());
            o.put("nonheap_max_size", nonheapUsage.getMax());
            o.put("thread_count", threadMXBean.getThreadCount());
            o.put("peak_thread_count", threadMXBean.getPeakThreadCount());
            o.put("started_thread_count", threadMXBean.getTotalStartedThreadCount());
            o.put("system_load_average", osMXBean.getSystemLoadAverage());
        }
        return o;
    }

    private static void addIni(ObjectNode o, Object configBean)  {
        Map<String, Object> iniMap = new HashMap<>();
        for (Field f : configBean.getClass().getFields()) {
            Option option = f.getAnnotation(Option.class);
            if (option == null) {
                continue;
            }
            final String optionName = option.name();
            Object value = null;
            try {
                value = f.get(configBean);
            } catch (IllegalAccessException e) {
                LOGGER.log(Level.WARNING, "Unable to access ini option " + optionName, e);
            }
            if (value != null) {
                if ("--".equals(optionName)) {
                    iniMap.put("app_args", value);
                } else {
                    iniMap.put(optionName.substring(1).replace('-', '_'),
                            "-iodevices".equals(optionName)
                            ? String.valueOf(value).split(",")
                            : value);
                }
            }
        }
        o.putPOJO("ini", iniMap);
    }

    public ObjectNode getDetail() {
        return detail;
    }
}
