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
package org.apache.asterix.api.http.server;

import static org.apache.asterix.api.http.servlet.ServletConstants.ASTERIX_BUILD_PROP_ATTR;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.runtime.utils.AppContextInfo;
import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.util.ServletUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public class VersionApiServlet extends AbstractServlet {
    private static final Logger LOGGER = Logger.getLogger(VersionApiServlet.class.getName());

    public VersionApiServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    @Override
    public void handle(IServletRequest request, IServletResponse response) {
        if (request.getHttpRequest().method() != HttpMethod.GET) {
            response.setStatus(HttpResponseStatus.METHOD_NOT_ALLOWED);
            return;
        }
        response.setStatus(HttpResponseStatus.OK);
        AppContextInfo props = (AppContextInfo) ctx.get(ASTERIX_BUILD_PROP_ATTR);
        Map<String, String> buildProperties = props.getBuildProperties().getAllProps();
        ObjectMapper om = new ObjectMapper();
        ObjectNode responseObject = om.createObjectNode();
        for (Map.Entry<String, String> e : buildProperties.entrySet()) {
            responseObject.put(e.getKey(), e.getValue());
        }
        try {
            ServletUtils.setContentType(response, IServlet.ContentType.TEXT_PLAIN, IServlet.Encoding.UTF8);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failure handling request", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }
        PrintWriter responseWriter = response.writer();
        responseWriter.write(responseObject.toString());
        responseWriter.flush();
    }

}
