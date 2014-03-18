/*
 * Copyright 2013 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.inputs.handlers.HttpMetricsIngestionServer;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * HTTP Ingestion Service.
 */
public class HttpIngestionService implements IngestionService {
    private HttpMetricsIngestionServer server;

    public void startService(ScheduleContext context) {
        server = new HttpMetricsIngestionServer(context);
        if(tenantRegexPattern != null) {
            server.setTenantRegexForDroppingMetrics(tenantRegexPattern);
        }
    }
}
