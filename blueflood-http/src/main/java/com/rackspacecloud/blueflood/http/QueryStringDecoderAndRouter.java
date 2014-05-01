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

package com.rackspacecloud.blueflood.http;

import com.codahale.metrics.Meter;
import com.rackspacecloud.blueflood.inputs.handlers.HttpMetricsIngestionServer;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.HttpConfig;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryStringDecoderAndRouter extends SimpleChannelUpstreamHandler {
    private static final Logger log = LoggerFactory.getLogger(QueryStringDecoderAndRouter.class);
    private final RouteMatcher router;
    private static final Meter requestDroppingMeter = Metrics.meter(HttpMetricsIngestionServer.class, "Requests dropped");

    public QueryStringDecoderAndRouter(RouteMatcher router) {
        this.router = router;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        Object msg = e.getMessage();
        if (msg instanceof DefaultHttpRequest) {
            final DefaultHttpRequest request = (DefaultHttpRequest) msg;
            if (Configuration.getInstance().getBooleanProperty(HttpConfig.KILLOVER_HTTP_PROCESSING)) {
                requestDroppingMeter.mark();
                HttpResponder.respond(ctx, request, HttpResponseStatus.OK);
            } else {
                router.route(ctx, HTTPRequestWithDecodedQueryParams.createHttpRequestWithDecodedQueryParams(request));
            }
        } else {
            log.error("Ignoring non HTTP message {}, from {}", e.getMessage(), e.getRemoteAddress());
            throw new Exception("Non-HTTP message from " + e.getRemoteAddress());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        log.warn("Exception event received: ", e.getCause());
    }
}
