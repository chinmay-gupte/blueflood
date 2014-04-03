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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.QueryStringDecoder;

public class QueryStringDecoderAndRouter extends SimpleChannelInboundHandler<DefaultFullHttpRequest> {
    private static final Logger log = LoggerFactory.getLogger(QueryStringDecoderAndRouter.class);
    private final RouteMatcher router;

    public QueryStringDecoderAndRouter(RouteMatcher router) {
        this.router = router;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, DefaultFullHttpRequest request) throws Exception {
        System.out.println("received request "+request);
        QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
        router.route(channelHandlerContext, request.setUri(decoder.path()));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("Exception event received: ", cause.getCause());
    }

}
