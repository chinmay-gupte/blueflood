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
package com.rackspacecloud.blueflood.eventemitter;

import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import java.util.concurrent.*;

public class RollupEventEmitter extends Emitter<RollupEvent> {
    private static final int numberOfWorkers = 5;
    private static ThreadPoolExecutor eventExecutors;
    private static final RollupEventEmitter instance = new RollupEventEmitter();

    private RollupEventEmitter() {
        eventExecutors = new ThreadPoolBuilder()
                .withName("RollupEventEmitter ThreadPool")
                .withCorePoolSize(numberOfWorkers)
                .withMaxPoolSize(numberOfWorkers)
                .withUnboundedQueue()
                .build();
    }

    public static RollupEventEmitter getInstance() { return instance; }

    @Override
    public Future emit(final String event, final RollupEvent... eventPayload) {
        return eventExecutors.submit(new Runnable() {
            @Override
            public void run() {
                RollupEventEmitter.super.emit(event, eventPayload);
            }
        });
    }
}