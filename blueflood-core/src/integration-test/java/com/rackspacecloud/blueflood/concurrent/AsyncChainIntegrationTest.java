package com.rackspacecloud.blueflood.concurrent;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.io.IntegrationTestBase;
import com.rackspacecloud.blueflood.types.IMetric;
import com.rackspacecloud.blueflood.types.Metric;
import junit.framework.Assert;
import org.junit.Test;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class AsyncChainIntegrationTest extends IntegrationTestBase {
    TestAsyncChain testAsyncChain = new TestAsyncChain(new ThreadPoolBuilder().build());
    AsyncChain<List<Metric>, Boolean> asyncChain = new AsyncChain<List<Metric>, Boolean>()
            .withFunction(testAsyncChain);
    Boolean processingComplete = false;

    public class TestAsyncChain extends AsyncFunctionWithThreadPool<List<Metric>, Boolean> {

        public TestAsyncChain(ThreadPoolExecutor threadPool) {
            super(threadPool);
        }

        public ListenableFuture<Boolean> apply(final List<Metric> input) throws Exception {
            boolean retFlag = true;
            getThreadPool().submit(new Callable<Boolean>() {
                public Boolean call() throws Exception {
                    for (IMetric metric : input);
                    processingComplete = true;
                    return true;
                }
            });

            return new NoOpFuture<Boolean>(retFlag);
        }
    }

    @Test
    public void testAsyncChainWithNoOp() throws Exception {
        List<Metric> metrics = makeRandomIntMetrics(10000);
        ListenableFuture<Boolean> returnedFuture = asyncChain.apply(metrics);
        returnedFuture.get();
        Assert.assertFalse(processingComplete);
    }
}
