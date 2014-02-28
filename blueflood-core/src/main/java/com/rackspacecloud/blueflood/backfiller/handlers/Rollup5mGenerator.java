package com.rackspacecloud.blueflood.backfiller.handlers;

import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Rollup5mGenerator implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Rollup5mGenerator.class);
    private static final Granularity rollupGran = Granularity.MIN_5;
    private static final CassandraModel.MetricColumnFamily dstCF = CassandraModel.CF_METRICS_5M;

    @Override
    public void run() {
        // TODO: need to make this multi threaded
        while (true) {
            ArrayList<SingleRollupWriteContext> writeContexts = new ArrayList<SingleRollupWriteContext>();
            Map<Range, TreeMap<Locator, Points>> dataToBeRolled = BuildStore.getEligibleData();

            try {

                if (dataToBeRolled != null && !dataToBeRolled.isEmpty()) {
                    Set<Range> ranges = dataToBeRolled.keySet();
                    for (Range range : ranges) {
                        Set<Locator> locators = dataToBeRolled.get(range).keySet();
                        for (Locator locator : locators) {
                            Rollup.Type rollupComputer = RollupRunnable.getRollupComputer(RollupType.BF_BASIC, Granularity.FULL);
                            Rollup rollup = rollupComputer.compute(dataToBeRolled.get(range).get(locator));
                            writeContexts.add(new SingleRollupWriteContext(rollup, new SingleRollupReadContext(locator, range, rollupGran), dstCF));
                        }
                    }
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) { }
                }

                //Write the batch if its not empty
                if(!writeContexts.isEmpty()) {
                        AstyanaxWriter.getInstance().insertRollups(writeContexts);
                }

            } catch (Throwable e) {
                log.error("Exception encountered while calculating rollups", e);
                throw new RuntimeException();
            }
        }
    }
}
