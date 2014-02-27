package com.rackspacecloud.blueflood.backfiller.handlers;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.*;

import java.io.IOException;
import java.util.*;

public class Rollup5mGenerator implements Runnable {

    // Ranges we want to rollup
    private static boolean running = false;
    private static final Granularity rollupGran = Granularity.MIN_5;
    private static final CassandraModel.MetricColumnFamily dstCF = CassandraModel.CF_METRICS_5M;

    @Override
    public void run() {

        while (running) {
            ArrayList<SingleRollupWriteContext> writeContexts = new ArrayList<SingleRollupWriteContext>();
            Map<Range, TreeMap<Locator, Points>> dataToBeRolled = BuildStore.getEligibleData();

            if(dataToBeRolled != null && !dataToBeRolled.isEmpty()) {
                Set<Range> ranges = dataToBeRolled.keySet();

                for (Range range : ranges) {

                    Set<Locator> locators = dataToBeRolled.get(range).keySet();

                    for (Locator locator : locators) {

                        //BIG TODO : figure out how we can get the rollup type
                        RollupType rollupType = null;

                        Rollup.Type rollupComputer = RollupRunnable.getRollupComputer(rollupType, Granularity.FULL);
                        try {
                            Rollup rollup = rollupComputer.compute(dataToBeRolled.get(range).get(locator));
                            writeContexts.add(new SingleRollupWriteContext(rollup, new SingleRollupReadContext(locator, range, rollupGran), dstCF));
                        } catch (IOException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
                    }

                }

            } else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }

            //Write the batch if its not empty
            if(!writeContexts.isEmpty()) {
                try {
                    AstyanaxWriter.getInstance().insertRollups(writeContexts);
                } catch (ConnectionException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }


    }
}
