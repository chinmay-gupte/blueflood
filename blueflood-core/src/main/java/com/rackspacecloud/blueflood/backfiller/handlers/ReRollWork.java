package com.rackspacecloud.blueflood.backfiller.handlers;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.netflix.astyanax.model.ColumnFamily;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.io.AstyanaxReader;
import com.rackspacecloud.blueflood.io.AstyanaxWriter;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.backfiller.ManualRollup;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.*;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.Metrics;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ReRollWork implements Callable<Boolean> {
    Locator locator;
    Granularity gran;
    Range range;

    private static final Logger log = LoggerFactory.getLogger(ManualRollup.class);
    private static final Meter failedMeter = Metrics.meter(ManualRollup.class, "Metadatacache exception while grabbing rollup type");
    private static final Timer rollupTimer = Metrics.timer(ManualRollup.class, "ReRoll Timer");
    private static final MetadataCache rollupTypeCache = MetadataCache.createLoadingCacheInstance(
            new TimeValue(48, TimeUnit.HOURS),
            Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_REROLL_THREADS));

    public ReRollWork (Locator locator, Granularity gran, Range range) {
        this.locator = locator;
        this.gran = gran;
        this.range = range;
    }

    @Override
    public Boolean call() throws Exception {
        Timer.Context rollupTimerContext = rollupTimer.time();
        try {
            RollupType rollupType = RollupType.fromString(rollupTypeCache.get(
                    locator, MetricMetadata.ROLLUP_TYPE.name().toLowerCase()));
            Class<? extends Rollup> rollupClass = RollupType.classOf(rollupType, gran);
            ColumnFamily<Locator, Long> srcCF = CassandraModel.getColumnFamily(rollupClass, gran.finer());
            ColumnFamily<Locator, Long> dstCF = CassandraModel.getColumnFamily(rollupClass, gran);

            //Get Rollup Computer
            Rollup.Type rollupComputer = RollupRunnable.getRollupComputer(rollupType, gran.finer());
            Iterable<Range> ranges = Range.rangesForInterval(gran, range.getStart(), range.getStop());
            ArrayList<SingleRollupWriteContext> writeContexts = new ArrayList<SingleRollupWriteContext>();

            for (Range r : ranges) {
                Points input;
                input = AstyanaxReader.getInstance().getDataToRoll(rollupClass,
                        locator, r, srcCF);
                Rollup rollup = rollupComputer.compute(input);
                writeContexts.add(new SingleRollupWriteContext(rollup, new SingleRollupReadContext(locator, r, gran), dstCF));
            }
            AstyanaxWriter.getInstance().insertRollups(writeContexts);
            log.info("Calculated rollups for Locator:" + locator + " for Granularity:" + gran + ". Reading from: " + srcCF.getName() + ". Writing to: " + dstCF.getName());
        } catch (Throwable e) {
            log.error("ReRoll failed for Locator: "+locator+" Granularity: "+gran, e);
            failedMeter.mark();
            //throw an exception here.
            throw new Exception(e);
        } finally {
            rollupTimerContext.stop();
        }
        return true;
    }
}
