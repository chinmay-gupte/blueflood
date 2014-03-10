package com.rackspacecloud.blueflood.backfiller.handlers;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.gson.Gson;
import com.rackspacecloud.blueflood.backfiller.download.CloudFilesManager;
import com.rackspacecloud.blueflood.backfiller.gson.CheckFromJson;
import com.rackspacecloud.blueflood.backfiller.gson.MetricPoint;
import com.rackspacecloud.blueflood.cache.MetadataCache;
import com.rackspacecloud.blueflood.concurrent.AsyncChain;
import com.rackspacecloud.blueflood.concurrent.ThreadPoolBuilder;
import com.rackspacecloud.blueflood.inputs.processors.RollupTypeCacher;
import com.rackspacecloud.blueflood.inputs.processors.TypeAndUnitProcessor;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.service.IncomingMetricMetadataAnalyzer;
import com.rackspacecloud.blueflood.service.TtlConfig;
import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

public class BuildStore {
    private static final BuildStore buildStore = new BuildStore();
    private static final Logger log = LoggerFactory.getLogger(BuildStore.class);
    // number of metrics coming in for ranges which we have already rolled up that can be tolerated
    private static final int OUT_OF_RANGE_TOLERATION_THRESHOLD = 10;
    // please getEligibleData for better understanding of what is this parameter
    private static final int RANGE_BUFFER = 2;
    private static int outOfRangeToleration = 0;
    // Specifies the sorting order in TreeMap
    private static Comparator<Range> rangeComparator = new Comparator<Range>() {
        @Override public int compare(Range r1, Range r2) {
            return (int) (r1.getStart() - r2.getStart());
        }
    };

    /*
     * This is a concurrent version of TreeMap.
     * Reason behind using a TreeMap : We want to keep the mapping from ranges to metrics sorted
     * Reason for using concurrent version : We do not want to block on simultaneous read(happens in RollupGenerator), write(happens here). Both these operations should not overlap
     * due to the range buffer
     */
    private static ConcurrentSkipListMap<Range, Map<Locator, Points>> locatorToTimestampToPoint = new ConcurrentSkipListMap<Range, Map<Locator, Points>>(rangeComparator);
    // Fixed list of ranges within the replay period to rollup
    private static List<Range> rangesToRollup = new ArrayList<Range>();
    //Only one thread will be calling this. No need to make this thread safe. Shrinking subset of ranges within replay period
    public static List<Range> rangesStillApplicable = new LinkedList<Range>();

    private static AsyncChain<MetricsCollection, Boolean> defaultProcessorChain;
    private static IncomingMetricMetadataAnalyzer metricMetadataAnalyzer =
            new IncomingMetricMetadataAnalyzer(MetadataCache.getInstance());

    static {
        for(Range range : CloudFilesManager.ranges) {
            rangesToRollup.add(range);
            rangesStillApplicable.add(range);
        }

        final AsyncFunction typeAndUnitProcessor = new TypeAndUnitProcessor(
                new ThreadPoolBuilder().withName("Metric type and unit processing").build(),
                metricMetadataAnalyzer
        ).withLogger(log);

        MetadataCache rollupTypeCache = MetadataCache.createLoadingCacheInstance(
                new TimeValue(48, TimeUnit.HOURS),
                Configuration.getInstance().getIntegerProperty(CoreConfig.MAX_ROLLUP_READ_THREADS));
        final AsyncFunction rollupTypeCacher = new RollupTypeCacher(
                new ThreadPoolBuilder().withName("Rollup type persistence").build(),
                rollupTypeCache,
                true
        ).withLogger(log);

        defaultProcessorChain = new AsyncChain<MetricsCollection, Boolean>()
                .withFunction(typeAndUnitProcessor)
                .withFunction(rollupTypeCacher);
    }


    public static BuildStore getBuilder() {
        return buildStore;
    }

    public static void merge (InputStream jsonInput) throws IOException {
        MetricsCollection metricsCollection = new MetricsCollection();
        List<Metric> listOfMetrics = new ArrayList<Metric>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(jsonInput));
        Gson gson = new Gson();
        String line = reader.readLine();
        try {
            while (line != null) {
                line = line.trim();
                if (line.length() == 0)
                    continue;

                // todo: per-line instrumentation starts here.

                CheckFromJson checkFromJson = gson.fromJson(line, CheckFromJson.class);
                if (!CheckFromJson.isValid(checkFromJson)) {
                    // instrument skipped line.
                } else {
                    for (String metricName : checkFromJson.getMetricNames()) {

                        MetricPoint metricPoint = checkFromJson.getMetric(metricName);

                        // Bail out for string metrics
                        if(metricPoint.getType() == String.class)
                            continue;

                        String longMetricName = String.format("%s.rackspace.monitoring.entities.%s.checks.%s.%s.%s",
                                checkFromJson.getTenantId(),
                                checkFromJson.getEntityId(),
                                checkFromJson.getCheckType(),
                                checkFromJson.getCheckId(),
                                metricName).trim();
                        Locator metricLocator = new Locator(longMetricName);
                        long timestamp = checkFromJson.getTimestamp();
                        Range rangeOfThisTimestamp = Granularity.MIN_5.deriveRange(Granularity.MIN_5.slot(timestamp), new Date().getTime());

                        //Do not add timestamps that lie out of range
                        if (!rangesToRollup.contains(rangeOfThisTimestamp)) {
                            log.warn("Timestamp of metric found lying out the range");
                            continue;
                        }

                        //These are out of band timestamps lying in the ranges which we have already rolled
                        if (rangesToRollup.contains(rangeOfThisTimestamp) && !rangesStillApplicable.contains(rangeOfThisTimestamp)) {
                            log.warn("Range of timestamp of metric "+ longMetricName + "is out of applicable ranges");
                            outOfRangeToleration++;

                            // If we are seeing a lot of out of band metrics, something is wrong. May be metrics are back logged a lot. stop immediately. try to increase the range buffer?
                            if (outOfRangeToleration > OUT_OF_RANGE_TOLERATION_THRESHOLD)
                                throw new RuntimeException("Starting to see a lot of metrics in non-applicable ranges");
                        }

                        // The following it required because concurrent data structure provides weak consistency. For eg. Two threads both calling get will see different results. putIfAbsent provides atomic operation
                        Map<Locator, Points> tsToPoint = locatorToTimestampToPoint.get(rangeOfThisTimestamp);
                        if(tsToPoint == null) {
                            // Need to synchronize this HashMap, because multiple threads might be calling containsKey and then putting values at the same time
                            final Map<Locator, Points> tsToPointVal = Collections.synchronizedMap(new HashMap<Locator, Points>());
                            tsToPoint = locatorToTimestampToPoint.putIfAbsent(rangeOfThisTimestamp, tsToPointVal);
                            if (tsToPoint == null) {
                               tsToPoint = tsToPointVal;
                            }
                        }

                        if (tsToPoint.containsKey(metricLocator)) {
                            tsToPoint.get(metricLocator).getPoints().put(timestamp, new Points.Point(timestamp, new SimpleNumber(metricPoint.getValue())));
                        } else {
                            Points points = new Points();
                            points.add(new Points.Point(timestamp, new SimpleNumber(metricPoint.getValue())));
                            tsToPoint.put(metricLocator, points);
                        }
                        //log.info("Added a new metric to buildstore. Locator:"+metricLocator+" timstamp:"+timestamp+" metric value:"+metricPoint.getValue());
                        // TODO: the unit is obviously wrong. change the metric point to be able to parse unit as well
                        Metric metric = new Metric(metricLocator, metricPoint.getValue(), timestamp, new TimeValue(Long.parseLong(TtlConfig.RAW_METRICS_TTL.getDefaultValue()), TimeUnit.DAYS), metricPoint.getType().getName());
                        listOfMetrics.add(metric);
                    }
                }

                line = reader.readLine();
            }
        } catch(Exception e) {
            log.error("Exception encountered while merging file into buildstore", e);
            throw new RuntimeException(e);
        }
        if(!metricsCollection.toMetrics().isEmpty()) {
            try {
                defaultProcessorChain.apply(metricsCollection);
            } catch (Exception e) {
                log.error("Error while persisting metadata and rollup type");
                throw new RuntimeException(e);
            }
        }
    }

    public void close() {
        // TODO : Wait for build store to get empty? But just emptying the store does not make sense to me. May be ranges have not been filled yet? Need to think of this more.
    }

    /*
     * Consider 5 files getting merged in the buildstore. Assuming the flush rate to be 1 file/min, we need all the files together in order to construct 5 min rollup
     * How are we going to decide that a particular range has been totally filled up and ready to be rolled?
     * One behaviour which we will start seeing in the buildstore is "higher" ranges starting to build up. This means the current range has almost filled up.
     * But there is still a possibility for backed up data, getting merged. So we provide RANGE_BUFFER.
     * Basically, for every call to getEligibleData, it is going to return (n-RANGE_BUFFER) ranges to get rolled up, and keep (RANGE_BUFFER) in buildstore
     * Also, note that returning the range, will eventually remove them from buildstore, after all rollups are completed in RollupGenerator for that range.
     */
    public static Map<Range, Map<Locator, Points>> getEligibleData() {

        if (locatorToTimestampToPoint.size() <= RANGE_BUFFER) {
            log.debug("Range buffer still not exceeded. Returning null data to rollup generator");
            return null;
        } else {
            Object[] sortedKeySet = locatorToTimestampToPoint.keySet().toArray();
            Range cutingPoint = (Range) sortedKeySet[sortedKeySet.length - RANGE_BUFFER - 1];
            log.info("Found completed ranges upto the cutting threshold of {}", cutingPoint);
            return locatorToTimestampToPoint.headMap(cutingPoint, true);
        }

    }
}
