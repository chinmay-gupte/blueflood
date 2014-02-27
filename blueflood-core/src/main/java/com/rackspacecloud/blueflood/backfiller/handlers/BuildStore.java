package com.rackspacecloud.blueflood.backfiller.handlers;

import com.google.gson.Gson;
import com.rackspacecloud.blueflood.backfiller.gson.CheckFromJson;
import com.rackspacecloud.blueflood.backfiller.gson.MetricPoint;
import com.rackspacecloud.blueflood.rollup.Granularity;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Points;
import com.rackspacecloud.blueflood.types.Range;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeMap;

public class BuildStore {

    private static BuildStore buildStore = new BuildStore();
    private static TreeMap<Range, TreeMap<Locator, Points>> locatorToTimestampToPoint;
    private static Map.Entry<Range, TreeMap<Locator, Points>> lastEntry;


    public static BuildStore getBuilder() {
        return buildStore;
    }

    public static void merge(InputStream jsonInput) throws IOException {

        locatorToTimestampToPoint = new TreeMap<Range, TreeMap<Locator, Points>>();

        if (lastEntry != null)
            locatorToTimestampToPoint.put(lastEntry.getKey(), lastEntry.getValue());

        BufferedReader reader = new BufferedReader(new InputStreamReader(jsonInput));
        Gson gson = new Gson();
        String line = reader.readLine();
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
                    String longMetricName = String.format("%s.rackspace.monitoring.entities.%s.checks.%s.%s.%s",
                            checkFromJson.getTenantId(),
                            checkFromJson.getEntityId(),
                            checkFromJson.getCheckType(),
                            checkFromJson.getCheckId(),
                            metricName).trim();
                    Locator metricLocator = new Locator(longMetricName);
                    long timestamp = checkFromJson.getTimestamp();
                    // BIG TODO : ---------> 0L?
                    Range rangeOfThisTimestamp = Granularity.MIN_5.deriveRange(Granularity.MIN_5.slot(timestamp), 0L);

                    if(locatorToTimestampToPoint.containsKey(rangeOfThisTimestamp)) {
                        if(locatorToTimestampToPoint.get(rangeOfThisTimestamp).containsKey(metricLocator)) {
                            locatorToTimestampToPoint.get(rangeOfThisTimestamp).get(metricLocator).getPoints().put(timestamp, new Points.Point(timestamp, metricPoint.getValue()));
                        }
                        else {
                            Points points = new Points();
                            points.add(new Points.Point(timestamp, metricPoint.getValue()));
                            locatorToTimestampToPoint.get(rangeOfThisTimestamp).put(metricLocator, points);
                        }
                    } else {
                        Points points = new Points();
                        points.add(new Points.Point(timestamp, metricPoint.getValue()));
                        TreeMap<Locator, Points> timeStampToPoint = new TreeMap<Locator, Points>();
                        timeStampToPoint.put(metricLocator, points);
                        locatorToTimestampToPoint.put(rangeOfThisTimestamp, timeStampToPoint);
                    }
                }
            }

            line = reader.readLine();
        }

    }

    public void close() {

    }

    // Kick-Ass method
    public static Map<Range, TreeMap<Locator, Points>> getEligibleData() {
        lastEntry = locatorToTimestampToPoint.lastEntry();
        return locatorToTimestampToPoint.headMap(lastEntry.getKey());
    }
}
