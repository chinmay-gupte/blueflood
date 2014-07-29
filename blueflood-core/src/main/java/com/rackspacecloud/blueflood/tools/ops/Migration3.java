package com.rackspacecloud.blueflood.tools.ops;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.TransportException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.FixedRetryBackoffStrategy;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.retry.RetryNTimes;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.types.Locator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import javax.xml.bind.DatatypeConverter;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Migration3 {
    private static final Options cliOptions = new Options();

    private static final String DST_CLUSTER = "dcluster";
    private static final String SRC_CLUSTER = "scluster";
    private static final String KEYSPACE = "keyspace";
    private static final String FROM = "from";
    private static final String TO = "to";
    private static final String COLUMN_FAMILY = "cf";
    private static final String TTL = "ttl";
    private static final String WRITE_THREADS = "writethreads";
    private static final String READ_THREADS = "readthreads";
    private static final String BATCH_SIZE = "batchsize";
    private static final String VERIFY = "verify";
    private static final String RATE = "rate";
    private static final String SAME = "SAME".intern();
    private static final String RENEW = "RENEW".intern();
    private static final String NONE = "NONE".intern();

    private static final Integer VERIFICATION_THREADS = 1;
    private static final int ADDITIONAL_CONNECTIONS_PER_HOST = 6;

    private static final PrintStream out = System.out;

    static {
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withValueSeparator(',').withDescription("[required] Source cassandra cluster (host:port).").create(SRC_CLUSTER));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withValueSeparator(',').withDescription("[required] Destination cassandra cluster (host:port).").create(DST_CLUSTER));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Keyspace (default=data)").create(KEYSPACE));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] ISO 6801 datetime (or millis since epoch) of when to start migrating data. defaults to one year ago.").create(FROM));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] ISO 6801 datetime (or millis since epoch) Datetime of when to stop migrating data. defaults to right now.").create(TO));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg().withValueSeparator(',').withDescription("[required] Which column family to migrate").create(COLUMN_FAMILY));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] ttl in seconds for new data. default=5x the default for the column family.").create(TTL));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] number of read threads to use. default=1").create(READ_THREADS));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] number of write threads to use. default=1").create(WRITE_THREADS));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] number of rows to read per query. default=100").create(BATCH_SIZE));
        cliOptions.addOption(OptionBuilder.withDescription("[optional] verify a sampling 0.5% of data copied").create(VERIFY));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] maximum number of rows per/second to transfer. default=500").create(RATE));
    }

    private static long nowInMilliSeconds() {
        return System.currentTimeMillis();
    }

    public static void main(String args[]) {
        nullRouteAllLog4j();

        Map<String, Object> options = parseOptions(args);

        final int readThreads = (Integer)options.get(READ_THREADS);
        final int writeThreads = (Integer)options.get(WRITE_THREADS);
        final int batchSize = (Integer)options.get(BATCH_SIZE);
        final String ttl = (String)options.get(TTL);
        final int rate = (Integer)options.get(RATE);
        final double verifyPercent = (Double)options.get(VERIFY);

        // connect to src cluster.
        final AstyanaxContext<Keyspace> srcContext = connect(options.get(SRC_CLUSTER).toString(), options.get(KEYSPACE).toString(), readThreads);
        final Keyspace srcKeyspace = srcContext.getEntity();

        // connect to dst cluster.
        final AstyanaxContext<Keyspace> dstContext = connect(options.get(DST_CLUSTER).toString(), options.get(KEYSPACE).toString(), writeThreads);
        final Keyspace dstKeyspace = dstContext.getEntity();

        final AtomicLong columnsTransferred = new AtomicLong(0);
        final long startClockTime = nowInMilliSeconds();

        // establish column range.
        final ByteBufferRange range = new RangeBuilder()
                .setStart((Long) options.get(FROM))
                .setEnd((Long) options.get(TO)).build();

        // create a threadpool that will write stuff into the destination.
        final ThreadPoolExecutor destWriteExecutor = new ThreadPoolExecutor(writeThreads, writeThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());

        // this threadpool performs verifications.
        final ThreadPoolExecutor verifyExecutor = new ThreadPoolExecutor(VERIFICATION_THREADS, VERIFICATION_THREADS, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

        // keep track of the number of keys that have been copied.
        final AtomicInteger processedKeys = new AtomicInteger(0);

        // sentinal that indicates it is time to stop doing everything.
        final AtomicBoolean stopAll = new AtomicBoolean(false);

        final Random random = new Random(System.nanoTime());

        // indicate what's going to happen.
        out.println(String.format("Will process metrics from %s to %s for dates %s to %s",
                options.get(SRC_CLUSTER),
                options.get(DST_CLUSTER),
                new Date((Long)options.get(FROM)),
                new Date((Long)options.get(TO))));

        final CassandraModel.MetricColumnFamily columnFamily = (CassandraModel.MetricColumnFamily)options.get(COLUMN_FAMILY);

        Function<Row<Locator, Long>, Boolean> rowFunction = new Function<Row<Locator, Long>, Boolean>() {

            @Override
            public Boolean apply(@Nullable final Row<Locator, Long> locatorLongRow) {
                // This is the best point to stop execution. This will stop the AllRowsReader ----> dstThreadPoolExecutor
                if (stopAll.get())
                    throw new RuntimeException();

                if (locatorLongRow == null) {
                    out.println("Found a null row");
                    return true;
                }

                if (!locatorLongRow.getKey().toString().contains("rackspace.monitoring")) {
                    out.println(String.format("Old style locator found. Not migrating %s", locatorLongRow.getKey()));
                    return true;
                }

                destWriteExecutor.submit(new Runnable() {
                    public void run() {
                        if (stopAll.get())
                            return;

                        ColumnList<Long> srcCols = locatorLongRow.getColumns();
                        int stopPoint = srcCols.size() - 1;

                        // Some values might be already stored. This block migrates only the values which do not exist
                        if (columnFamily.equals(CassandraModel.CF_METRICS_STRING)) {
                            try {
                                ColumnList<Long> query = dstKeyspace
                                        .prepareQuery(CassandraModel.CF_METRICS_STRING)
                                        .setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
                                        .getKey(locatorLongRow.getKey())
                                        .withColumnRange(range)
                                        .execute()
                                        .getResult();

                                int existingDataSize = query.size() - 1;

                                while (stopPoint >= 0 && existingDataSize >= 0) { // Try to get the overlapping point
                                    if (query.getColumnByIndex(existingDataSize).getStringValue().equals(srcCols.getColumnByIndex(stopPoint).getStringValue()) ||
                                            (!query.getColumnByIndex(existingDataSize).hasValue() && !srcCols.getColumnByIndex(stopPoint).hasValue())) {
                                        stopPoint--;
                                        existingDataSize--;
                                    } else if (existingDataSize >=0 && query.size() > 1 && (query.getColumnByIndex(existingDataSize + 1).getStringValue().equals(query.getColumnByIndex(existingDataSize).getStringValue()) ||
                                            (!query.getColumnByIndex(existingDataSize + 1).hasValue() && !query.getColumnByIndex(existingDataSize).hasValue()))) { // Multiple cols for same values might have been written
                                        out.println(String.format("Unoptimized data in dst for %s at index %d", locatorLongRow.getKey(), existingDataSize));
                                        existingDataSize--;
                                    }
                                    else
                                        break;
                                }

                                // all cols in src CF have been exhausted, but duplicate cols for first value
                                while (stopPoint == -1 && existingDataSize >= 0) {
                                    out.println(String.format("Checking unoptimized for %s", locatorLongRow.getKey()));
                                    if (query.size() > 1 && (query.getColumnByIndex(existingDataSize + 1).getStringValue().equals(query.getColumnByIndex(existingDataSize).getStringValue())
                                         || (!query.getColumnByIndex(existingDataSize + 1).hasValue() && !query.getColumnByIndex(existingDataSize).hasValue()))) {
                                        existingDataSize--;
                                        out.println(String.format("Unoptimized edge case %s %d", locatorLongRow.getKey(), existingDataSize));
                                    }
                                    else
                                        break;
                                }


                                if (stopPoint == -1 && existingDataSize >= 0) { // Nothing to copy. Think about it.
                                    if (existingDataSize == 0) {
                                        out.println(String.format("Unique corner case. That is it. Locator: %s %d", locatorLongRow.getKey(), existingDataSize));
                                        return;
                                    }
                                    out.println(String.format("This is impossible as we have been ingesting in dcass for a loooong time. Locator: %s %d", locatorLongRow.getKey(), existingDataSize));
                                    return;
                                } else if (stopPoint == -1 && existingDataSize == -1) {
                                    out.println(String.format("All cols already exist. Nothing to copy %s", locatorLongRow.getKey()));
                                    return;
                                } else if (stopPoint >= 0 && existingDataSize >= 0) {
                                    out.println(String.format("No overlap at the end for some values. Copying %s anyways, %d ---> %d", locatorLongRow.getKey(), stopPoint, existingDataSize));
                                    //return;
                                } else {
                                    out.println(String.format("Copying data for %s, %d ---> %d", locatorLongRow.getKey(), stopPoint, existingDataSize));
                                }
                            } catch (ConnectionException e) {
                                out.println("Connection exception while string optimization query");
                                e.printStackTrace();
                                stopAll.set(true);
                                return;
                            }
                        }

                        int safetyTtlInSeconds = 5 * (int)columnFamily.getDefaultTTL().toSeconds();
                        int nowInSeconds = (int)(System.currentTimeMillis() / 1000);

                        // copy the column.
                        MutationBatch batch = dstKeyspace.prepareMutationBatch();
                        ColumnListMutation<Long> mutation = batch.withRow(columnFamily,locatorLongRow.getKey());

                        for (int i = 0; i < stopPoint + 1; i++) {
                            if (ttl != NONE) {
                                // ttl will either be the safety value or the difference between the safety value and the age of the column.
                                int ttlSeconds = ttl == RENEW ? safetyTtlInSeconds : (safetyTtlInSeconds - nowInSeconds + (int)(srcCols.getColumnByIndex(i).getName()/1000));
                                mutation.putColumn(srcCols.getColumnByIndex(i).getName(), srcCols.getColumnByIndex(i).getByteArrayValue(), ttlSeconds);
                            } else {
                                mutation.putColumn(srcCols.getColumnByIndex(i).getName(), srcCols.getColumnByIndex(i).getByteArrayValue());
                            }
                        }

                        columnsTransferred.addAndGet(stopPoint + 1);

                        out.println(String.format("%d copied %d for %s at %.2f rps (%s)", processedKeys.incrementAndGet(), stopPoint + 1, locatorLongRow.getKey(), processedKeys.get() / ((nowInMilliSeconds() - startClockTime)/1000f), Thread.currentThread().getName()));

                        try {
                            batch.execute();
                            if (stopPoint + 1 > 0 && random.nextFloat() < verifyPercent) {
                                verifyExecutor.submit(new Runnable() {public void run() {
                                    try {
                                        ColumnList<Long> srcData = srcKeyspace.prepareQuery(columnFamily)
                                                .getKey(locatorLongRow.getKey())
                                                .withColumnRange(range)
                                                .execute()
                                                .getResult();
                                        ColumnList<Long> dstData = dstKeyspace.prepareQuery(columnFamily)
                                                .getKey(locatorLongRow.getKey())
                                                .withColumnRange(range)
                                                .execute()
                                                .getResult();

                                        checkSameResults(locatorLongRow.getKey().toString(), srcData, dstData);
                                        out.println(String.format("Verified migration for %s", locatorLongRow.getKey()));
                                    } catch (ConnectionException ex) {
                                        out.println("There was an error verifying data: " + ex.getMessage());
                                        ex.printStackTrace(out);
                                        stopAll.set(true);
                                    } catch (Exception ex) {
                                        out.println("Exception encountered while verifying data: " + ex.getMessage() + " " + locatorLongRow.getKey().toString());
                                        //stopAll.set(true);
                                    }
                                }});
                            }
                        } catch (TransportException ex) {
                            out.println(String.format("Transport exception encountered. pass for now. Locator : %s", locatorLongRow.getKey()));
                            ex.printStackTrace(out);
                            return;
                        } catch (ConnectionException ex) {
                            out.println("There was an error A: " + ex.getMessage());
                            ex.printStackTrace(out);
                            stopAll.set(true);
                        }
                    }
                });

                // this will throttle the read threads of AllRowsReader
                while (destWriteExecutor.getQueue().size() > rate) {

                    if (stopAll.get())
                        throw new RuntimeException();

                    out.println(String.format("Rate limit throttling. %s being put to sleep", Thread.currentThread().getName()));
                    try { Thread.sleep(200); } catch (Exception ex) {}
                }

                return true;
            }
        };

        try {
            Boolean result = new AllRowsReader.Builder<Locator, Long>(srcKeyspace, columnFamily)
                                    .withColumnRange((Long) options.get(FROM), (Long) options.get(TO), false, Integer.MAX_VALUE)
                                    .withPageSize(batchSize)
                                    .withConcurrencyLevel(readThreads)
                                    .withConsistencyLevel(ConsistencyLevel.CL_QUORUM)
                                    .withPartitioner(null)
                                    .forEachRow(rowFunction)
                                    .build()
                                    .call();
        } catch (Exception e) {
            out.println("Error encountered E:" + e);
            e.printStackTrace(out);
        } finally {
            // Wait for copy work to clear up
            while (destWriteExecutor.getQueue().size() > 0) {
                out.println("Waiting for write thread pool to clear up");
                try { Thread.currentThread().sleep(1000); } catch (Exception ex) {};
            }

            out.print("shutting down...");
            destWriteExecutor.shutdown();
            try {
                boolean clean = destWriteExecutor.awaitTermination(5, TimeUnit.MINUTES);
                if (clean) {
                    srcContext.shutdown();
                    dstContext.shutdown();
                } else {
                    out.println("uncleanly");
                    System.exit(-1);
                }
            } catch (InterruptedException ex) {
                ex.printStackTrace(out);
                System.exit(-1);
            }
            out.println("Done");
            System.exit(1);
        }
    }

    private static void checkSameResults(String locator, ColumnList<Long> x, ColumnList<Long> y) throws Exception {
        /*
        if (x.size() != y.size()) {
            throw new Exception("source and destination column lengths do not match");
        }*/
        /* Its ok to have different column names
        if (Sets.difference(new HashSet<Long>(x.getColumnNames()), new HashSet<Long>(y.getColumnNames())).size() != 0) {
            throw new Exception("source and destination did not contain the same column names");
        }*/

        int srcColCount = 0;

        for (int i = 0; i < y.size(); i++) {

            if (!x.getColumnByIndex(srcColCount).hasValue() && !y.getColumnByIndex(i).hasValue()) {
                out.println(String.format("No values present for %s at index %d", locator, i));
                continue;
            }

            byte[] bx = x.getColumnByIndex(srcColCount).getByteArrayValue();
            byte[] by = y.getColumnByIndex(i).getByteArrayValue();

            try {
                checkBytes(bx, by);
                srcColCount++;
            } catch (Exception e) {
                out.println(String.format("Verification failed for locator %s at index %d. Retrying for unoptimized data in dst...", locator, i));
                if (i>0) {
                    try {
                        checkBytes(y.getColumnByIndex(i).getByteArrayValue(), y.getColumnByIndex(i-1).getByteArrayValue());
                        out.println(String.format("Retry successful for locator %s at index %d. Continuing...", locator, i));
                        continue;
                    } catch (Exception ex) {
                        out.println(String.format("Retry failed for locator %s at index %d. Bailing out...", locator, i));
                        throw e;
                    }
                } else {
                    out.println(String.format("Verification failed for the first col %s. Bailing out...", locator));
                    throw e;
                }
            }
        }
    }

    private static void checkBytes (byte[] bx, byte[] by) throws Exception{
        if (bx.length != by.length) {
            throw new Exception("source and destination column lengths did not match for column ");
        }
        // only examine every third byte.
        for (int j = 0; j < bx.length; j+=3) {
            if (bx[j] != by[j]) {
                throw new Exception("source and destination column values did not match for column ");
            }
        }
    }

    private static void nullRouteAllLog4j() {
        List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for ( Logger logger : loggers ) {
            logger.setLevel(Level.OFF);
        }
    }

    private static AstyanaxContext<Keyspace> connect(String clusterSpec, String keyspace, int threads) {
        out.print(String.format("Connecting to %s:%s...", clusterSpec, keyspace));
        final List<Host> hosts = new ArrayList<Host>();
        for (String hostSpec : clusterSpec.split(",", -1)) {
            hosts.add(new Host(Host.parseHostFromHostAndPort(hostSpec), Host.parsePortFromHostAndPort(hostSpec, -1)));
        }
        int maxConsPerHost = Math.max(1, threads / hosts.size()) + ADDITIONAL_CONNECTIONS_PER_HOST;
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forKeyspace(keyspace)
                .withHostSupplier(new Supplier<List<Host>>() {
                    @Override
                    public List<Host> get() {
                        return hosts;
                    }
                })
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.NONE)
                        .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                        .setRetryPolicy(new RetryNTimes(3)))

                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(keyspace)
                        .setMaxConns(threads * 2)
                        .setMaxConnsPerHost(maxConsPerHost)
                        .setConnectTimeout(2000)
                        .setSocketTimeout(5000 * 100)
                        .setRetryBackoffStrategy(new FixedRetryBackoffStrategy(1000, 1000)))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
        out.println("done");
        return context;
    }

    private static Map<String, Object> parseOptions(String[] args) {
        final GnuParser parser = new GnuParser();
        final Map<String, Object> options = new HashMap<String, Object>();
        try {
            final long now = System.currentTimeMillis();
            CommandLine line = parser.parse(cliOptions, args);

            options.put(DST_CLUSTER, line.getOptionValue(DST_CLUSTER));
            options.put(SRC_CLUSTER, line.getOptionValue(SRC_CLUSTER));
            options.put(KEYSPACE, line.hasOption(KEYSPACE) ? line.getOptionValue(KEYSPACE) : "DATA");

            // default range is one year ago until now.
            options.put(FROM, line.hasOption(FROM) ? parseDateTime(line.getOptionValue(FROM)) : now-(365L*24L*60L*60L*1000L));
            options.put(TO, line.hasOption(TO) ? parseDateTime(line.getOptionValue(TO)) : now);

            options.put(BATCH_SIZE, line.hasOption(BATCH_SIZE) ? Integer.parseInt(line.getOptionValue(BATCH_SIZE)) : 100);

            // create a mapping of all cf names -> cf.
            // then determine which column family to process.
            Map<String, ColumnFamily<Locator, Long>> nameToCf = new HashMap<String, ColumnFamily<Locator, Long>>() {{
                for (CassandraModel.MetricColumnFamily cf : CassandraModel.getMetricColumnFamilies()) {
                    put(cf.getName(), cf);
                }
            }};
            if (nameToCf.get(line.getOptionValue(COLUMN_FAMILY)) == null) {
                throw new ParseException("Invalid column family");
            }
            CassandraModel.MetricColumnFamily columnFamily = (CassandraModel.MetricColumnFamily)nameToCf.get(line.getOptionValue(COLUMN_FAMILY));
            options.put(COLUMN_FAMILY, columnFamily);

            List<String> validTtlStrings = Lists.newArrayList(SAME, RENEW, NONE);
            String ttlString = line.hasOption(TTL) ? line.getOptionValue(TTL) : SAME;

            if (!validTtlStrings.contains(ttlString)) {
                throw new ParseException("Invalid TTL: " + ttlString);
            }

            options.put(TTL, ttlString);
            options.put(READ_THREADS, line.hasOption(READ_THREADS) ? Integer.parseInt(line.getOptionValue(READ_THREADS)) : 1);
            options.put(WRITE_THREADS, line.hasOption(WRITE_THREADS) ? Integer.parseInt(line.getOptionValue(WRITE_THREADS)) : 1);
            options.put(VERIFY, line.hasOption(VERIFY) ? Double.parseDouble(line.getOptionValue(VERIFY)) : 0.005d);
            options.put(RATE, line.hasOption(RATE) ? Integer.parseInt(line.getOptionValue(RATE)) : 200);
        } catch (ParseException ex) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("bf-migrate", cliOptions);
            System.exit(-1);
        }

        return options;
    }

    private static long parseDateTime(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ex) {
            return DatatypeConverter.parseDateTime(s).getTime().getTime();
        }
    }
}


