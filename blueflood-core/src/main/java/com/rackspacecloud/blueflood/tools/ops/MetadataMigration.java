package com.rackspacecloud.blueflood.tools.ops;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
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
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.io.serializers.LocatorSerializer;
import com.rackspacecloud.blueflood.io.serializers.StringMetadataSerializer;
import com.rackspacecloud.blueflood.types.Locator;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MetadataMigration {
    private static final Options cliOptions = new Options();

    private static final String DST_CLUSTER = "dcluster";
    private static final String SRC_CLUSTER = "scluster";
    private static final String KEYSPACE = "keyspace";
    private static final String WRITE_THREADS = "writethreads";
    private static final String READ_THREADS = "readthreads";
    private static final String BATCH_SIZE = "batchsize";
    private static final String VERIFY = "verify";
    private static final String RATE = "rate";

    private static final Integer VERIFICATION_THREADS = 1;
    private static final int ADDITIONAL_CONNECTIONS_PER_HOST = 6;

    private static final PrintStream out = System.out;

    static {
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withValueSeparator(',').withDescription("[required] Source cassandra cluster (host:port).").create(SRC_CLUSTER));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withValueSeparator(',').withDescription("[required] Destination cassandra cluster (host:port).").create(DST_CLUSTER));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Keyspace (default=data)").create(KEYSPACE));
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
        final int rate = (Integer)options.get(RATE);
        final double verifyPercent = (Double)options.get(VERIFY);

        // connect to src cluster.
        final AstyanaxContext<Keyspace> srcContext = connect(options.get(SRC_CLUSTER).toString(), options.get(KEYSPACE).toString(), readThreads);
        final Keyspace srcKeyspace = srcContext.getEntity();

        // connect to dst cluster.
        final AstyanaxContext<Keyspace> dstContext = connect(options.get(DST_CLUSTER).toString(), options.get(KEYSPACE).toString(), writeThreads);
        final Keyspace dstKeyspace = dstContext.getEntity();

        final ColumnFamily<Locator, String> columnFamily = new ColumnFamily<Locator, String>("metrics_metadata",
                LocatorSerializer.get(),
                StringSerializer.get());

        final AtomicLong columnsTransferred = new AtomicLong(0);
        final long startClockTime = nowInMilliSeconds();

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
        out.println(String.format("Will migrate metadata from %s to %s",
                options.get(SRC_CLUSTER),
                options.get(DST_CLUSTER)));

        Function<Row<Locator, String>, Boolean> rowFunction = new Function<Row<Locator, String>, Boolean>() {

            @Override
            public Boolean apply(@Nullable final Row<Locator, String> locatorLongRow) {
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

                        // Migrate metadata only if it is not existing in the dst
                        try {
                            final ColumnList<String> results = dstKeyspace
                                    .prepareQuery(CassandraModel.CF_METRIC_METADATA)
                                    .getKey(locatorLongRow.getKey())
                                    .execute().getResult();
                            if (!results.isEmpty()) {
                                out.println(String.format("Metadata already exists for locator %s", locatorLongRow.getKey()));
                                return;
                            }
                        } catch (ConnectionException e) {
                            out.println("There was an error while trying to get metadata from the destination: " + e.getMessage());
                            e.printStackTrace(out);
                            stopAll.set(true);
                        }

                        ColumnList<String> srcCols = locatorLongRow.getColumns();

                        // copy the column.
                        MutationBatch batch = dstKeyspace.prepareMutationBatch();
                        ColumnListMutation<String> mutation = batch.withRow(columnFamily,locatorLongRow.getKey());

                        for (int i = 0; i < srcCols.size(); i++) {
                                mutation.putColumn(srcCols.getColumnByIndex(i).getName(), srcCols.getColumnByIndex(i).getValue(StringMetadataSerializer.get()), StringMetadataSerializer.get(), null);
                        }

                        columnsTransferred.addAndGet(srcCols.size());

                        out.println(String.format("%d copied %d for %s at %.2f rps (%s)", processedKeys.incrementAndGet(), srcCols.size(), locatorLongRow.getKey(), processedKeys.get() / ((nowInMilliSeconds() - startClockTime)/1000f), Thread.currentThread().getName()));

                        try {
                            batch.execute();
                            if (srcCols.size() > 0 && random.nextFloat() < verifyPercent) {
                                verifyExecutor.submit(new Runnable() {public void run() {
                                    try {
                                        ColumnList<String> srcData = srcKeyspace.prepareQuery(columnFamily)
                                                .getKey(locatorLongRow.getKey())
                                                .execute()
                                                .getResult();
                                        ColumnList<String> dstData = dstKeyspace.prepareQuery(columnFamily)
                                                .getKey(locatorLongRow.getKey())
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
                                        stopAll.set(true);
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
            Boolean result = new AllRowsReader.Builder<Locator, String>(srcKeyspace, columnFamily)
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

    private static void checkSameResults(String locator, ColumnList<String> x, ColumnList<String> y) throws Exception {

        if (x.size() != y.size()) {
            throw new Exception("source and destination column lengths do not match");
        }

        if (Sets.difference(new HashSet<String>(x.getColumnNames()), new HashSet<String>(y.getColumnNames())).size() != 0) {
            throw new Exception("source and destination did not contain the same column names");
        }

        for (int i = 0; i < y.size(); i++) {
            String src = x.getColumnByIndex(i).getValue(StringMetadataSerializer.get());
            String dst = y.getColumnByIndex(i).getValue(StringMetadataSerializer.get());
            if (!src.equals(dst)) {
                throw new Exception(String.format("source and destination metadata value for %s does not match for locator %s. Value found at src: %s & dst: %s", x.getColumnByIndex(i).getName(), locator, src, dst));
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
            CommandLine line = parser.parse(cliOptions, args);

            options.put(DST_CLUSTER, line.getOptionValue(DST_CLUSTER));
            options.put(SRC_CLUSTER, line.getOptionValue(SRC_CLUSTER));
            options.put(KEYSPACE, line.hasOption(KEYSPACE) ? line.getOptionValue(KEYSPACE) : "DATA");

            options.put(BATCH_SIZE, line.hasOption(BATCH_SIZE) ? Integer.parseInt(line.getOptionValue(BATCH_SIZE)) : 100);
            options.put(READ_THREADS, line.hasOption(READ_THREADS) ? Integer.parseInt(line.getOptionValue(READ_THREADS)) : 1);
            options.put(WRITE_THREADS, line.hasOption(WRITE_THREADS) ? Integer.parseInt(line.getOptionValue(WRITE_THREADS)) : 1);
            options.put(VERIFY, line.hasOption(VERIFY) ? Double.parseDouble(line.getOptionValue(VERIFY)) : 0.075d);
            options.put(RATE, line.hasOption(RATE) ? Integer.parseInt(line.getOptionValue(RATE)) : 200);
        } catch (ParseException ex) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("bf-migrate", cliOptions);
            System.exit(-1);
        }

        return options;
    }
}
