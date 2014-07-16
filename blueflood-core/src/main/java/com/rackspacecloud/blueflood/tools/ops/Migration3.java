package com.rackspacecloud.blueflood.tools.ops;

import com.google.common.base.Function;
import com.google.common.collect.Sets;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.FixedRetryBackoffStrategy;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.retry.RetryNTimes;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;
import com.rackspacecloud.blueflood.io.AstyanaxIO;
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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Migration3 {

    private static final Options cliOptions = new Options();
    private static final double VERIFY_PERCENT = 0.005f; // half of one percent.

    private static final String SRC = "src";
    private static final String DST = "dst";
    private static final String FROM = "from";
    private static final String TO = "to";
    private static final String COLUMN_FAMILY = "cf";
    private static final String TTL = "ttl";
    private static final String WRITE_THREADS = "writethreads";
    private static final String READ_THREADS = "readthreads";
    private static final String BATCH_SIZE = "batchsize";
    private static final String VERIFY = "verify";
    private static final String DISCOVER = "discover";
    private static final String RATE = "rate";

    private static final PrintStream out = System.out;

    static {
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withValueSeparator(',').withDescription("[required] Source cassandra cluster (host:port:keyspace).").create(SRC));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg(true).withValueSeparator(',').withDescription("[required] Destination cassandra cluster (host:port:keyspace).").create(DST));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] ISO 6801 datetime (or millis since epoch) of when to start migrating data. defaults to one year ago.").create(FROM));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] ISO 6801 datetime (or millis since epoch) Datetime of when to stop migrating data. defaults to right now.").create(TO));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg().withValueSeparator(',').withDescription("[required] Which column family to migrate").create(COLUMN_FAMILY));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] ttl in seconds for new data. default=5x the default for the column family.").create(TTL));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] number of read threads to use. default=1").create(READ_THREADS));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] number of write threads to use. default=1").create(WRITE_THREADS));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] number of rows to read per query. default=100").create(BATCH_SIZE));
        cliOptions.addOption(OptionBuilder.withDescription("[optional] verify a sampling 0.5% of data copied").create(VERIFY));
        cliOptions.addOption(OptionBuilder.withDescription("[optional] discover and query other cassandra nodes").create(DISCOVER));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] maximum number of columns per/second to transfer. default=500").create(RATE));
    }

    private static long nowInSeconds() {
        return System.currentTimeMillis() / 1000;
    }

    public static void main(String args[]) {
        nullRouteAllLog4j();

        Map<String, Object> options = parseOptions(args);

        final int readThreads = (Integer)options.get(READ_THREADS);
        final int writeThreads = (Integer)options.get(WRITE_THREADS);
        final int batchSize = (Integer)options.get(BATCH_SIZE);
        final int ttl = (Integer)options.get(TTL);
        final int rate = (Integer)options.get(RATE);
        NodeDiscoveryType discovery = (NodeDiscoveryType)options.get(DISCOVER);

        // connect to src cluster.
        String[] srcParts = options.get(SRC).toString().split(":", -1);
        final AstyanaxContext<Keyspace> srcContext = connect(srcParts[0], Integer.parseInt(srcParts[1]), srcParts[2], readThreads, discovery);
        final Keyspace srcKeyspace = srcContext.getEntity();

        // connect to dst cluster.
        String[] dstParts = options.get(DST).toString().split(":", -1);
        final AstyanaxContext<Keyspace> dstContext = connect(dstParts[0], Integer.parseInt(dstParts[1]), dstParts[2], writeThreads, discovery);
        final Keyspace dstKeyspace = dstContext.getEntity();

        final AtomicLong columnsTransferred = new AtomicLong(0);
        final long startClockTime = nowInSeconds();

        // establish column range.
        final ByteBufferRange range = new RangeBuilder()
                .setStart((Long) options.get(FROM))
                .setEnd((Long) options.get(TO)).build();

        // create a threadpool that will write stuff into the destination.
        final ThreadPoolExecutor destWriteExecutor = new ThreadPoolExecutor(writeThreads, writeThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());

        // this threadpool performs verifications.
        final ThreadPoolExecutor verifyExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

        // keep track of the number of keys that have been copied.
        final AtomicInteger processedKeys = new AtomicInteger(0);

        // sentinal that indicates it is time to stop doing everything.
        final AtomicBoolean stopAll = new AtomicBoolean(false);

        final boolean verify = (Boolean)options.get(VERIFY);
        final Random random = new Random(System.nanoTime());

        // indicate what's going to happen.
        out.println(String.format("Will process metrics from %s to %s for dates %s to %s",
                options.get(SRC),
                options.get(DST),
                new Date((Long)options.get(FROM)),
                new Date((Long)options.get(TO))));

        final ColumnFamily<Locator, Long> columnFamily = (ColumnFamily<Locator, Long>)options.get(COLUMN_FAMILY);

        Function<Row<Locator, Long>, Boolean> rowFunction = new Function<Row<Locator, Long>, Boolean>() {

            @Override
            public Boolean apply(@Nullable final Row<Locator, Long> locatorLongRow) {

                if (locatorLongRow == null) {
                    out.println("Found a null row");
                    return true;
                }

                destWriteExecutor.submit(new Runnable() {
                    public void run() {

                        // copy the column.
                        MutationBatch batch = dstKeyspace.prepareMutationBatch();
                        ColumnListMutation<Long> mutation = batch.withRow(columnFamily,locatorLongRow.getKey());

                        assert ttl != 0;
                        long colCount = 0;
                        for (Column<Long> c : locatorLongRow.getColumns()) {
                            mutation.putColumn(c.getName(), c.getByteBufferValue(), ttl);
                            colCount += 1;
                        }

                        columnsTransferred.addAndGet(colCount);

                        out.println(String.format("%d copied %d for %s", processedKeys.incrementAndGet(), colCount, locatorLongRow.getKey()));

                        try {
                            batch.execute();
                            if (verify && random.nextFloat() < VERIFY_PERCENT) {
                                verifyExecutor.submit(new Runnable() {public void run() {
                                    try {
                                        ColumnList<Long> srcData = srcKeyspace.prepareQuery(columnFamily).getKey(locatorLongRow.getKey())
                                                .withColumnRange(range)
                                                .execute()
                                                .getResult();
                                        ColumnList<Long> dstData = dstKeyspace.prepareQuery(columnFamily).getKey(locatorLongRow.getKey())
                                                .withColumnRange(range)
                                                .execute()
                                                .getResult();

                                        checkSameResults(srcData, dstData);
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
                        } catch (ConnectionException ex) {
                            out.println("There was an error A: " + ex.getMessage());
                            ex.printStackTrace(out);
                            stopAll.set(true);
                        }
                    }
                });

                // This will apparently stop everything
                if(stopAll.get())
                    throw new RuntimeException();

                // this will throttle the threads of AllRowsReader
                /*
                while (columnsTransferred.get() / (nowInSeconds() - startClockTime-1) > rate) {
                    try { Thread.sleep(200); } catch (Exception ex) {}
                }
                */
                return true;
            }
        };

        try {
            new AllRowsReader.Builder<Locator, Long>(srcKeyspace, columnFamily)
                    .withColumnRange((Long) options.get(FROM), (Long) options.get(TO), false, Integer.MAX_VALUE)
                    .withPageSize(batchSize)
                    .withConcurrencyLevel(readThreads)
                    .withPartitioner(null)
                    .forEachRow(rowFunction)
                    .build()
                    .call();
        } catch (Exception e) {
            out.println("Error encountered E:" + e);
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void checkSameResults(ColumnList<Long> x, ColumnList<Long> y) throws Exception {
        if (x.size() != y.size()) {
            throw new Exception("source and destination column lengths do not match");
        }
        if (Sets.difference(new HashSet<Long>(x.getColumnNames()), new HashSet<Long>(y.getColumnNames())).size() != 0) {
            throw new Exception("source and destination did not contain the same column names");
        }

        for (int i = 0; i < x.size(); i++) {
            byte[] bx = x.getColumnByIndex(i).getByteArrayValue();
            byte[] by = y.getColumnByIndex(i).getByteArrayValue();
            if (bx.length != by.length) {
                throw new Exception("source and destination column values did not match for column " + i);
            }
            // only examine every third byte.
            for (int j = 0; j < bx.length; j+=3) {
                if (bx[j] != by[j]) {
                    throw new Exception("source and destination column values did not match for column " + i);
                }
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

    private static AstyanaxContext<Keyspace> connect(String host, int port, String keyspace, int threads, NodeDiscoveryType discovery) {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forKeyspace(keyspace)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(discovery)
                        .setRetryPolicy(new RetryNTimes(10)))

                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(host + ":" + keyspace)
                        .setMaxConns(threads * 2)
                        .setSeeds(host)
                        .setPort(port)
                        .setRetryBackoffStrategy(new FixedRetryBackoffStrategy(1000, 1000)))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
        return context;
    }

    // construct a well-formed options map. There should be no guesswork/checking for null after this point. All defaults
    // should be populated.
    private static Map<String, Object> parseOptions(String[] args) {
        final GnuParser parser = new GnuParser();
        final Map<String, Object> options = new HashMap<String, Object>();
        try {
            final long now = System.currentTimeMillis();
            CommandLine line = parser.parse(cliOptions, args);

            options.put(SRC, line.getOptionValue(SRC));
            options.put(DST, line.getOptionValue(DST));

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

            options.put(TTL, line.hasOption(TTL) ? Integer.parseInt(line.getOptionValue(TTL)) : (int)(5 * columnFamily.getDefaultTTL().toSeconds()));

            options.put(READ_THREADS, line.hasOption(READ_THREADS) ? Integer.parseInt(line.getOptionValue(READ_THREADS)) : 1);
            options.put(WRITE_THREADS, line.hasOption(WRITE_THREADS) ? Integer.parseInt(line.getOptionValue(WRITE_THREADS)) : 1);

            options.put(VERIFY, line.hasOption(VERIFY));

            options.put(DISCOVER, line.hasOption(DISCOVER) ? NodeDiscoveryType.RING_DESCRIBE : NodeDiscoveryType.NONE);
            options.put(RATE, line.hasOption(RATE) ? Integer.parseInt(line.getOptionValue(RATE)) : 500);

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
            // convert from a ISO 6801 date String.
            return DatatypeConverter.parseDateTime(s).getTime().getTime();
        }
    }
}


