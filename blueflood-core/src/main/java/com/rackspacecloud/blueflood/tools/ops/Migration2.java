package com.rackspacecloud.blueflood.tools.ops;

import com.google.common.base.Joiner;
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
import com.netflix.astyanax.connectionpool.exceptions.OperationTimeoutException;
import com.netflix.astyanax.connectionpool.exceptions.TimeoutException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
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

import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Migration2 {
    
    private static final String SAME = "SAME".intern();
    private static final String RENEW = "RENEW".intern();
    private static final String NONE = "NONE".intern();
    private static final String DO_NOT_RESUME = "DO NOT RESUME".intern();
    private static final int CONCURRENCY_FACTOR = 2;
    private static final int ADDITIONAL_CONNECTIONS_PER_HOST = 6;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    
    private static final PrintStream out = System.out;
    private final static Random random = new Random(System.nanoTime());
    
    private static final Options cliOptions = new Options();
    
    private static final String FILE = "file";
    
    private static final String DST_CLUSTER = "dcluster";
    private static final String DST_KEYSPACE = "dkeyspace";
    private static final String DST_CF = "dfamily";
    private static final String DST_VERSION = "dver";
    
    private static final String SRC_CLUSTER = "scluster";
    private static final String SRC_KEYSPACE = "skeyspace";
    private static final String SRC_CF = "sfamily";
    private static final String SRC_VERSION = "sver";
    
    private static final String FROM = "from";
    private static final String TO = "to";
    
    private static final String MAX_ROWS = "rows";
    private static final String TTL_SECONDS = "ttl";
    private static final String RATE_PER_MINUTE = "rate";
    
    private static final String CONCURRENCY = "concurrency";
    private static final String RESUME = "resume";
    private static final String VERIFY = "verify";
    private static final String VERBOSE = "verbose";
    
    static {
        cliOptions.addOption(OptionBuilder.hasArg().isRequired().withDescription("Location of locator file").create(FILE));
        cliOptions.addOption(OptionBuilder.hasArg().isRequired().withValueSeparator(',').withDescription("comma delimited list of host:port of cassandra cluster to write to").create(DST_CLUSTER));
        cliOptions.addOption(OptionBuilder.hasArg().isRequired().withValueSeparator(',').withDescription("comma delimited list of host:port of cassandra cluster to read from").create(SRC_CLUSTER));
        cliOptions.addOption(OptionBuilder.hasArg().isRequired().withLongOpt("Destination column family to migrate").create(DST_CF));
        cliOptions.addOption(OptionBuilder.hasArg().isRequired().withLongOpt("Source column family to migrate").create(SRC_CF));

        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] ISO 6801 datetime (or millis since epoch) of when to start migrating data. defaults to one year ago.").create(FROM));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] ISO 6801 datetime (or millis since epoch) Datetime of when to stop migrating data. defaults to right now.").create(TO));
        
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Destination keyspace (default=data)").create(DST_KEYSPACE));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Destination cassandra version (default=2.0)").create(DST_VERSION));
        
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Source keyspace (default=DATA)").create(SRC_KEYSPACE));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Source cassandra version (default=1.0)").create(SRC_VERSION));
        
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Maximum number of rows to copy (default=INFINITY)").create(MAX_ROWS));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] TTL in seconds for copied columns (default=SAME), {SAME | RENEW | NONE}").create(TTL_SECONDS));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Maximum rate of transfer, in rows per minute (default=Integer.MAX_VALUE)").create(RATE_PER_MINUTE));
        
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Number of read/write threads to use (default=1)").create(CONCURRENCY));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Locator to resume processing at.").create(RESUME));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Percentage of copies to verify by reading (0..1) (default=0.005)").create(VERIFY));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("[optional] Include all output (default=true)").create(VERBOSE));
    }
    
    public static void main(String args[]) {
        nullRouteAllLog4j();
        
        Map<String, Object> options = parseOptions(args);
        
        // establish column range.
        final ByteBufferRange range = new RangeBuilder()
            .setStart((Long) options.get(FROM))
            .setEnd((Long) options.get(TO)).build();
        
        // read all locators into memory.
        try {
            final boolean verbose = (Boolean)options.get(VERBOSE);
            
            out.println(String.format("Will migrate %s/%s/%s to %s/%s/%s for time %s to %s",
                    options.get(SRC_CLUSTER),
                    options.get(SRC_KEYSPACE),
                    options.get(SRC_VERSION),
                    options.get(DST_CLUSTER),
                    options.get(DST_KEYSPACE),
                    options.get(DST_VERSION),
                    TIMESTAMP_FORMAT.format(new Date((Long)options.get(FROM))),
                    TIMESTAMP_FORMAT.format(new Date((Long)options.get(TO)))
            ));
            out.println(String.format("Verbose is %b", verbose));
            
            out.print(String.format("Reading and sorting locators from %s...", ((File)options.get(FILE)).getAbsolutePath()));
            Collection<StringLocator> locators = readLocators((File)options.get(FILE));
            out.println(String.format("done (%d)", locators.size()));
            
            AstyanaxContext<Keyspace> srcContext = connect(
                    options.get(SRC_CLUSTER).toString(),
                    options.get(SRC_KEYSPACE).toString(),
                    options.get(SRC_VERSION).toString(),
                    (Integer)options.get(CONCURRENCY)
            );
            final Keyspace srcKeyspace = srcContext.getEntity();
            final CassandraModel.MetricColumnFamily srcCf = (CassandraModel.MetricColumnFamily)options.get(SRC_CF);
            
            AstyanaxContext<Keyspace> dstContext = connect(
                    options.get(DST_CLUSTER).toString(),
                    options.get(DST_KEYSPACE).toString(),
                    options.get(DST_VERSION).toString(),
                    (Integer)options.get(CONCURRENCY)
            );
            final Keyspace dstKeyspace = dstContext.getEntity();
            final CassandraModel.MetricColumnFamily dstCf = (CassandraModel.MetricColumnFamily)options.get(DST_CF);
            
            final int maxRowsPerMinute = (Integer)options.get(RATE_PER_MINUTE);
            final int maxRows = (Integer)options.get(MAX_ROWS);
            final AtomicInteger movedRowCount = new AtomicInteger(0);
            final AtomicInteger emptyRowCount = new AtomicInteger(0);
            final AtomicInteger absoluteRowCount = new AtomicInteger(0);
            final int concurrency = (Integer)options.get(CONCURRENCY);
            final double verifyPercent = (Double)options.get(VERIFY); 
            
            final String ttl = options.get(TTL_SECONDS).toString(); 
            final ThreadPoolExecutor copyThreads = new ThreadPoolExecutor(
                    concurrency, concurrency,
                    0L, TimeUnit.MILLISECONDS, 
                    // this means that we'll be consuming concurrency+1 maximum connections to the cluster.
                    new LinkedBlockingQueue<Runnable>(concurrency),
                    Executors.defaultThreadFactory(),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );
            final AtomicBoolean breakSignal = new AtomicBoolean(false);
            String resumeAt = options.get(RESUME).toString();
            boolean skipping = resumeAt != DO_NOT_RESUME;
            
            final int numLocators = locators.size();
            final long startSeconds = System.currentTimeMillis() / 1000;
            
            for (StringLocator sl : locators) {
                
                // skip to resume;
                if (skipping) {
                    if (sl.locator.equals(resumeAt)) {
                        skipping = false;
                        out.println("Resuming at " + resumeAt);
                    } else {
                        continue;
                    }
                }
                
                if (breakSignal.get()) {
                    break;
                }
                
                final String locatorString = sl.locator;
                final long locatorStamp = sl.timestamp;
                copyThreads.submit(new Runnable() {
                    public void run() {

                        // don't bother if we're being shut down.
                        if (breakSignal.get()) {
                            return;
                        }

                        // calculate current rows per minute.
                        double runningMinutes = (double) ((System.currentTimeMillis() / 1000) - startSeconds) / 60d;
                        double rowsPerMinute = (double) movedRowCount.get() / runningMinutes;

                        // wait until rate limiting is over.
                        while (rowsPerMinute > maxRowsPerMinute && movedRowCount.get() > 0) {
                            //out.println(String.format("%.2f > %d", rowsPerMinute, maxRowsPerMinute));
                            try {
                                Thread.currentThread().sleep(1000);
                            }
                            catch (Exception ex) {
                            }

                            runningMinutes = (double) ((System.currentTimeMillis() / 1000) - startSeconds) / 60d;
                            rowsPerMinute = (double) movedRowCount.get() / runningMinutes;
                        }

                        // copy this locator.
                        Locator locator = Locator.createLocatorFromDbKey(locatorString);
                        int copiedCols = 0;
                        int copyTries = 3;
                        while (copyTries > 0) {
                            try {
                                copiedCols = copyWithVerify(locator, srcKeyspace, dstKeyspace, srcCf, dstCf, range, ttl, random.nextDouble() < verifyPercent);
                                break;
                            } catch (TimeoutException ex) {
                                out.println("Copy timed out. sleeping.");
                                try { Thread.currentThread().sleep(5000); } catch (Exception ignore) {}
                            } catch (ConnectionException ex) {
                                // verification failed. stop processing.
                                out.println("SIGNALING: Copy failure");
                                breakSignal.set(true);
                                break;
                            } finally {
                                copyTries -= 1;    
                            }
                        } 
                        int rowId = 0;
                        int numEmptyRows = 0;
                        int absoluteRowId = absoluteRowCount.incrementAndGet();
                        if (copiedCols > 0) {
                            rowId = movedRowCount.incrementAndGet();
                            numEmptyRows = emptyRowCount.get();
                        } else {
                            numEmptyRows = emptyRowCount.incrementAndGet();
                        }
                        if (verbose || copiedCols > 0) {
                            out.println(String.format("%d/%d/%d moved %d cols for locator %s last seen %s %d empties %.2f rpm",
                                    rowId, absoluteRowId, numLocators, copiedCols, locator.toString(), DATE_FORMAT.format(new Date(locatorStamp)), numEmptyRows, rowsPerMinute));
                        }

                        if (movedRowCount.get() >= maxRows) {
                            out.println("SIGNALING: Reached max rows " + Thread.currentThread().getName());
                            breakSignal.set(true);
                        }
                    }
                });

            }
            
            while (copyThreads.getQueue().size() > 0) {
                try { Thread.currentThread().sleep(1000); } catch (Exception ex) {};
            }

            out.print("shutting down...");
            copyThreads.shutdown();
            try {
                boolean clean = copyThreads.awaitTermination(10, TimeUnit.MINUTES);
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
            
            out.println("done");
            out.println(String.format("%d moved, %d empties, %d counted, total %d of %d",
                    movedRowCount.get(), emptyRowCount.get(), absoluteRowCount.get(), movedRowCount.get() + emptyRowCount.get(), locators.size()));
            
        } catch (IOException ex) {
            ex.printStackTrace(out);
            System.exit(-1);
        }
    }
    
    private static int checkSameResults(ColumnList<Long> source, ColumnList<Long> dest) throws Exception {
        // source size should not be bigger than dest.
        // a 1 col delta is ok to account for a single TTL expiration.
        if (source.size() - dest.size() > 1) {
            throw new Exception(String.format("some columns were missed. source:%d, dest:%d", source.size(), dest.size()));
        }
        
        // remove the difference(dest, source) from dest.
        Set<Long> compareCols = Sets.intersection(
                new HashSet<Long>(source.getColumnNames()),
                new HashSet<Long>(dest.getColumnNames()));
        
        List<Long> sortedCols = new ArrayList<Long>(compareCols);
        Collections.sort(sortedCols);
        
        if (sortedCols.size() == 0) {
            throw new Exception("Nothing in the intersection of columns.");
        }
        
        // verify that values match.
        for (Long colName : sortedCols) {
            if (!bytesSame(source.getColumnByName(colName).getByteArrayValue(), dest.getColumnByName(colName).getByteArrayValue())) {
                // bail here.
                throw new Exception("source and destination column values did not match for column " + colName);
            }
        }
        
        return sortedCols.size();
    }
    
    private static boolean bytesSame(byte[] bx, byte[] by) {
        if (bx.length != by.length) {
            return false;
        }
        // only examine every third byte.
        for (int j = 0; j < bx.length; j+=3) {
            if (bx[j] != by[j]) {
                return false;
            }
        }
        return true;
    }
    
    private static void dumpCompare(ColumnList<Long> source, ColumnList<Long> dest) {
        Set<Long> sourceColumnNames = new HashSet<Long>(source.getColumnNames());
        Set<Long> destColumnNames = new HashSet<Long>(dest.getColumnNames());
        
        Sets.SetView<Long> missingFromDest = Sets.difference(sourceColumnNames, destColumnNames);
        
        if (missingFromDest.size() > 0) {
            out.println(String.format("Missing from destination: %s", Joiner.on(",").join(missingFromDest)));
        }
        
        if (missingFromDest.size() == 0) {
            List<Long> sortedSourceColumnNames = new ArrayList<Long>(source.getColumnNames());
            List<Long> sortedDestColumnNames = new ArrayList<Long>(dest.getColumnNames());
            Collections.sort(sortedSourceColumnNames);
            Collections.sort(sortedDestColumnNames);
            Iterator<Long> sourceCols = sortedSourceColumnNames.iterator();
            Iterator<Long> destCols = sortedDestColumnNames.iterator();
            
            while (sourceCols.hasNext() && destCols.hasNext()) {
                long sourceCol = sourceCols.next();
                long destCol = destCols.next(); 
                String sourceValue = toString(source.getByteArrayValue(sourceCol, new byte[]{-1}));
                String destValue = toString(dest.getByteArrayValue(destCol, new byte[]{-1}));
                out.println(String.format("%d:%d 0x%s,0x%s",sourceCol, destCol, sourceValue, destValue));
            }
        }
    }
    
    private static String toString(byte[] buf) {
        StringBuilder sb = new StringBuilder();
        for (byte b : buf) {
            String s = Integer.toString(0x000000ff & b, 16);
            if (s.length() == 0)
                sb = sb.append(0);
            sb = sb.append(s);
        }
        return sb.toString();
    }
    
    private static boolean verifyWrite(Locator locator, ColumnList<Long> srcData, Keyspace dstKeyspace, CassandraModel.MetricColumnFamily dstCf, ByteBufferRange range) throws ConnectionException {
        int tries = 3;
        Exception problem = null;
        ColumnList<Long> srcDump = null, dstDump = null;
        boolean possibleTrouble = false;
        ColumnList<Long> dstData = null;
        while (tries > 0) {
            try {
                // take a little break. this gives data a chance to move around.
                Thread.currentThread().sleep(1000);
                
                dstData = dstKeyspace
                        .prepareQuery(dstCf)
                        .setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
                        .getKey(locator)
                        .withColumnRange(range)
                        .execute()
                        .getResult();
            
                int matches = checkSameResults(srcData, dstData);
                out.println(String.format("Verified%scopy for %s src:%d,dst:%d,ver:%d", possibleTrouble ? " (barely) " : " ", locator.toString(), srcData.size(), dstData.size(), matches));
                problem = null;
                break;
            } catch (OperationTimeoutException ex) {
                out.println("Verification timed out. sleeping");
                try { Thread.currentThread().sleep(5000); } catch (Exception ignore) {}
                // do not decrement tries.
                continue;
            } catch (Exception any) {
                problem = any;
                srcDump = srcData;
                dstDump = dstData;
                out.println(String.format("Verification trouble for %s. %s", locator.toString(), any.getMessage()));
                possibleTrouble = true;
            }
            tries--;
        }
        
        if (problem != null) {
            dumpCompare(srcDump, dstDump);
            return false;
        }
        
        return true;
    }
    
    // keep this method threadsafe!
    private static int copyWithVerify(Locator locator, Keyspace src, Keyspace dst, CassandraModel.MetricColumnFamily srcCf, CassandraModel.MetricColumnFamily dstCf, ByteBufferRange range, final String ttl, boolean verify) throws ConnectionException {
        // read row.
        ColumnList<Long> columnList = src
                .prepareQuery(srcCf)
                .setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
                .getKey(locator)
                .withColumnRange(range)
                .execute().getResult();
        
        // don't bother with empty rows.
        if (columnList.size() == 0) {
            return 0;
        }
        
        int safetyTtlInSeconds = 5 * (int)dstCf.getDefaultTTL().toSeconds();
        int nowInSeconds = (int)(System.currentTimeMillis() / 1000);
        
        // write row.
        MutationBatch batch = dst.prepareMutationBatch();
        ColumnListMutation<Long> mutation = batch.withRow(dstCf, locator);
        int colCount = 0;
        for (Column<Long> c : columnList) {
            if (ttl != NONE) {
                // ttl will either be the safety value or the difference between the safety value and the age of the column.
                int ttlSeconds = ttl == RENEW ? safetyTtlInSeconds : (safetyTtlInSeconds - nowInSeconds + (int)(c.getName()/1000));
                mutation.putColumn(c.getName(), c.getByteBufferValue(), ttlSeconds);
            } else {
                mutation.putColumn(c.getName(), c.getByteBufferValue());
            }
            colCount += 1;
        }
        batch.execute().getResult();
        
        
        if (colCount > 0 && verify) {
            if (!verifyWrite(locator, columnList, dst, dstCf, range)) {
                throw new ConnectionException("VERIFICATION FAILED for " + locator.toString()) {};
            }
        }
        return colCount;
    }
    
    // assume we have duplicate listings in the locator file. Some of them are more recent and therefore, more 
    // important. Remove the duplicates and return a sorted collection.
    private static Collection<StringLocator> readLocators(File f) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
        String line = reader.readLine();
        
        Map<String, StringLocator> locators = new HashMap<String, StringLocator>();
        
        while (line != null) {
            StringLocator locator = StringLocator.make(line);
            if (StringLocator.isValid(locator)) {
                if (!locators.containsKey(locator.locator)) {
                    locators.put(locator.locator, locator);
                } else {
                    StringLocator oldLocator = locators.get(locator.locator);
                    // we want the newest one.
                    if (oldLocator.timestamp < locator.timestamp) {
                        locators.put(locator.locator, locator);
                    } else {
                        locators.put(oldLocator.locator, oldLocator);
                    }
                }
            }
            
            line = reader.readLine();
        }
        
        ArrayList<StringLocator> allLocators = new ArrayList<StringLocator>(locators.values());
        Collections.sort(allLocators);
        // we want most recent first.
        Collections.reverse(allLocators);
        return allLocators;
    }
    
    // capture a locator + timestamp.
    private static class StringLocator implements Comparable<StringLocator> {
        private final String locator;
        private final long timestamp;
        
        public static boolean isValid(StringLocator locator) {
            if (locator.locator.contains(",")) {
                return false;
            }
            
            long sixMonthsAgo = System.currentTimeMillis() - (6L * 30L * 24L * 60L * 60L * 1000L);
            if (locator.timestamp < sixMonthsAgo) { // "is before six months ago"
                return false;
            }
            
            if (locator.locator.startsWith("null")) {
                return false;
            }
            
            return true;
        }
        
        public static StringLocator make(String string) {
            String[] bigParts = string.split("\\t", -1);
            String[] subParts = bigParts[1].split(",", -1);
            return new StringLocator(bigParts[0], Long.parseLong(subParts[0]));
        }
        
        private StringLocator(String locator, long timestamp) {
            this.locator = locator;
            this.timestamp = timestamp;
        }

        @Override
        public int hashCode() {
            // do not include timestamp.
            return locator.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) return false;
            if (!(obj instanceof StringLocator)) return false;
            StringLocator other = (StringLocator)obj;
            // do not include timestamp.
            return other.locator.equals(this.locator);
        }

        // standard ascending order comparator.
        @Override
        public int compareTo(StringLocator o) {
            long diff = timestamp - o.timestamp;
            if (diff == 0) {
                return locator.compareTo(o.locator);
            } else if (diff > Integer.MIN_VALUE && diff < Integer.MAX_VALUE) {
                return (int)diff;
            } else if (diff < 0) {
                return -1;
            } else if (diff > 0) {
                return 1;
            } else {
                throw new RuntimeException("Unexpected condition");
            }
        }
    }
    
    // shut log4j off.
    private static void nullRouteAllLog4j() {
        List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for ( Logger logger : loggers ) {
            logger.setLevel(Level.OFF);
        }
    }
    
    // connect to a database. Key features: 1 connection per host. If you want more connections, specify more hosts.
    // duplicates are ok.
    private static AstyanaxContext<Keyspace> connect(String clusterSpec, String keyspace, String version, int concurrency) {
        out.print(String.format("Connecting to %s:%s/%s...", clusterSpec, keyspace, version));
        final List<Host> hosts = new ArrayList<Host>();
        for (String hostSpec : clusterSpec.split(",", -1)) {
            hosts.add(new Host(Host.parseHostFromHostAndPort(hostSpec), Host.parsePortFromHostAndPort(hostSpec, -1)));
        }
        int maxConsPerHost = Math.max(1, concurrency / hosts.size()) + ADDITIONAL_CONNECTIONS_PER_HOST;
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
                        .setDefaultReadConsistencyLevel(ConsistencyLevel.CL_ONE)
                        .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                        .setTargetCassandraVersion(version)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(keyspace)
                        .setMaxConns(concurrency * CONCURRENCY_FACTOR)       // are these
                        .setMaxConnsPerHost(maxConsPerHost) // numbers safe?
                        .setConnectTimeout(2000)
                        .setSocketTimeout(5000 * 100)
                )
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
            
            // ensure file exists
            File locatorFile = new File(line.getOptionValue(FILE));
            if (!locatorFile.exists()) {
                throw new ParseException("Locator file does not exist");
            }
            
            // create a mapping of all cf names -> cf.
            // then determine which column family to process.
            Map<String, CassandraModel.MetricColumnFamily> nameToCf = new HashMap<String, CassandraModel.MetricColumnFamily>() {{
                for (CassandraModel.MetricColumnFamily cf : CassandraModel.getMetricColumnFamilies()) {
                    put(cf.getName(), cf);
                }
            }};
            
            CassandraModel.MetricColumnFamily srcCf = nameToCf.get(line.getOptionValue(SRC_CF));
            CassandraModel.MetricColumnFamily dstCf = nameToCf.get(line.getOptionValue(DST_CF));

            if (srcCf == null) {
                throw new ParseException("Invalid source column family");
            }
            if (dstCf == null) {
                throw new ParseException("Invalid destination column family");
            }
            
            List<String> validTtlStrings = Lists.newArrayList(SAME, RENEW, NONE);
            String ttlString = line.hasOption(TTL_SECONDS) ? line.getOptionValue(TTL_SECONDS) : SAME;
            if (!validTtlStrings.contains(ttlString)) {
                throw new ParseException("Invalid TTL: " + ttlString);
            }
            
            int maxRows = line.hasOption(MAX_ROWS) ? Integer.parseInt(line.getOptionValue(MAX_ROWS)) : Integer.MAX_VALUE;
            int ratePerMinute = line.hasOption(RATE_PER_MINUTE) ? Integer.parseInt(line.getOptionValue(RATE_PER_MINUTE)) : Integer.MAX_VALUE;
            int concurrency = line.hasOption(CONCURRENCY) ? Integer.parseInt(line.getOptionValue(CONCURRENCY)) : 1;
                    
            options.put(FILE, locatorFile);
            
            options.put(DST_CLUSTER, line.getOptionValue(DST_CLUSTER));
            options.put(DST_KEYSPACE, line.hasOption(DST_KEYSPACE) ? line.getOptionValue(DST_KEYSPACE) : "data");
            options.put(DST_CF, dstCf);
            options.put(DST_VERSION, line.hasOption(DST_VERSION) ? line.getOptionValue(DST_VERSION) : "2.0");
            
            options.put(SRC_CLUSTER, line.getOptionValue(SRC_CLUSTER));
            options.put(SRC_KEYSPACE, line.hasOption(SRC_KEYSPACE) ? line.getOptionValue(SRC_KEYSPACE) : "DATA");
            options.put(SRC_CF, srcCf);
            options.put(SRC_VERSION, line.hasOption(SRC_VERSION) ? line.getOptionValue(SRC_VERSION) : "1.0");
            
            // default range is one year ago until now.
            options.put(FROM, line.hasOption(FROM) ? parseDateTime(line.getOptionValue(FROM)) : now-(365L*24L*60L*60L*1000L));
            options.put(TO, line.hasOption(TO) ? parseDateTime(line.getOptionValue(TO)) : now);
            
            options.put(MAX_ROWS, maxRows);
            options.put(TTL_SECONDS, ttlString);
            options.put(RATE_PER_MINUTE, ratePerMinute);
            options.put(CONCURRENCY, concurrency);
            
            options.put(RESUME, line.hasOption(RESUME) ? line.getOptionValue(RESUME) : DO_NOT_RESUME);
            options.put(VERIFY, line.hasOption(VERIFY) ? Double.parseDouble(line.getOptionValue(VERIFY)) : 0.005d);
            options.put(VERBOSE, line.hasOption(VERBOSE) ? Boolean.parseBoolean(line.getOptionValue(VERBOSE)) : Boolean.TRUE);
            
        } catch (ParseException ex) {
            ex.printStackTrace(out);
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
