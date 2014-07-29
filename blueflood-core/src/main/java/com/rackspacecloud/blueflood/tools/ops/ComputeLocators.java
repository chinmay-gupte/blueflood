package com.rackspacecloud.blueflood.tools.ops;

import com.google.common.base.Supplier;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.rackspacecloud.blueflood.io.CassandraModel;
import com.rackspacecloud.blueflood.types.Locator;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// todo: we need a similar thing that will generate locators from metrics_locator.
// E.G.:  [LOCATOR][TAB][NOW_MILLIS][COMMA][ZERO][COMMA][NOW_DATE_STRING][0x09]

public class ComputeLocators {
    
    private static final PrintStream out = System.out;
    
    private static final Options cliOptions = new Options();
    
    private static final String FILE = "file";
    private static final String KEYSPACE = "keyspace";
    private static final String COLUMN_FAMILY = "cf";
    private static final String CASS_VERSION = "version";
    private static final String HOST = "hosts";
    private static final String SPLITS = "splits";
    private static final int EOL = (int)'\n';
    private static final int TAB = (int)0x09;
    
    
    static {
        cliOptions.addOption(OptionBuilder.isRequired().hasArg().withDescription("File to write locators to").create(FILE));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg().withDescription("Keyspace to read locators from").create(KEYSPACE));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("Column family to read locators from (default=metrics_1440m").create(COLUMN_FAMILY));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("Cassandra version (default=1.0)").create(CASS_VERSION));
        cliOptions.addOption(OptionBuilder.isRequired().hasArg().withDescription("[required] A cassandra host to read from (host:port").create(HOST));
        cliOptions.addOption(OptionBuilder.hasArg().withDescription("Number of key ranges to split keyspace into for queries (default=1000)").create(SPLITS));
    }
    
    public static void main(String args[]) {
        nullRouteAllLog4j();
        
        Map<String, Object> options = parseOptions(args);
        
        AstyanaxContext<Keyspace> context = connect(
                options.get(HOST).toString(),
                options.get(KEYSPACE).toString(), 
                options.get(CASS_VERSION).toString()
        );
        Keyspace keyspace = context.getEntity();
        
        int numRanges = 0;
        int maxRowsSeen = 0;
        long start = System.currentTimeMillis();
        long locatorCount = 0;
        
        try {
            final ColumnFamily<Locator, Long> columnFamily = (ColumnFamily<Locator, Long>)options.get(COLUMN_FAMILY);
            final OutputStream locatorStream = new FileOutputStream((File)options.get(FILE), false);
            Iterable<Pair<String, String>> splits = splitTokens((Integer)options.get(SPLITS));
            for (Pair<String, String> range : splits) {
        
                int bestCountGuess = numRanges > 10 ? maxRowsSeen * 2 : 5000;
                Rows<Locator, Long> rows;
                while (true) {
                    double rate = (double)locatorCount / (double)(Math.max(1d, (double)((System.currentTimeMillis()-start)/1000)));
                    out.println(String.format("trying (%s,%s) with %d (%.2f locators/sec)", range.left, range.right, bestCountGuess, rate));
                    rows = keyspace.prepareQuery(columnFamily)
                            .setConsistencyLevel(ConsistencyLevel.CL_ONE)
                            .getRowRange(null, null, range.left, range.right, bestCountGuess)
                            .execute()
                            .getResult();
                    if (rows.size() < bestCountGuess) {
                        // got them all.
                        break;
                    } else {
                        // try again.
                        bestCountGuess *= 2;
                        out.println(String.format("will retry (%s,%s)", range.left, range.right));
                    }
                }
                
                numRanges += 1;
                maxRowsSeen = Math.max(maxRowsSeen, rows.size());
                
                if (rows.size() == 0) {
                    continue; // it happens. remember: we're scanning.
                }
                
                for (Locator locator : rows.getKeys()) {
                    try {
                        locatorCount += 1;
                        
                        ColumnList<Long> cols = rows.getRow(locator).getColumns();
                        Collection<Long> timestamps = cols.getColumnNames();
                        long maxTimestamp = timestamps.size() > 0 ? Collections.max(timestamps) : 0L;
                        String details = String.format("%d,%d,%s", maxTimestamp, cols.size(), new Date(maxTimestamp).toString().replace(",","_"));
                        
                        locatorStream.write(locator.toString().getBytes());
                        locatorStream.write(TAB);
                        locatorStream.write(details.getBytes());
                        locatorStream.write(EOL);
                        locatorStream.flush();
                    } catch (IOException ex) {
                        throw new IOError(ex);
                    }
                }
            }
        } catch (FileNotFoundException ex) {
            out.println(ex.getMessage());
            System.exit(-1);
        } catch (ConnectionException ex) {
            ex.printStackTrace(out);
            System.exit(-1);
        } finally {
            context.shutdown();
        }
    }
    
    private static AstyanaxContext<Keyspace> connect(String hostPort, String keyspace, String cassVersion) {
        final List<Host> hosts = new ArrayList<Host>();
        int port = Integer.parseInt(hostPort.split(":", -1)[1]);
        hosts.add(new Host(Host.parseHostFromHostAndPort(hostPort), port));
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
                        .setConnectionPoolType(ConnectionPoolType.BAG)
                        .setTargetCassandraVersion(cassVersion)
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(keyspace)
                        .setMaxConns(hosts.size())
                        .setMaxConnsPerHost(1)
                        .setConnectTimeout(2000)
                        .setSocketTimeout(5000 * 100)
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
        return context;
    }
    
    private static Map<String, Object> parseOptions(String[] args) {
        final GnuParser parser = new GnuParser();
        final Map<String, Object> options = new HashMap<String, Object>();
        try {
            CommandLine line = parser.parse(cliOptions, args);
            
            // ensure file exists and can be made.
            File outputFile = new File(line.getOptionValue(FILE));
            if (!outputFile.getParentFile().exists()) {
                if (!outputFile.getParentFile().mkdirs()) {
                    throw new RuntimeException("Could not create output file");
                }
            }
            
            // create a mapping of all cf names -> cf.
            // then determine which column family to process.
            String cfName = line.hasOption(COLUMN_FAMILY) ? line.getOptionValue(COLUMN_FAMILY) : "metrics_1440m";
            Map<String, ColumnFamily<Locator, Long>> nameToCf = new HashMap<String, ColumnFamily<Locator, Long>>() {{
                for (CassandraModel.MetricColumnFamily cf : CassandraModel.getMetricColumnFamilies()) {
                    put(cf.getName(), cf);
                }
            }};
            if (nameToCf.get(cfName) == null) {
                throw new ParseException("Invalid column family");
            }
            CassandraModel.MetricColumnFamily columnFamily = (CassandraModel.MetricColumnFamily)nameToCf.get(cfName);
            
            options.put(FILE, outputFile);
            options.put(KEYSPACE, line.getOptionValue(KEYSPACE));
            options.put(COLUMN_FAMILY, columnFamily);
            options.put(CASS_VERSION, line.hasOption(CASS_VERSION) ? line.getOptionValue(CASS_VERSION) : "1.0");
            options.put(HOST, line.getOptionValue(HOST));
            options.put(SPLITS, line.hasOption(SPLITS) ? Integer.parseInt(line.getOptionValue(SPLITS)) : 1000);
            
        } catch (ParseException ex) {
            ex.printStackTrace(out);
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("compute-locators", cliOptions);
            System.exit(-1);
        }
        
        return options;
    }
    
    private static void nullRouteAllLog4j() {
        List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
        loggers.add(LogManager.getRootLogger());
        for ( Logger logger : loggers ) {
            logger.setLevel(Level.OFF);
        }
    }
    
    private static final BigInteger MAX_TOKEN = new BigInteger("42535295865117307932921825928971026431");
    private static Iterable<Pair<String, String>> splitTokens(int count) {
        List<Pair<String, String>> list = new ArrayList<Pair<String, String>>();
        
        BigInteger[] parts = MAX_TOKEN.divideAndRemainder(new BigInteger(Integer.toString(count)));
        BigInteger cur = new BigInteger("0");
        
        
        for (int i = 0; i < count; i++) {
            BigInteger start = new BigInteger(cur.toString(10));
            BigInteger end = new BigInteger(start.toString(10));
            end = end.add(parts[0]);
            
            list.add(Pair.create(start.toString(), end.toString()));
            
            cur = end;
        }
        
        if (parts[1].intValue() > 0) {
            list.add(Pair.create(cur.toString(), MAX_TOKEN.toString()));
        }
        
        return list;
    }
}
