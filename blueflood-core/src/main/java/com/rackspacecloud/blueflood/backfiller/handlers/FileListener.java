package com.rackspacecloud.blueflood.backfiller.handlers;

import com.rackspacecloud.blueflood.backfiller.download.NewFileListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;

public class FileListener implements NewFileListener {

    private static final Logger log = LoggerFactory.getLogger(FileListener.class);
    private final ExecutorService parserThreadPool = Executors.newFixedThreadPool(5);
    private final ExecutorService deletionThreadPool = Executors.newFixedThreadPool(5);

    @Override
    public void fileReceived(final File f) {

        final Future<File> parseResult = parserThreadPool.submit(new Callable<File>() {
            public File call() throws Exception {
                log.info("Parsing {}", f.getAbsolutePath());
                BuildStore storeBuilder = BuildStore.getBuilder();
                try {
                    InputStream in = new GZIPInputStream(new FileInputStream(f), 0x00100000);
                    storeBuilder.merge(in);
                    in.close();
                    log.debug("Done parsing {}", f.getAbsolutePath());
                } finally {
                    storeBuilder.close();
                }
                return f;
            }
        });

        deletionThreadPool.submit(new Runnable() {
            public void run() {
                try {
                    File willDelete = parseResult.get();
                    log.debug("Removed " + willDelete.getAbsolutePath());
                    if (!willDelete.delete()) {
                        throw new ExecutionException(new IOException("Cannot delete file: " + willDelete.getAbsolutePath()));
                    }
                } catch (InterruptedException ex) {
                    // requeue
                    // TODO: there is a chance for infinite requeing. Set a finite threshold and stop the download service if it exceeds?
                    log.info("Requeueing {}", f.getAbsolutePath());
                    fileReceived(f);
                } catch (ExecutionException ex) {
                    // something happened during parsing.
                    log.error("Could not parse {} {}", f.getAbsolutePath(), ex);
                    try { Thread.sleep(30000L); } catch (Exception err) {}
                    log.info("Requeueing {}", f.getAbsolutePath());
                    fileReceived(f);
                }
            }
        });

    }
}
