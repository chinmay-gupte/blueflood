package com.rackspacecloud.blueflood.backfiller.handlers;

import com.rackspacecloud.blueflood.backfiller.download.NewFileListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

public class FileListener implements NewFileListener {

    private static final Logger log = LoggerFactory.getLogger(FileListener.class);

    private final ExecutorService parserThreadPool = Executors.newFixedThreadPool(1);

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
                    // release dat lock.
                    storeBuilder.close();
                }
                return f;
            }
        });

    }
}
