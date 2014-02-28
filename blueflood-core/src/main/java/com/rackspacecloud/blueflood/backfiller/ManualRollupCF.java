package com.rackspacecloud.blueflood.backfiller;


import com.rackspacecloud.blueflood.backfiller.download.CloudFilesManager;
import com.rackspacecloud.blueflood.backfiller.download.DownloadService;
import com.rackspacecloud.blueflood.backfiller.download.FileManager;
import com.rackspacecloud.blueflood.backfiller.handlers.FileListener;
import com.rackspacecloud.blueflood.backfiller.handlers.Rollup5mGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

public class ManualRollupCF {

    private static final Logger log = LoggerFactory.getLogger(ManualRollupCF.class);

    public static void main(String args[]) {
        final String USER = System.getProperty("CLOUDFILES_USER");
        final String KEY = System.getProperty("CLOUDFILES_KEY");
        final String PROVIDER = "cloudfiles-us";
        final String ZONE = "IAD";
        final String CONTAINER = System.getProperty("CLOUDFILES_CONTAINER");

        final File downloadDir = new File("/tmp/metrics_gzipped");
        downloadDir.mkdirs();

        // connect the download service to the file manager.
        FileManager fileManager = new CloudFilesManager(USER, KEY, PROVIDER, ZONE, CONTAINER, 10);
        DownloadService downloadService = new DownloadService(downloadDir);
        downloadService.setFileManager(fileManager);


        // delete any temp files before starting.
        for (File tmp : downloadDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".json.gz.tmp");
            }
        })) {
            if (!tmp.delete()) {
                log.error("Could not delete a temp file");
                System.exit(-1);
            }
        }

        // connect the file manager to the file parser listener
        fileManager.addNewFileListener(new FileListener());

        try {
            downloadService.start();
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(-1);
        }

        // Start the 5m rollup generator thread
        Thread rollup5mGenerator = new Thread(new Rollup5mGenerator());
        rollup5mGenerator.start();

    }
}
