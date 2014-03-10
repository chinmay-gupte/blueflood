package com.rackspacecloud.blueflood.backfiller;


import com.rackspacecloud.blueflood.backfiller.download.CloudFilesManager;
import com.rackspacecloud.blueflood.backfiller.download.DownloadService;
import com.rackspacecloud.blueflood.backfiller.download.FileManager;
import com.rackspacecloud.blueflood.backfiller.download.NewFileListener;
import com.rackspacecloud.blueflood.backfiller.handlers.FileListener;
import com.rackspacecloud.blueflood.backfiller.handlers.Rollup5mGenerator;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

public class CloudFilesBackFiller {
    private static final Logger log = LoggerFactory.getLogger(CloudFilesBackFiller.class);

    public static void main(String args[]) {
        //final String USER = System.getProperty("CLOUDFILES_USER");
        //final String KEY = System.getProperty("CLOUDFILES_KEY");
        System.setProperty("log4j.configuration", "file:/Users/chinmaygupte/Repo/forked_repos/blueflood/blueflood-log4j.properties");
        String log4jConfig = System.getProperty("log4j.configuration");
        if (log4jConfig != null && log4jConfig.startsWith("file:")) {
            System.out.println("Found log4jconfig as "+log4jConfig);
            PropertyConfigurator.configureAndWatch(log4jConfig.substring("file:".length()), 5000);
        }
        final String PROVIDER = "cloudfiles-us";
        final String ZONE = "IAD";
        //final String CONTAINER = System.getProperty("CLOUDFILES_CONTAINER");
        final String USER = "";
        final String KEY = "";

        final File downloadDir = new File("/tmp/metrics_gzipped");
        downloadDir.mkdirs();

        // connect the download service to the file manager.
        FileManager fileManager = new CloudFilesManager(USER, KEY, PROVIDER, ZONE, "metric-data", 5);
        DownloadService downloadService = new DownloadService(downloadDir);
        downloadService.setFileManager(fileManager);

        NewFileListener newFileListener = new FileListener();

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

        // This is to be used only for testing already parsed files. In order to generate the rollups on the fly, the only option as of know, is to start again from the beginning of replay period
        for (File gz : downloadDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".json.gz");
            }
        })) {
            log.info("Parsing existing file {}", gz.getName());
            newFileListener.fileReceived(gz);
        }

        // connect the file manager to the file parser listener
        fileManager.addNewFileListener(newFileListener);

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
