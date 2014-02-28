package com.rackspacecloud.blueflood.backfiller.download;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DownloadService {
    private static final Logger log = LoggerFactory.getLogger(DownloadService.class);
    private static final int MAX_FILES_IN_DIR = 100;
    private static final int MAX_UNEXPECTED_ERRORS = 5;

    
    private final File downloadDir;
    private final Thread thread;
    private final Lock downloadLock = new ReentrantLock(true);
    private FileManager fileManager = null; // todo: should be final.
    
    private boolean running = false;
    private boolean terminated = false;
    
    private int unexpectedErrors = 0;
    
    
    public DownloadService(final File downloadDir) {
        this.downloadDir = downloadDir;
        this.running = false;
        this.thread = new Thread("Download Service") {
            public void run() {
                while (!terminated) {
                    // if there are > 1 tmp files in the dir, just wait.
                    FilenameFilter filter = new FilenameFilter() {
                        public boolean accept(File dir, String name) {
                            return name.endsWith(".json.gz.tmp");
                        }
                    };
                    //This might be redundant. If the lock has been acquired back, there would be no tmp filed *ideally* Sleeping in the finally block of lock acquiring code makes more sense
                    while (downloadDir.listFiles(filter).length > 1) {
                        try { 
                            sleep(200L);
                        } catch (InterruptedException ex) {
                            Thread.interrupted(); // clears interrupt for tidiness sake.
                        }
                    }
                    doCheck();
                }
                log.debug("Download service thread stopping");
            }
        };
        this.thread.start();
    }
    
    public void setFileManager(FileManager newFileManager) {
        downloadLock.lock();
        try {
            fileManager = newFileManager;
        } finally {
            downloadLock.unlock();
        }
    }
    
    public synchronized void start() throws IOException {
        if (terminated)
            throw new IOException("Download service has been terminated. It cannot be restarted.");
        if (running)
            throw new IOException("Download service is already running.");
        if (!downloadDir.exists())
            throw new IOException("Download directory does not exist");
        
        log.info("Resuming downloads");
        running = true;
        thread.interrupt();
    }
    
    public synchronized void stop() {
        log.info("Stopping download service");
        running = false;
        thread.interrupt();
    }
    
    public synchronized void terminate(boolean waitFor) {
        terminated = true;
        stop();
        log.info("Terminating download service");
        thread.interrupt();
        if (waitFor) {
            log.info("Wating for download service termination");
            while (thread.isInterrupted() || thread.isAlive()) {
                try { Thread.sleep(100); } catch (Exception ex) {}
            }
            log.info("Download service terminated");
        }
    }
    
    // gets run by the thread.
    private void doCheck() {
        if (!running) return;
        if (fileManager == null) return;
        
        if (unexpectedErrors > MAX_UNEXPECTED_ERRORS) {
            log.info("Terminating because of errors");
            terminate(false);
            return;
        }
        
        // safety valve. Possible infinite thread sleep? This will make sure we fire downloading only when are the files are consumed/merged
        while (downloadDir.listFiles().length != 0) {
            log.debug("Too many queued files; sleeping for 5m");
            try { Thread.sleep(5 * 60 * 1000); } catch (Exception ex) {}
        }
        
        if (downloadLock.tryLock()) {
            try {
                if (fileManager.hasNewFiles()) {
                    fileManager.downloadNewFiles(downloadDir);
                }
            } catch (Throwable unexpected) {
                unexpectedErrors += 1;
                log.error("UNEXPECTED; WILL TRY TO RECOVER");
                log.error(unexpected.getMessage(), unexpected);
                // sleep for a minute?
                if (Thread.interrupted()) {
                    try {
                        thread.sleep(60000);
                    } catch (Exception ex) {
                        log.error(ex.getMessage(), ex);
                    }
                }
            } finally {
                downloadLock.unlock();
            }
        } else {
            log.debug("Download in progress");
        }
    }
}
