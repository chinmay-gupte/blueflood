package com.rackspacecloud.blueflood.backfiller.download;

import java.io.File;

public interface NewFileListener {
    public void fileReceived(File f);
}
