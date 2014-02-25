package com.rackspacecloud.blueflood.service;

import com.rackspacecloud.blueflood.io.ManualRollup;

public class RunRollupTool {

    public static void main(String args[]) {
        Configuration conf = Configuration.getInstance();
        String newPrefix = conf.getStringProperty("GRAPHITE_HOST") + ".rollupTool";
        System.setProperty(conf.getStringProperty("GRAPHITE_HOST").toString(), newPrefix);
        ManualRollup rollupTool = new ManualRollup();
        rollupTool.startManualRollup();
    }
}
