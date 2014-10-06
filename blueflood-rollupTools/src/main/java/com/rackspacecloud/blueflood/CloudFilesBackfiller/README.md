## Cloud Files Backfiller

This tool will allow you to backfill the 5m rollups from the raw data backup on Rackspace Cloud Files. It is really helpful when you failed to ingest raw data for a certain time
range or you ingested data but it ttl'd out.

## Overview

The tool has 2 parts:

1. **RangeDownloader**: 
 
	As the name suggests, this class will kick off a process to start grabbing raw metric files from Rackspace Cloud Files. 

1. **OufOfBandRollup**: 

   This process keeps on listening to the path `DOWNLOAD_DIR` for the Cloud Files getting downloaded by the RangeDownloader.

## Details

### RangeDownloader Details
 
   * Cloud Files are lexicographically stored and they can be paginated using a marker value. The marker value is tracked in a file called as .last_marker and is assumed to be present on the path when the RangeDownloader is run. 
   * As the files start getting downloaded, RangeDownloader will keep on updating the last marker value and persisting it to the disk, so that the downloads can be resumed later.
   * If this file is not found, the RangeDownloader will keep on listing the metric files until it gets files within the supplied range. So it becomes necessary to set the marker value to be a little prior to when you want to start backfilling data, in order to avoid this redundant time spent on listing the files outside the backfill range.  
      * For example, to start backfilling from
1410217462000, we set the last marker value to 20140908_1400199246233_0.json.gz.
      * Note, the '.json.gz' file extension is an artifact of the way we name these backup files.  The format is `{data_ts_chunk.json.gz}`.  You can change this marker to account for the way your backup files format looks like.
   * The range within which we start downloading the files is set to 15 minutes before the `REPLAY_PERIOD_START` and 30 minutes after `REPLAY_PERIOD_STOP`. This buffer accounts for the delay incurred while pushing the metrics to Cloud Files. This has been hardcoded in CloudFilesManager and can be changed if you want a larger buffer period.

##### RangeDownloader Configuration Options

NOTE: All these settings need to be supplied in the blueflood config file which we use while running these tools. It helps to inherit a lot of settings like cassandra connection pool settings

   1. Cloud Files specific credentials like username, apikey, provider, zone and container to give the RangeDownloader the exact location in Rackspace Cloud Files to start grabbing data from
   1. `DOWNLOAD_DIR` -  specifies the location where you want to store the cloud files which are grabbed by RangeDownloader. Note that       this location will also be used later by the OutOfBandRollup.
   1. `BATCH_SIZE` - specified the the number of files which get fetched concurrently from the Cloud Files. This also determines the number of metrics from these files that get parsed by OutOfBandRollup in memory while calculating rollups on the fly, so determine the memory usage of the rollup generating process
   1. `REPLAY_PERIOD_START` - determines the start of the range within which you need to backfill 5m rollups.
   1. `REPLAY_PERIOD_STOP` - determines the end of the range within which you need to backfill 5m rollups.


### OufOfBandRollup Details

   * OutOfBandRollup parses CloudFiles into memory by mapping them to the 5m time slot within the replay period and ordering them in time. 
   * As the metrics start getting parsed, we start seeing new 5m time slots getting filled up. 
   * The order of operations for OutOfBandRollup is as follows:
      * buffer `NUMBER_OF_BUFFERRED_SLOTS` in memory
      * receive metrics belonging to slots beyond this buffer
      * grab the metrics from the "completed" time slots
      * roll them up to 5m metrics and pushes them to cassandra 
   * As further explanation, `NUMBER_OF_BUFFERRED_SLOTS` determines the memory usage in the form of number of parsed metrics we keep in memory and is a control knob that needs to be adjusted while running the tool by taking into consideration how much distributed are your metrics across the Cloud Files.  The more that metrics are distributed across cloud files, the more buffered slots you will keep in memory.
   * If we start seeing metrics greater than a certain threshold belonging to the range which has been already rolled, the tool will kill itself. In this case, this is one of the knobs you need to adjust.

##### OutOfBandRollups Configuration Options

   1. `SHARDS_TO_BACKFILL` - will backfill metrics belonging to only the supplied shards.
   2. `NUMBER_OF_BUFFERRED_SLOTS` - number of complete+incomplete slots to be kept in memory, once this number gets exceeded the head slots are grabbed, rollups calculated and pushed into cassandra.
   
    
## Examples:

1. RangeDownloader:

   `java -Dblueflood.config=file:/home/chinmay/bf-CFBackfiller.conf -Dlog4j.configuration=file:/home/chinmay/log4jCFDownloader.properties -Xms1G -Xmx2G -cp blueflood-rax-all-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.rackspacecloud.blueflood.backfiller.service.RangeDownloader`

2. OutOfBandRollup:

   `java -Dblueflood.config=file:/home/chinmay/bf-CFBackfiller.conf -Dlog4j.configuration=file:/home/chinmay/log4jRollupGenerator.properties -Xms3G -Xmx6G -cp blueflood-rax-all-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.rackspacecloud.blueflood.backfiller.service.OutOFBandRollup`

