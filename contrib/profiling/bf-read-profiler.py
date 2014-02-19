#! /usr/bin/python
# Licensed to Rackspace under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# Rackspace licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License."

import pycassa
import logging
import subprocess
import sys
import shlex


def __is_more_available(len_fetched, page_size):
    return (len_fetched >= page_size)


def _get_server_list(servers_string):
    return [x.strip() for x in servers_string.split(',')]


def _percentile(N, percent, key=lambda x:x):
    if not N:
        return None
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (k-f)
    d1 = key(N[int(c)]) * (c-k)
    return d0+d1


def get_locators_for_shard(shard, servers):
    """ Returns all the locators for the shard """

    pool = pycassa.ConnectionPool('DATA',
                                  server_list=_get_server_list(servers))
    cf = pycassa.ColumnFamily(pool, 'metrics_locator')
    page_size = 100
    start = ''
    locators = []

    while True:
        batch = cf.get(int(float(shard)), column_start=start,
                       column_finish='', column_count=page_size)

        keys = batch.keys()
        locators.extend(keys)

        if not __is_more_available(len(batch), page_size):
            break

        start = keys[len(batch)]

    return locators


def _get_query_command_for_locator(locator):
     """ Returns a complete GetPoints command, bailing out for invalid locators """

     if locator.startswith('null.rackspace.'):
         return None
     elif '.rackspace.monitoring.' not in locator:
         return None

     try:
         tenant_id, metric = locator.split('.', 1)
     except ValueError:
         return None

     return '''time -p java -Dblueflood.config=/opt/ele-conf/bf-0.conf -cp /opt/blueflood/blueflood.jar
            com.rackspacecloud.blueflood.ops.GetPoints -tenantId ''' + tenant_id+' -metric ' + metric


def query_each_locator(locators):
    """ Queries each locator using the GetPoints tool and records the timing stats """

    stats_per_locator = {}

    for locator in locators:
        query_command = _get_query_command_for_locator(locator)

        if query_command is not None:
            p = subprocess.Popen(shlex.split(query_command), stdout=subprocess.PIPE,
                  stderr=subprocess.PIPE)
            std_out,std_err = p.communicate()

            if std_err:
              raise RuntimeError(std_err)

            WTF_is_timing = _parse_timing(std_out)
            
            if WTF_is_timing:
                stats_per_locator[locator] = WTF_is_timing

    return stats_per_locator


def _parse_timing(std_out):
    """ Parses the output of timing the GetPoints tool and filters out the user timings stat """

    found_usage = False

    for line in std_out.split("\n"):
        line = line.strip()
        if not found_usage:
            found_usage = line.startswith("real")
            continue
        if found_usage:
            user_timing = line.split()[1]
            if not user_timing:
                return None

    return user_timing


def print_stats(stats_per_locator):
    """ Prints user stats for all locators of the supplied shard. Also prints min, max, 80th and 95th percentiles"""

    for locator,user_stat in stats_per_locator.items():
        print 'Locator: %s    Time Taken: %s' % (locator,user_stat)

    float_timings = [float(x) for x in stats_per_locator.values()]

    print 'Minimum time taken %f' % min(float_timings)
    print 'Max time taken %f' % max(float_timings)
    print '80th percentile %f' % (_percentile(float_timings.sort(), 0.8))
    print '95th percentile %f' % (_percentile(float_timings.sort(), 0.95))


def main(servers, shard):
    try:
        logging.debug('Profiling reads for shard %d', shard)
        print 'Getting all locators for shard %s' % (shard)
        all_locators_for_shard = get_locators_for_shard(shard,
                                                        servers)
        print 'Getting query timings for...'
        stats_per_locator = query_each_locator(all_locators_for_shard)

        if len(stats_per_locator) > 0:
            print 'Printing read profile for shard %s' % (shard)
            print_stats(stats_per_locator)
        else:
            print 'No stats retrieved. Nothing to print'

        print 'Cheers! Completed successfully.'
    except Exception, ex:
        logging.exception(ex)
        print "Error while profiling reads", ex

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
