#!/usr/bin/env python

# -*- coding: utf-8 -*-

"""
clean log for hbase and hadoop
"""

import os
import sys
import time
import glob
import datetime


SETTINGS = {
    "hbase": (
        ("/home/hadoop/hbase-single/logs/hbase-hadoop-*-E?EX-LA-WEB*.log.*-??-??", 7),
    ),
    "hadoop": (
        ("/var/log/hadoop/job_*_*_conf.xml", 30),
        ("/var/log/hadoop/history/E?EX-LA-WEB*_*_job_*_*_conf.xml", 30),
        ("/var/log/hadoop/hadoop-hadoop-*-E?EX-LA-WEB*.log.*-??-??", 7),
    )
}


def __clean_file(pattern, days_before=30, do_clean=False):
    print locals()

    today = datetime.date.today()
    timestamp = time.mktime(today.timetuple())

    for path in glob.glob(pattern):
        if os.path.isfile(path):
            stat = os.stat(path)
            if timestamp - stat.st_mtime > days_before * 24 * 3600:
                print path
                if do_clean:
                    os.remove(path)


def main(argv):
    if len(argv) != 2 or argv[1] not in SETTINGS.keys():
        print "Usage: %s {%s}" % (argv[0], "|".join(SETTINGS.keys()))
        return

    service = argv[1]
    for pattern, days_before in SETTINGS.get(service):
        __clean_file(pattern, days_before, do_clean=True)


if __name__ == "__main__":
    main(sys.argv)
