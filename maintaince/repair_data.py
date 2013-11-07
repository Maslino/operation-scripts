# coding=utf8

"""
补数据
"""

import os
from datetime import datetime
from contextlib import closing

CONFIG_PROPERTIES = "config.properties"
SEND_LOG_PROGRESS = "sendlog.process"

DATA_DIR = "/data/log"
EVENT_LOG = "stream.log"
EVENT_CONFIG_DIR = os.path.join(DATA_DIR, "event16config")
EVENT_REPAIR_CONFIG_DIR = os.path.join(DATA_DIR, "event16repairconfig")

CONFIG_DATA_DIR = "datadir"
CONFIG_DATA_FILE = "datafile"
CONFIG_DAY = "day"

DATETIME_FORMAT_ONE = "%Y%m%d%H%M%S"                # 20131031180000
DATETIME_FORMAT_TWO = "%a %b %d %H:%M:%S %Z %Y"     # Wed Oct 30 11:41:40 CST 2013


def find_start_pos(start_date_string):
    start_date = datetime.strptime(start_date_string, DATETIME_FORMAT_ONE)
    with closing(open(os.path.join(EVENT_CONFIG_DIR, SEND_LOG_PROGRESS), "r")) as progress_file:
        target_line = None
        for line in progress_file:
            line = line.strip()
            items = line.split("\t")
            assert len(items) == 3
            date_string = items[2]
            date_time = datetime.strptime(date_string, DATETIME_FORMAT_TWO)
            if date_time < start_date:
                target_line = line
            else:
                items = target_line.split("\t")
                return items[0], items[1]


def find_stop_pos(stop_date_string):
    stop_date = datetime.strptime(stop_date_string, DATETIME_FORMAT_ONE)
    with closing(open(os.path.join(EVENT_CONFIG_DIR, SEND_LOG_PROGRESS), "r")) as progress_file:
        for line in progress_file:
            line = line.strip()
            items = line.split("\t")
            assert len(items) == 3
            date_string = items[2]
            date_time = datetime.strptime(date_string, DATETIME_FORMAT_TWO)
            if date_time > stop_date:
                return items[0], items[1]


def update_event_repair_config(start_date_string):
    day, progress = find_start_pos(start_date_string)
    now = datetime.now().strftime(DATETIME_FORMAT_TWO)
    with closing(open(os.path.join(EVENT_REPAIR_CONFIG_DIR, CONFIG_PROPERTIES), "w")) as config_file:
        lines = "\n".join([
            "# " + now,
            CONFIG_DATA_DIR + "=" + DATA_DIR,
            CONFIG_DATA_FILE + "=" + EVENT_LOG,
            CONFIG_DAY + "=" + day
        ])
        config_file.write(lines)
        config_file.flush()

    with closing(open(os.path.join(EVENT_REPAIR_CONFIG_DIR, SEND_LOG_PROGRESS), "a")) as progress_file:
        progress_file.write("\t".join([day, progress, now]))
        progress_file.write("\n")
        progress_file.flush()


if __name__ == "__main__":
    day, start_progress = find_start_pos("20131107060000")
    day, stop_progress = find_stop_pos("20131107123000")

    print "start: ", day, start_progress
    print "stop: ", day, stop_progress

    update_event_repair_config("20131107060000")