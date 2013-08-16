# coding=utf8

"""
clean log for HBase and Hadoop
"""
import os
import datetime
from fabric.api import sudo, local, run, execute
from fabric.contrib.files import exists
from config.roles import *


HDFS_LOG_DIR = "/var/log/hadoop-hdfs"
MAPRED_LOG_DIR = "/var/log/hadoop-0.20-mapreduce"
MAPRED_HISTORY_DIR = os.path.join(MAPRED_LOG_DIR, "history")
HBASE_LOG_DIR = "/home/hadoop/hbase-single/logs"


def __arg2bool(arg):
    if isinstance(arg, bool):
        return arg
    if arg.lower() == "false":
        return False
    if arg.lower() == "true":
        return True

    raise Exception("Invalid arg: %s" % arg)


def __clean_hdfs_log(remote, delete=False):
    remote = __arg2bool(remote)
    delete = __arg2bool(delete)
    cmd = "find %s -type f -mtime 7 -name 'hadoop-hdfs-*-ELEX-LA-*.log.*'" % (HDFS_LOG_DIR, )
    if delete:
        cmd = "%s -delete" % cmd

    if remote:
        if exists(HDFS_LOG_DIR):
            sudo(cmd)
    else:
        if os.path.exists(HDFS_LOG_DIR):
            local("sudo %s" % cmd)


def __clean_mapred_log(remote, delete=False):
    remote = __arg2bool(remote)
    delete = __arg2bool(delete)
    cmd = "find %s -type f -mtime 7 -name 'hadoop-hadoop-*-ELEX-LA-*.log.*'" % (MAPRED_LOG_DIR, )
    if delete:
        cmd = "%s -delete" % cmd

    if remote:
        if exists(MAPRED_LOG_DIR):
            sudo(cmd)
    else:
        if os.path.exists(MAPRED_LOG_DIR):
            local("sudo %s" % cmd)


def __clean_hbase_log(delete=False):
    delete = __arg2bool(delete)
    cmd = "find %s -type f -mtime 7 -name 'hbase-hadoop-*-ELEX-LA-*.log.*'" % (HBASE_LOG_DIR, )
    if delete:
        cmd = "%s -delete" % cmd

    if exists(HBASE_LOG_DIR):
        run(cmd)


def __clean_mapred_history(delete=False):
    delete = __arg2bool(delete)
    thirty_days_ago = datetime.datetime.today() - datetime.timedelta(days=30)

    # 删除30天前的的历史记录
    target_dir = os.path.join(MAPRED_HISTORY_DIR, "done/ELEX-LA-WEB1_1372484246521_")
    month_dir_to_delete = os.path.join(target_dir, "%d" % thirty_days_ago.year, "%02d" % thirty_days_ago.month)

    for day in range(1, thirty_days_ago.day):
        day_dir = os.path.join(month_dir_to_delete, "%02d" % day)
        if not os.path.exists(day_dir):
            print "%s not exists." % day_dir
            continue

        if delete:
            local("sudo rm -rf %s" % day_dir)
        else:
            local("sudo ls -lR %s" % day_dir)


def do_clean(delete=False):
    execute(__clean_hbase_log, delete, role=PRODUCTION_DATANODES)
    execute(__clean_hdfs_log, False, delete)
    execute(__clean_hdfs_log, True, delete, roles=PRODUCTION_DATANODES)
    execute(__clean_mapred_log, False, delete)
    execute(__clean_mapred_log, True, delete, roles=PRODUCTION_DATANODES)
    execute(__clean_mapred_history, delete)