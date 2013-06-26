"""
define some constants
"""


HBASE_HOME_DIR = "/home/hadoop/hbase-single"
BACKUP_DIR = "/home/hadoop/backup"
DOWNLOAD_DIR = "/home/hadoop/download"


HBASE_HBCK_OLD_LOG = "hbase-hbck-old.log"
HDFS_FSCK_OLD_LOG = "hdfs-fsck-old.log"
HDFS_REPORT_OLD_LOG = "hdfs-report-old.log"
HDFS_LSR_OLD_LOG = "hdfs-lsr-old.log"


################################################################################
# for test environment


MAPRED_SYSTEM_DIR = "/home/hadoop/data/hadoop/cache/hadoop/mapred/system"
MAPRED_LOCAL_DIR_LIST = [
    "/home/hadoop/data/hadoop/cache/hadoop/mapred/local",
    "/home/hadoop/data2/hadoop/cache/hadoop/mapred/local",
    "/home/hadoop/data3/hadoop/cache/hadoop/mapred/local",
    "/home/hadoop/data4/hadoop/cache/hadoop/mapred/local",
]
HDFS_NAME_DIR_LIST = [
    "/home/hadoop/data/hadoop/cache/hadoop/dfs/name",
]
HDFS_DATA_DIR_LIST = [
    "/home/hadoop/data/hadoop/cache/hadoop/dfs/data",
    "/home/hadoop/data2/hadoop/cache/hadoop/dfs/data",
    "/home/hadoop/data3/hadoop/cache/hadoop/dfs/data",
    "/home/hadoop/data4/hadoop/cache/hadoop/dfs/data",
]


#################################################################################
# for production environment

# MAPRED_SYSTEM_DIR = "/data/hadoop/cache/hadoop/mapred/system"
# MAPRED_LOCAL_DIR_LIST = [
#     "/data/hadoop/cache/hadoop/mapred/local",
#     "/data2/hadoop/cache/hadoop/mapred/local",
#     "/data3/hadoop/cache/hadoop/mapred/local",
#     "/data4/hadoop/cache/hadoop/mapred/local",
# ]
# HDFS_NAME_DIR_LIST = [
#     "/data/hadoop/cache/hadoop/dfs/name",
# ]
# HDFS_DATA_DIR_LIST = [
#     "/data/hadoop/cache/hadoop/dfs/data",
#     "/data2/hadoop/cache/hadoop/dfs/data",
#     "/data3/hadoop/cache/hadoop/dfs/data",
#     "/data4/hadoop/cache/hadoop/dfs/data",
# ]


##################################################################################

PREPARED_CONF_DIR_FOR_CDH4 = "/home/hadoop/backup/conf.cdh4.prepared"
