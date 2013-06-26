# coding=utf8

import commands
from fabric.colors import green

STEPS = [
    "fab make_directory:false",

    "fab check_hbase -H localhost",
    "fab stop_hbase -H localhost",
    "fab confirm_hbase_stopped -H localhost",

    "fab stop_jobtracker_cdh3",
    "fab stop_tasktracker_cdh3 -H localhost",
    "fab confirm_mapred_stopped:false",
    "fab confirm_mapred_stopped:true -H localhost",

    "fab check_hdfs",
    "fab stop_namenode_cdh3",
    "fab stop_datanode_cdh3 -H localhost",
    "fab confirm_hdfs_stopped:false",
    "fab confirm_hdfs_stopped:true -H localhost",

    "fab backup_hdfs_metadata",
    "fab backup_hadoop_conf",

    "fab uninstall_cdh3:false",
    "fab uninstall_cdh3:true -H localhost",
    "fab install_cdh4:false",
    "fab install_cdh4:true -H localhost",
    "fab install_lzo:false",
    "fab install_lzo:true -H localhost",

    "fab update_conf:false",
    "fab change_mod_and_perm:false",
    "fab change_mod_and_perm:true -H localhost",
]


for step in STEPS:
    print green("Exec command: %s" % step)
    status, output = commands.getstatusoutput(step)
    print output
    if status != 0:
        break
