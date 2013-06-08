# -*- coding: utf-8 -*-

"""
fabfile for Fabric
"""
import os
from fabric.api import *


env.roledefs = {
    "local": (
	'localhost',
    ),
    "namenode": (
        "hadoop@nn",
    ),
    "datanode": (
        "hadoop@192.168.1.147",
        "hadoop@192.168.1.148",
        "hadoop@192.168.1.150",
        "hadoop@192.168.1.151",
        "hadoop@192.168.1.152",
        "hadoop@192.168.1.154",
    )
}


@roles("local")
def local_uname():
    local("uname -a")


@roles('namenode')
def nn_uname():
    env.key_filename = "/home/liuxiong/.ssh/id_rsa.pub"
    run("uname -a")


@roles("datanode")
def dn_uname():
    env.key_filename = "/home/hadoop/.ssh/id_rsa.pub"
    run("uname -a")


@roles("datanode")
def exec_cmd(command_string):
    run(command_string)

###################################################################

def install_jdk7():
    # assume jdk7 tarball is under directory ~/download
    jdk7_tarball = "/home/hadoop/download/jdk-7u21-linux-x64.tar.gz"
    target_dir = "/usr/java"
    with cd(target_dir):
        sudo("tar zxf %s"%jdk7_tarball)
        for link in ["jdk", "latest"]:
            sudo("rm %s"%link)
            sudo("ln -s %s %s"%(target_dir + "/jdk1.7.0_21", link))

###################################################################

def stop_hbase():
    stop_script = "/home/hadoop/hbase-single/bin/start-hbase.sh"
    run("%s"%stop_script)
    
    
def check_hbase():
    output = run("jps")
    for daemon in ["hmaster", "hregionserver", "hquorumpeer"]:
        assert daemon not in output.lower()


def stop_namenode():
    namenode_service = ""
    jobtracker_service = ""
    sudo("service %s stop"%namenode_service)
    sudo("service %s stop"%jobtracker_service)


def stop_datanode():
    datanode_service = ""
    tasktracker_service = ""
    sudo("service %s stop"%datanode_service)
    sudo("service %s stop"%tasktracker_service)


def check_hadoop():
    output = run("jps")
    for daemon in ["datanode", "tasktracker", "jobtracker", "namenode"]:
        assert daemon not in output.lower()

#################################################################

def backup_hdfs_metadata():
    metadata_dir = "/data/hadoop/cache/hadoop/dfs/name"
    backup_dir = "/home/hadoop/backup"
    assert "lock" not in local("ls %s"%metadata_dir).lower()
    local("mkdir %s"%backup_dir)
    local("tar cvf %s %s"%(backup_dir + "/namenode_backup_metadata.tar", metadata_dir))
    
        
def backup_hadoop_conf():
    orig_conf_dir = "/etc/hadoop/conf"
    backup_conf_dir = "/etc/hadoop/conf.cdh4"
    sudo("cp -r %s %s"%(orig_conf_dir, backup_dir_conf))
    sudo("update-alternatives --install %s hadoop-conf %s 50"%(orig_conf_dir, backup_conf_dir))
    output = run("alternatives --display hadoop-conf")
    assert "something is right"


################################################################

def uninstall_cdh3():
    sudo("yum remove hadoop-0.20 bigtop-utils")
    sudo("yum remove cloudera-cdh3")

def install_cdh4():
    cdh_rpm_url = "http://archive.cloudera.com/cdh4/one-click-install/redhat/6/x86_64/cloudera-cdh-4-0.x86_64.rpm"
    download_dir = "/home/hadoop/download"
    run("mkdir %s"%download_dir)
    with cd(download_dir):
        run("wget %s"%cdh_rmp_url)
        sudo("yum --nogpgcheck localinstall cloudera-cdh-4-0.x86_64.rpm")
        sudo("rpm --import http://archive.cloudera.com/cdh4/redhat/6/x86_64/cdh/RPM-GPG-KEY-cloudera")
        sudo("yum install hadoop-0.20-mapreduce-jobtracker")
        sudo("yum install hadoop-hdfs-namenode")
        sudo("yum install hadoop-0.20-mapreduce-tasktracker")
        sudo("yum install hadoop-hdfs-datanode")

#################################################################

def copy_log4j_properties():
    run("cp /etc/hadoop/conf.empty/log4j.properties\
        /etc/hadoop/conf.cdh4/log4j.properties")

#################################################################
        
def upgrade_hdfs():
    sudo("service hadoop-hdfs-namenode upgrade")

def start_datanode():
    sudo("service hadoop-hdfs-datanode start")

def start_tasktracker():
    sudo("service hadoop-0.20-mapreduce-tasktracker start")
    assert "tasktracker" in run("jps").lower()
    
def start_jobtracker():
    sudo("service hadoop-0.20-mapreduce-jobtracker start")
    assert "jobtracker" in run("jps").lower()

##################################################################

def recover_hadoop_conf():
    pass

#################################################################

def restart_cluster():
    pass

def finalize():
    sudo("hdfs dfsadmin -finalizeUpgrade")

