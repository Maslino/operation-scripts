# -*- coding: utf-8 -*-

"""
fabfile for Fabric

Usage: fab task_name -R role1,role2
"""
import os
from fabric.api import *
from fabric.contrib.files import exists

env.roledefs = {
    "nn": (
        "hadoop@nn",
    ),
    "dn": (
        "hadoop@192.168.1.147",
        "hadoop@192.168.1.148",
        "hadoop@192.168.1.150",
        "hadoop@192.168.1.151",
        "hadoop@192.168.1.152",
        "hadoop@192.168.1.154",
    ),
    "nn-dev": (
        "hadoop@10.1.16.221",
    ),
    "dn-dev": (
        "hadoop@10.1.16.221",
        "hadoop@10.1.16.222",
    ),
}


def local_uname():
    local("uname -a")


def uname():
    run("uname -a")


def exec_cmd(command_string, require_sudo=0):
    if require_sudo:
        sudo(command_string)
    else:
        run(command_string)

###################################################################


def install_jdk7():
    """
    在所有节点上安装jdk7
    """
    # assume jdk7 tarball is under directory ~/download
    download_dir = "/home/hadoop/download"
    if not exists(download_dir):
        run("mkdir %s" % download_dir)

    jdk7_tarball = os.path.join(download_dir, "jdk-7u21-linux-x64.tar.gz")
    if not exists(jdk7_tarball):
        raise Exception("jdk7 tarball not found in directory: %s" % download_dir)

    target_dir = "/usr/java"
    with cd(target_dir):
        sudo("tar -zxf %s" % jdk7_tarball)
        for link in ["jdk", "latest"]:
            if exists(os.path.join(target_dir, link)):
                sudo("rm %s" % link)
            sudo("ln -s %s %s" % (os.path.join(target_dir, "jdk1.7.0_21"), link))

###################################################################


def stop_hbase():
    """
    在所有datanode节点上停止hbase
    """
    stop_script = "/home/hadoop/hbase-single/bin/stop-hbase.sh"
    run("%s" % stop_script)


def check_hbase():
    """
    在所有datanode节点上检查hbase是否已被停止
    """
    output = run("jps")
    for daemon in ["hmaster", "hregionserver", "hquorumpeer"]:
        assert daemon not in output.lower()


def stop_jobtracker():
    """
    停止namenode节点上的jobtracker服务
    """
    jobtracker_service = "hadoop-0.20-jobtracker"
    sudo("service %s stop" % jobtracker_service)


def stop_tasktracker():
    """
    停止所有datanode节点上的tasktracker服务
    """
    tasktracker_service = "hadoop-0.20-tasktracker"
    sudo("service %s stop" % tasktracker_service)


def stop_namenode():
    """
    停止namenode节点上的namenode服务
    """
    namenode_service = "hadoop-0.20-namenode"
    sudo("service %s stop" % namenode_service)


def stop_datanode():
    """
    停止所有datanode节点上的datanode服务
    """
    datanode_service = "hadoop-0.20-datanode"
    sudo("service %s stop" % datanode_service)


def check_hadoop():
    """
    检查hdfs和mapreduce服务是否停止
    """
    output = run("jps")
    for daemon in ["datanode", "tasktracker", "jobtracker", "namenode"]:
        assert daemon not in output.lower()

#################################################################


def backup_hdfs_metadata():
    """
    在namenode节点上备份hdfs的元数据
    """
    metadata_dir = "/data/hadoop/cache/hadoop/dfs/name"
    assert exists(metadata_dir)
    backup_dir = "/home/hadoop/backup"
    if not exists(backup_dir):
        run("mkdir %s" % backup_dir)

    assert "lock" not in run("ls %s" % metadata_dir).lower()

    run("tar -cvf %s %s" % (
        os.path.join(backup_dir, "namenode_backup_metadata.tar"), metadata_dir))


def backup_hadoop_conf():
    """
    备份所有节点上的配置文件
    """
    orig_conf_dir = "/etc/hadoop/conf"
    backup_conf_dir = "/etc/hadoop/conf.cdh4"
    run("cp -r %s %s" % (orig_conf_dir, backup_conf_dir))
    sudo("update-alternatives --install %s hadoop-conf %s 50" % (orig_conf_dir, backup_conf_dir))

    output = run("alternatives --display hadoop-conf")
    lines = output.split("\n")
    assert backup_conf_dir in lines[1]

################################################################


def uninstall_cdh3():
    """
    在所有节点上卸载cdh3
    """
    sudo("yum remove hadoop-0.20")
    sudo("yum remove bigtop-utils")
    sudo("yum remove cloudera-cdh3")

    output = run("rpm -qa | grep -i hadoop")
    if output:
        assert "hadoop" not in output.lower()


def install_cdh4():
    """
    在所有节点上安装cdh4
    """
    cdh4_rpm_url = "http://archive.cloudera.com/cdh4/one-click-install/redhat/6/x86_64/cloudera-cdh-4-0.x86_64.rpm"
    download_dir = "/home/hadoop/download"
    if not exists(download_dir):
        run("mkdir %s" % download_dir)

    with cd(download_dir):
        run("wget %s" % cdh4_rpm_url)
        sudo("yum --nogpgcheck localinstall -y cloudera-cdh-4-0.x86_64.rpm")

    sudo("rpm --import http://archive.cloudera.com/cdh4/redhat/6/x86_64/cdh/RPM-GPG-KEY-cloudera")
    sudo("yum install -y hadoop-0.20-mapreduce-jobtracker")
    sudo("yum install -y hadoop-hdfs-namenode")
    sudo("yum install -y hadoop-0.20-mapreduce-tasktracker")
    sudo("yum install -y hadoop-hdfs-datanode")
    # sudo("yum install hadoop-client")

#################################################################


def replace_log4j_properties():
    """
    在所有节点上，用cdh4的log4j配置文件覆盖老版本的log4j配置文件
    """
    run("cp /etc/hadoop/conf.dist/log4j.properties /etc/hadoop/conf.cdh4/log4j.properties")


def recover_hadoop_conf():
    """
    在所有节点上，使用原先备份的配置文件
    """
    #todo
    pass

#################################################################


def upgrade_hdfs():
    """
    升级hdfs， 在namenode节点上执行
    等待升级完成
    """
    sudo("service hadoop-hdfs-namenode upgrade")


def start_datanode():
    """
    启动所有datanode节点
    等待namenode退出安全模式
    """
    sudo("service hadoop-hdfs-datanode start")


def start_tasktracker():
    """
    在每个datanode上启动TaskTracker
    """
    sudo("service hadoop-0.20-mapreduce-tasktracker start")
    assert "tasktracker" in run("jps").lower()


def start_jobtracker():
    """
    在namenode节点上启动JobTracker
    """
    sudo("service hadoop-0.20-mapreduce-jobtracker start")
    assert "jobtracker" in run("jps").lower()

#################################################################


def restart_cluster():
    pass


def finalize():
    run("hdfs dfsadmin -finalizeUpgrade")
