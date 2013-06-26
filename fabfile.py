# -*- coding: utf-8 -*-

"""
fabfile for Fabric

Usage:
on namenode machine, run "fab task_name -R role1,role2"
"""

import os
import time
from fabric.api import *
from fabric.colors import red
from fabric.contrib.files import exists

env.roledefs = {
    "pro-nn": (
        "hadoop@pro-nn",
    ),
    "pro-dn": (
        "hadoop@192.168.1.147",
        "hadoop@192.168.1.148",
        "hadoop@192.168.1.150",
        "hadoop@192.168.1.151",
        "hadoop@192.168.1.152",
        "hadoop@192.168.1.154",
    ),
    "dev-nn": (
        "hadoop@10.1.16.221",
    ),
    "dev-dn": (
        "hadoop@10.1.16.221",
        "hadoop@10.1.16.222",
    ),
    "dev-online":(
        "hadoop@dev-online",
    )
}


def exec_cmd(command_string, require_sudo=0):
    if require_sudo:
        sudo(command_string)
    else:
        run(command_string)


def convert_str_to_bool(string):
    if string.lower() == "true":
        return True
    elif string.lower() == "false":
        return False

    raise ValueError("cannot convert to bool")


def test(remote):
    remote = convert_str_to_bool(remote)
    if remote:
        run("uname -a")
    else:
        local("uname -a")

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


HBASE_HOME_DIR = "/home/hadoop/hbase-single"
BACKUP_DIR = "/home/hadoop/backup"
DOWNLOAD_DIR = "/home/hadoop/download"
HBASE_HBCK_OLD_LOG = "hbase-hbck-old.log"
HDFS_FSCK_OLD_LOG = "hdfs-fsck-old.log"
HDFS_REPORT_OLD_LOG = "hdfs-report-old.log"
HDFS_LSR_OLD_LOG = "hdfs-lsr-old.log"


def make_directory(remote):
    """
    在所有节点上创建相关目录
    """
    remote = convert_str_to_bool(remote)
    for directory in (BACKUP_DIR, DOWNLOAD_DIR):
        if remote:
            if not exists(directory):
                run("mkdir -p %s" % directory)
        else:
            if not os.path.exists(directory):
                local("mkdir -p %s" % directory)


def check_hbase():
    """
    在datanode节点上检查hbase数据
    """
    hbck_log = os.path.join(BACKUP_DIR, HBASE_HBCK_OLD_LOG)
    while not exists(hbck_log):
        hbck_log = ".".join([hbck_log, str(time.time())])

    success = True
    with cd(HBASE_HOME_DIR):
        run("bin/hbase hbck > %s 2>&1" % hbck_log)
        last_two_line = run("tail -n2 %s" % hbck_log)
        if "OK" not in last_two_line:
            success = False
            print red("hbck result: \n%s" % last_two_line)

    if not success:
        raise Exception("hbck failed")


def stop_hbase():
    """
    在所有datanode节点上停止hbase
    """
    with cd(HBASE_HOME_DIR):
        stop_script = "bin/stop-hbase.sh"
        run("%s" % stop_script)


def detect_jdk_home(remote):
    remote = convert_str_to_bool(remote)
    jdk_home = None
    for soft_link in ("jdk", "latest", "default"):
        jdk = os.path.join("/usr/java", soft_link)
        if remote:
            if exists(jdk):
                jdk_home = jdk
                break
        else:
            if os.path.exists(jdk):
                jdk_home = jdk

    assert jdk_home is not None
    return jdk_home


def get_jps_path(remote):
    remote = convert_str_to_bool(remote)
    return os.path.join(detect_jdk_home(remote), "bin/jps")


def confirm_hbase_stopped():
    """
    在所有datanode节点上检查hbase是否已被停止
    """
    output = sudo("%s" % get_jps_path(remote=True))
    for daemon in ["hmaster", "hregionserver", "hquorumpeer"]:
        assert daemon not in output.lower()
        #todo : kill -9 if not stopped?


def stop_jobtracker_cdh3():
    """
    停止namenode节点上的jobtracker服务
    """
    jobtracker_service = "hadoop-0.20-mapreduce-jobtracker"
    local("sudo /sbin/service %s stop" % jobtracker_service)


def stop_tasktracker_cdh3():
    """
    停止所有datanode节点上的tasktracker服务
    """
    tasktracker_service = "hadoop-0.20-mapreduce-tasktracker"
    sudo("/sbin/service %s stop" % tasktracker_service)


def confirm_mapred_stopped(remote):
    """
    检查mapreduce服务是否停止
    """
    remote = convert_str_to_bool(remote)
    if remote:
        output = sudo("%s" % get_jps_path(remote))
        assert "tasktracker" not in output.lower()
    else:
        output = local("sudo %s" % get_jps_path(remote))
        assert "jobtracker" not in output.lower()


def check_hdfs():
    fsck_log = HDFS_FSCK_OLD_LOG
    while not exists(os.path.join(BACKUP_DIR, fsck_log)):
        fsck_log = ".".join([fsck_log, str(time.time())])
    local("hadoop fsck / -files -blocks -locations > %s" % fsck_log)

    lsr_log = HDFS_LSR_OLD_LOG
    while not exists(os.path.join(BACKUP_DIR, lsr_log)):
        lsr_log = ".".join([lsr_log, str(time.time())])
    local("hadoop fs -lsr / > %s" % lsr_log)

    report_log = HDFS_REPORT_OLD_LOG
    while not exists(os.path.join(BACKUP_DIR, report_log)):
        report_log = ".".join([report_log, str(time.time())])
    local("hadoop dfsadmin -report > %s" % report_log)


def stop_namenode_cdh3():
    """
    停止namenode节点上的namenode服务
    """
    namenode_service = "hadoop-0.20-namenode"
    local("sudo /sbin/service %s stop" % namenode_service)


def stop_datanode_cdh3():
    """
    停止所有datanode节点上的datanode服务
    """
    datanode_service = "hadoop-0.20-datanode"
    sudo("/sbin/service %s stop" % datanode_service)


def confirm_hdfs_stopped(remote):
    """
    检查hdfs和mapreduce服务是否停止
    """
    remote = convert_str_to_bool(remote)
    if remote:
        output = sudo("%s" % get_jps_path(remote))
        assert "datanode" not in output.lower()
    else:
        output = local("%s" % get_jps_path(remote))
        assert "namenode" not in output.lower()

#################################################################


def backup_hdfs_metadata():
    """
    在namenode节点上备份hdfs的元数据
    """
    # metadata_dir = "/data/hadoop/cache/hadoop/dfs/name"
    metadata_dir = "/home/hadoop/data/hadoop/cache/hadoop/dfs/name"
    assert exists(metadata_dir)
    assert "lock" not in local("ls %s" % metadata_dir).lower()

    local("tar -cvf %s %s" % (
        os.path.join(BACKUP_DIR, "namenode_backup_metadata.tar"), metadata_dir))


def backup_hadoop_conf():
    """
    备份namenode节点上的配置文件
    """
    orig_conf_dir = "/etc/hadoop-0.20/conf"
    assert os.path.exists(orig_conf_dir)
    backup_conf_dir = os.path.join(BACKUP_DIR, "conf.cdh3")

    local("mdkir %s" % backup_conf_dir)
    local("cp -r %s %s" % (orig_conf_dir + "/*", backup_conf_dir))


################################################################


def uninstall_cdh3(remote):
    """
    在所有节点上卸载cdh3
    """
    remote = convert_str_to_bool(remote)
    for package in ("hadoop-0.20", "bigtop-utils", "cloudera-cdh3"):
        if remote:
            sudo("yum remove -y %s" % package)
        else:
            local("sudo yum remove -y %s" % package)

    # ensure that no hadoop or cdh package exists
    with settings(hide("stdout")):
        if remote:
            output = run("rpm -qa")
        else:
            output = local("rpm -qa")
        if output:
            assert "hadoop" not in output.lower()
            assert "cdh" not in output.lower()

    cdh3_repo_path = "/etc/yum.repos.d/cloudera-cdh3.repo"
    if remote:
        if exists(cdh3_repo_path):
            sudo("rm %s" % cdh3_repo_path)
    else:
        if os.path.exists(cdh3_repo_path):
            local("sudo rm %s" % cdh3_repo_path)


def install_cdh4(remote, centos_version=6):
    """
    在所有节点上安装cdh4
    """
    remote = convert_str_to_bool(remote)
    centos_version = int(centos_version)
    if centos_version not in (6, 5):
        raise Exception("invalid parameter")

    if centos_version == 6:
        cdh4_rpm_url = "http://archive.cloudera.com/cdh4/one-click-install/redhat/6/x86_64/cloudera-cdh-4-0.x86_64.rpm"
    else:
        cdh4_rpm_url = "http://archive.cloudera.com/cdh4/one-click-install/redhat/5/x86_64/cloudera-cdh-4-0.x86_64.rpm"

    with cd(DOWNLOAD_DIR):
        if remote:
            run("wget %s" % cdh4_rpm_url)
            sudo("yum --nogpgcheck localinstall -y cloudera-cdh-4-0.x86_64.rpm")
        else:
            local("wget %s" % cdh4_rpm_url)
            local("sudo yum --nogpgcheck localinstall -y cloudera-cdh-4-0.x86_64.rpm")

    if centos_version == 6:
        if remote:
            sudo("rpm --import http://archive.cloudera.com/cdh4/redhat/6/x86_64/cdh/RPM-GPG-KEY-cloudera")
        else:
            local("sudo rpm --import http://archive.cloudera.com/cdh4/redhat/6/x86_64/cdh/RPM-GPG-KEY-cloudera")
    else:
        if remote:
            sudo("rpm --import http://archive.cloudera.com/cdh4/redhat/5/x86_64/cdh/RPM-GPG-KEY-cloudera")
        else:
            local("sudo rpm --import http://archive.cloudera.com/cdh4/redhat/5/x86_64/cdh/RPM-GPG-KEY-cloudera")

    if remote:
        sudo("yum install -y hadoop-0.20-mapreduce-jobtracker")
        sudo("yum install -y hadoop-hdfs-namenode")
        sudo("yum install -y hadoop-0.20-mapreduce-tasktracker")
        sudo("yum install -y hadoop-hdfs-datanode")
    else:
        local("sudo yum install -y hadoop-0.20-mapreduce-jobtracker")
        local("sudo yum install -y hadoop-hdfs-namenode")
        local("sudo yum install -y hadoop-0.20-mapreduce-tasktracker")
        local("sudo yum install -y hadoop-hdfs-datanode")


def install_lzo(remote, centos_version=6):
    """
    在所有节点上安装LZO压缩库
    """
    remote = convert_str_to_bool(remote)
    centos_version = int(centos_version)
    if centos_version not in (6, 5):
        raise Exception("invalid parameter")

    if centos_version == 6:
        lzo_cdh4_repo = "http://archive.cloudera.com/gplextras/redhat/6/x86_64/gplextras/cloudera-gplextras4.repo"
    else:
        lzo_cdh4_repo = "http://archive.cloudera.com/gplextras/redhat/5/x86_64/gplextras/cloudera-gplextras4.repo"

    with cd(DOWNLOAD_DIR):
        if remote:
            run("wget %s" % lzo_cdh4_repo)
            sudo("cp cloudera-gplextras4.repo /etc/yum.repos.d/")
            sudo("yum install -y hadoop-lzo-cdh4")
        else:
            local("wget %s" % lzo_cdh4_repo)
            local("sudo cp cloudera-gplextras4.repo /etc/yum.repos.d/")
            local("sudo yum install -y hadoop-lzo-cdh4")


#################################################################

# todo: config for cdh4

PREPARED_CONF_DIR_FOR_CDH4 = "/home/hadoop/backup/conf.cdh4.prepared"


def rsync_conf():
    remote_host = "@".join([env.user, env.host])
    local("rsync -avz --delete %s %s:%s" %(PREPARED_CONF_DIR_FOR_CDH4, remote_host, BACKUP_DIR))


def update_conf(remote):
    remote = convert_str_to_bool(remote)

    target_cdh4_conf_dir = "/etc/hadoop/conf.cdh4"
    assert os.path.exists(PREPARED_CONF_DIR_FOR_CDH4)

    if remote:
        rsync_conf()
        if not exists(target_cdh4_conf_dir):
            sudo("mkdir %s" % target_cdh4_conf_dir)

        sudo("cp %s %s" %(PREPARED_CONF_DIR_FOR_CDH4 + "/*", target_cdh4_conf_dir))
        sudo("update-alternatives --install %s hadoop-conf %s 50" % ("/etc/hadoop/conf", target_cdh4_conf_dir))

        output = run("alternatives --display hadoop-conf")
        lines = output.split("\n")
        assert target_cdh4_conf_dir in lines[1]
    else:
        if not os.path.exists(target_cdh4_conf_dir):
            local("sudo mkdir %s" % target_cdh4_conf_dir)

        local("sudo cp %s %s" %(PREPARED_CONF_DIR_FOR_CDH4 + "/*", target_cdh4_conf_dir))
        local("sudo update-alternatives --install %s hadoop-conf %s 50" % ("/etc/hadoop/conf", target_cdh4_conf_dir))

        output = local("alternatives --display hadoop-conf")
        lines = output.split("\n")
        assert target_cdh4_conf_dir in lines[1]


def change_mod_and_perm():
    pass


#################################################################


def upgrade_hdfs():
    """
    升级hdfs， 在namenode节点上执行
    """
    local("sudo /sbin/service hadoop-hdfs-namenode upgrade")
    assert "namenode" in local("sudo %s" % get_jps_path(remote=0)).lower()


def start_datanode_cdh4():
    """
    启动所有datanode节点
    等待namenode退出安全模式
    """
    sudo("/sbin/service hadoop-hdfs-datanode start")
    assert "datanode" in sudo("%s" % get_jps_path(remote=1)).lower()


def start_tasktracker_cdh4():
    """
    在每个datanode上启动TaskTracker
    """
    sudo("/sbin/service hadoop-0.20-mapreduce-tasktracker start")
    assert "tasktracker" in sudo("%s" % get_jps_path(remote=1)).lower()


def start_jobtracker_cdh4():
    """
    在namenode节点上启动JobTracker
    """
    local("sudo /sbin/service hadoop-0.20-mapreduce-jobtracker start")
    assert "jobtracker" in local("sudo %s" % get_jps_path(remote=0)).lower()

#################################################################


# def finalize():
#     run("hdfs dfsadmin -finalizeUpgrade")
