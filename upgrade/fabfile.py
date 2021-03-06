# -*- coding: utf-8 -*-

"""
fabfile for Fabric

Usage:
on namenode machine, run "fab task_name -R role1,role2"
"""

import os
from fabric.api import *
from fabric.colors import red
from fabric.contrib.files import exists
import time

from config import *
from utils import *


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
    "dev-online": (
        "hadoop@dev-online",
    )
}


def exec_cmd(command_string, use_sudo='false'):
    use_sudo = convert_str_to_bool(use_sudo)
    if use_sudo:
        sudo(command_string)
    else:
        run(command_string)


def enable_epel(centos_version=6):
    version = int(centos_version)
    if version == 6:
        epel_url = "http://ftp.cuhk.edu.hk/pub/linux/fedora-epel/6/i386/epel-release-6-8.noarch.rpm"
        epel_pkg = "epel-release-6-8.noarch.rpm"
    else:
        epel_url = "http://ftp.cuhk.edu.hk/pub/linux/fedora-epel/5/i386/epel-release-5-4.noarch.rpm"
        epel_pkg = "epel-release-5-4.noarch.rpm"

    with cd(DOWNLOAD_DIR):
        run("wget %s" % epel_url)
        sudo("rpm -ivh %s" % epel_pkg)


###################################################################


def install_jdk6():
    jdk6_bin = "jdk-6u45-linux-x64.bin"
    jdk6_dir = "jdk1.6.0_45"
    target_dir = "/usr/java"

    local("rsync --progress %s %s@%s:%s" %(os.path.join(DOWNLOAD_DIR, jdk6_bin), env.user, env.host, DOWNLOAD_DIR))

    with cd(DOWNLOAD_DIR):
        assert exists(os.path.join(DOWNLOAD_DIR, jdk6_bin))
        run("chmod +x %s" % jdk6_bin)
        run("./%s" % jdk6_bin)
        if not exists(target_dir):
            sudo("mkdir %s" % target_dir)
        sudo("mv %s %s" %(jdk6_dir, os.path.join(target_dir, jdk6_dir)))

    with cd(target_dir):
        assert exists(os.path.join(target_dir, jdk6_dir))
        for link in ("jdk", "latest"):
            if exists(os.path.join(target_dir, link)):
                sudo("rm %s" % link)
            sudo("ln -s %s %s" % (os.path.join(target_dir, jdk6_dir), link))


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


def make_directory(remote):
    """
    在所有节点上创建相关目录
    """
    remote = convert_str_to_bool(remote)
    for directory in (BACKUP_DIR, DOWNLOAD_DIR, RELEASE_DIR):
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
    hbck_log = ".".join([hbck_log, date_suffix()])

    success = True
    with cd(HBASE_HOME_DIR):
        run("bin/hbase hbck > %s 2>&1" % hbck_log)
        tail_lines = run("tail -n20 %s" % hbck_log)
        if "OK" not in tail_lines:
            success = False
            print red("hbck result: \n%s" % tail_lines)

    if not success:
        raise Exception("hbck failed")


def stop_hbase():
    """
    在所有datanode节点上停止hbase
    """
    with cd(HBASE_HOME_DIR):
        stop_script = "bin/stop-hbase.sh"
        run("%s" % stop_script)


def start_hbase():
    with cd(HBASE_HOME_DIR):
        start_script = "bin/start-hbase.sh"
        run("%s" % start_script)


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
                break

    assert jdk_home is not None
    return jdk_home


def get_jps_path(remote):
    remote = convert_str_to_bool(remote)
    jps_path = os.path.join(detect_jdk_home(remote), "bin/jps")
    if remote:
        assert exists(jps_path)
    else:
        assert os.path.exists(jps_path)

    return jps_path


def confirm_hbase_stopped():
    """
    在所有datanode节点上检查hbase是否已被停止
    """
    output = sudo("%s" % get_jps_path(remote=True))
    assert "hmaster" not in output.lower()
    assert "hquorumpeer" not in output.lower()

    if "hregionserver" in output.lower():
        print red("hregionserver is still running...")

        hregionserver_pid = None
        lines = output.strip().split('\n')
        for line in lines:
            pid, pname = line.strip().split()
            if pname.lower() == "hregionserver":
                hregionserver_pid = pid
        print red("hregionserver pid: %s" % hregionserver_pid)

        times = 3
        while times > 0:
            print red("attempt to kill hregionserver...")
            run("kill %s" % hregionserver_pid)
            time.sleep(2)
            output = sudo("%s" % get_jps_path(remote=True))
            if "hregionserver" not in output.lower():
                break
            times = times - 1

    for daemon in ["hmaster", "hregionserver", "hquorumpeer"]:
        assert daemon not in output.lower()


def service_jobtracker_cdh3(start_or_stop):
    """
    停止/启动namenode节点上的jobtracker服务
    """
    validate_args(start_or_stop, ("start", "stop"))

    jobtracker_service = "hadoop-0.20-jobtracker"
    local("sudo /sbin/service %s %s" % (jobtracker_service, start_or_stop))


def service_tasktracker_cdh3(start_or_stop):
    """
    停止/启动所有datanode节点上的tasktracker服务
    """
    validate_args(start_or_stop, ("start", "stop"))

    tasktracker_service = "hadoop-0.20-tasktracker"
    sudo("/sbin/service %s %s" % (tasktracker_service, start_or_stop))


def confirm_mapred_stopped(remote):
    """
    检查mapreduce服务是否停止
    """
    remote = convert_str_to_bool(remote)
    if remote:
        output = sudo("%s" % get_jps_path(remote))
        assert "tasktracker" not in output.lower()
    else:
        output = local("sudo %s" % get_jps_path(remote), capture=True)
        assert "jobtracker" not in output.lower()


def check_hdfs():
    fsck_log = ".".join([os.path.join(BACKUP_DIR, HDFS_FSCK_OLD_LOG), date_suffix()])
    local("hadoop fsck / -files -blocks -locations > %s" % fsck_log)

    lsr_log = ".".join([os.path.join(BACKUP_DIR, HDFS_LSR_OLD_LOG), date_suffix()])
    local("hadoop fs -lsr / > %s" % lsr_log)

    report_log = ".".join([os.path.join(BACKUP_DIR, HDFS_REPORT_OLD_LOG), date_suffix()])
    local("hadoop dfsadmin -report > %s" % report_log)

    output = local("tail -n19 %s" % fsck_log, capture=True)
    if "HEALTHY" not in output:
        print red("HDFS IS NOT HEALTHY")
        raise Exception("hdfs is not healthy")


def service_namenode_cdh3(start_or_stop):
    """
    停止/启动namenode节点上的namenode服务
    """
    validate_args(start_or_stop, ("start", "stop"))

    namenode_service = "hadoop-0.20-namenode"
    local("sudo /sbin/service %s %s" % (namenode_service, start_or_stop))


def service_datanode_cdh3(start_or_stop):
    """
    停止/启动所有datanode节点上的datanode服务
    """
    validate_args(start_or_stop, ("start", "stop"))

    datanode_service = "hadoop-0.20-datanode"
    sudo("/sbin/service %s %s" % (datanode_service, start_or_stop))


def confirm_hdfs_stopped(remote):
    """
    检查hdfs和mapreduce服务是否停止
    """
    remote = convert_str_to_bool(remote)
    if remote:
        output = sudo("%s" % get_jps_path(remote))
        assert "datanode" not in output.lower()
    else:
        output = local("%s" % get_jps_path(remote), capture=True)
        assert "namenode" not in output.lower()

#################################################################


def backup_hdfs_metadata():
    """
    在namenode节点上备份hdfs的元数据
    """
    # metadata_dir = "/data/hadoop/cache/hadoop/dfs/name"
    metadata_dir = HDFS_NAME_DIR_LIST[0]
    assert os.path.exists(metadata_dir)
    assert "lock" not in local("ls %s" % metadata_dir, capture=True).lower()

    local("tar -cvf %s %s" % (
        os.path.join(BACKUP_DIR, "namenode_backup_metadata.tar"), metadata_dir))


def backup_hadoop_conf():
    """
    备份namenode节点上的配置文件
    """
    orig_conf_dir = "/etc/hadoop-0.20/conf"
    assert os.path.exists(orig_conf_dir)
    backup_conf_dir = os.path.join(BACKUP_DIR, "conf.cdh3")

    if not os.path.exists(backup_conf_dir):
        local("mkdir -p %s" % backup_conf_dir)

    local("cp -r %s %s" % (orig_conf_dir + "/*", backup_conf_dir))


################################################################


def uninstall_cdh3(remote):
    """
    在所有节点上卸载cdh3
    """
    remote = convert_str_to_bool(remote)
    for package in ("hadoop-0.20", "bigtop-utils", "cloudera-cdh3", "cdh3-repository"):
        if remote:
            sudo("yum remove -y %s" % package)
        else:
            local("sudo yum remove -y %s" % package)

    # ensure that no hadoop or cdh package exists
    with settings(hide("stdout")):
        if remote:
            output = run("rpm -qa")
        else:
            output = local("rpm -qa", capture=True)
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
            # todo: the files downloaded will not be saved in the DOWNLOAD_DIR but in the current working directory
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


def chkconfig_cdh4_service_off(remote):
    """
    关闭相关服务自启动
    """
    remote = convert_str_to_bool(remote)

    if remote:
        # off namenode and jobtracker service
        sudo("chkconfig hadoop-hdfs-namenode off")
        sudo("chkconfig hadoop-0.20-mapreduce-jobtracker off")
    else:
        # off datanode and tasktracker service
        local("sudo chkconfig hadoop-hdfs-datanode off")
        local("sudo chkconfig hadoop-0.20-mapreduce-tasktracker off")


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


def rsync_conf():
    remote_host = "@".join([env.user, env.host])
    local("rsync -avz --delete %s %s:%s" % (PREPARED_CONF_DIR_FOR_CDH4, remote_host, BACKUP_DIR))


def update_conf(remote):
    remote = convert_str_to_bool(remote)

    target_cdh4_conf_dir = "/etc/hadoop/conf.cdh4"
    assert os.path.exists(PREPARED_CONF_DIR_FOR_CDH4)

    if remote:
        rsync_conf()
        if not exists(target_cdh4_conf_dir):
            sudo("mkdir %s" % target_cdh4_conf_dir)

        sudo("cp %s %s" % (PREPARED_CONF_DIR_FOR_CDH4 + "/*", target_cdh4_conf_dir))
        sudo("update-alternatives --install %s hadoop-conf %s 50" % ("/etc/hadoop/conf", target_cdh4_conf_dir))

        output = run("alternatives --display hadoop-conf")
        lines = output.split("\n")
        assert target_cdh4_conf_dir in lines[1]
    else:
        if not os.path.exists(target_cdh4_conf_dir):
            local("sudo mkdir %s" % target_cdh4_conf_dir)

        local("sudo cp %s %s" % (PREPARED_CONF_DIR_FOR_CDH4 + "/*", target_cdh4_conf_dir))
        local("sudo update-alternatives --install %s hadoop-conf %s 50" % ("/etc/hadoop/conf", target_cdh4_conf_dir))

        output = local("alternatives --display hadoop-conf", capture=True)
        lines = output.split("\n")
        assert target_cdh4_conf_dir in lines[1]


def change_mod_and_perm(remote):
    if remote:
        for data_dir in HDFS_DATA_DIR_LIST:
            if exists(data_dir):
                sudo("chmod -R 755 %s" % data_dir)
                sudo("chown -R hdfs:hdfs %s" % data_dir)

        for mapred_local_dir in MAPRED_LOCAL_DIR_LIST:
            if exists(mapred_local_dir):
                sudo("chown -R mapred:mapred %s" % mapred_local_dir)
    else:
        for name_dir in HDFS_NAME_DIR_LIST:
            if os.path.exists(name_dir):
                local("sudo chown -R hdfs:hdfs %s" % name_dir)

        for mapred_local_dir in MAPRED_LOCAL_DIR_LIST:
            if os.path.exists(mapred_local_dir):
                local("sudo chown -R mapred:mapred %s" % mapred_local_dir)


def chown_mapred_system_dir():
    """
    exec this task when hdfs is running and before starting mapreduce service
    """
    local("sudo -u hdfs hadoop fs -chown -R mapred %s" % MAPRED_SYSTEM_DIR)


#################################################################


def upgrade_hdfs():
    """
    升级hdfs， 在namenode节点上执行
    """
    local("sudo /sbin/service hadoop-hdfs-namenode upgrade")
    assert "namenode" in local("sudo %s" % get_jps_path(remote=False), capture=True).lower()


def service_datanode_cdh4(start_or_stop):
    """
    启动/停止所有datanode节点
    """
    validate_args(start_or_stop, ("start", "stop"))

    sudo("/sbin/service hadoop-hdfs-datanode %s" % start_or_stop)

    output = sudo("%s" % get_jps_path(remote=True))
    if start_or_stop == "start":
        assert "datanode" in output.lower()
    elif start_or_stop == "stop":
        assert "datanode" not in output.lower()


def service_tasktracker_cdh4(start_or_stop):
    """
    在每个datanode上启动/停止TaskTracker
    """
    validate_args(start_or_stop, ("start", "stop"))

    sudo("/sbin/service hadoop-0.20-mapreduce-tasktracker %s" % start_or_stop)

    output = sudo("%s" % get_jps_path(remote=True))
    if start_or_stop == "start":
        assert "tasktracker" in output.lower()
    elif start_or_stop == "stop":
        assert "tasktracker" not in output.lower()


def service_jobtracker_cdh4(start_or_stop):
    """
    在namenode节点上启动/停止JobTracker
    """
    validate_args(start_or_stop, ("start", "stop"))

    local("sudo /sbin/service hadoop-0.20-mapreduce-jobtracker %s" % start_or_stop)

    output = local("sudo %s" % get_jps_path(remote=False), capture=True)
    if start_or_stop == "start":
        assert "jobtracker" in output.lower()
    elif start_or_stop == "stop":
        assert "jobtracker" not in output.lower()


#################################################################


# def finalize():
#     run("hdfs dfsadmin -finalizeUpgrade")


#################################################################


def rsync_hbase_0_94_6_hadoop_2():
    hbase_tarball = os.path.join(RELEASE_DIR, HBASE_0_94_6_HADOOP_2_TAR_GZ)
    assert os.path.exists(hbase_tarball)

    target_host = "@".join([env.user, env.host])
    local("rsync --progress %s %s:%s" % (hbase_tarball, target_host, RELEASE_DIR))


def setup_hbase_0_94_6_hadoop_2():
    hbase_tarball = os.path.join(RELEASE_DIR, HBASE_0_94_6_HADOOP_2_TAR_GZ)
    assert exists(hbase_tarball)

    with cd(RELEASE_DIR):
        run("tar zxf %s" % HBASE_0_94_6_HADOOP_2_TAR_GZ)
        run("mv %s %s" % (HBASE_0_94_6_DIR, HBASE_0_94_6_HADOOP_2_DIR))


def config_hbase_0_94_6_hadoop_2():
    old_hbase_conf_dir = os.path.join(HBASE_HOME_DIR, "conf")
    assert exists(old_hbase_conf_dir)

    new_hbase_home_dir = os.path.join(RELEASE_DIR, HBASE_0_94_6_HADOOP_2_DIR)
    with cd(new_hbase_home_dir):
        assert exists(os.path.join(new_hbase_home_dir, "conf"))
        run("mv conf conf.empty")
        run("mkdir conf")
        run("cp %s %s" % (old_hbase_conf_dir + "/*", os.path.join(new_hbase_home_dir, "conf")))

        run("rm %s" % HBASE_HOME_DIR)
        run("ln -s %s %s" % (new_hbase_home_dir, HBASE_HOME_DIR))


def start_hbase_0_94_6_hadoop_2():
    """
    在start前确认coprocessor jar是否在lib目录中；
    配置文件中hbase.hstore.compactionThreshold和hbase.hstore.blockingStoreFiles
    属性的值是否为一个足够大的整数而不是Integer.MAX_VALUE
    """
    confirm_hbase_stopped()
    with cd(HBASE_HOME_DIR):
        run("bin/start-hbase.sh")

    time.sleep(2)

    output = sudo("%s" % get_jps_path(remote=True))
    success = True
    for daemon in ["hmaster", "hregionserver", "hquorumpeer"]:
        if daemon not in output.lower():
            success = False
            print red("%s not started, please check" % daemon)

    if not success:
        raise Exception("start hbase failed.")


#################################################################


def update_hbase_dev():
    execute(rsync_hbase_0_94_6_hadoop_2, role="dev-online")
    execute(setup_hbase_0_94_6_hadoop_2, role="dev-online")
    execute(config_hbase_0_94_6_hadoop_2, role="dev-online")


def update_hbase_pro():
    execute(rsync_hbase_0_94_6_hadoop_2, role=ROLE_DATANODE_PRODUCTION)
    execute(setup_hbase_0_94_6_hadoop_2, role=ROLE_DATANODE_PRODUCTION)
    execute(config_hbase_0_94_6_hadoop_2, role=ROLE_DATANODE_PRODUCTION)


def before_upgrade_metadata_dev():
    execute(make_directory, False)

    execute(check_hbase, host='localhost')
    execute(stop_hbase, host='localhost')
    execute(confirm_hbase_stopped, host='localhost')

    execute(service_jobtracker_cdh3, "stop")
    execute(service_tasktracker_cdh3, "stop", host='localhost')
    execute(confirm_mapred_stopped, False)
    execute(confirm_mapred_stopped, True, host="localhost")

    execute(check_hdfs)
    execute(service_namenode_cdh3, "stop")
    execute(service_datanode_cdh3, "stop", host="localhost")
    execute(confirm_hdfs_stopped, False)
    execute(confirm_hdfs_stopped, True, host="localhost")

    execute(backup_hdfs_metadata)
    execute(backup_hadoop_conf)

    execute(uninstall_cdh3, False)
    execute(uninstall_cdh3, True, host="localhost")
    execute(install_cdh4, False)
    execute(install_cdh4, True, host="localhost")
    execute(install_lzo, False)
    execute(install_lzo, True, host="localhost")

    execute(update_conf, False)
    execute(change_mod_and_perm, False)
    execute(change_mod_and_perm, True, host="localhost")


ROLE_DATANODE_PRODUCTION = "pro-dn"

def before_upgrade_metadata_pro():
    execute(make_directory, False)
    execute(make_directory, True, role=ROLE_DATANODE_PRODUCTION)

    execute(check_hbase, role=ROLE_DATANODE_PRODUCTION)
    execute(stop_hbase, role=ROLE_DATANODE_PRODUCTION)
    execute(confirm_hbase_stopped, role=ROLE_DATANODE_PRODUCTION)

    execute(service_jobtracker_cdh3, "stop")
    execute(service_tasktracker_cdh3, "stop", role=ROLE_DATANODE_PRODUCTION)
    execute(confirm_mapred_stopped, False)
    execute(confirm_mapred_stopped, True, role=ROLE_DATANODE_PRODUCTION)

    execute(check_hdfs)
    execute(service_namenode_cdh3, "stop")
    execute(service_datanode_cdh3, "stop", role=ROLE_DATANODE_PRODUCTION)
    execute(confirm_hdfs_stopped, False)
    execute(confirm_hdfs_stopped, True, role=ROLE_DATANODE_PRODUCTION)

    execute(backup_hdfs_metadata)
    execute(backup_hadoop_conf)

    execute(uninstall_cdh3, False)
    execute(uninstall_cdh3, True, role=ROLE_DATANODE_PRODUCTION)
    execute(install_cdh4, False)
    execute(install_cdh4, True, role=ROLE_DATANODE_PRODUCTION)
    execute(install_lzo, False)
    execute(install_lzo, True, role=ROLE_DATANODE_PRODUCTION)

    execute(update_conf, False)
    execute(update_conf, True, role=ROLE_DATANODE_PRODUCTION)
    execute(change_mod_and_perm, False)
    execute(change_mod_and_perm, True, role=ROLE_DATANODE_PRODUCTION)


def make_data_dir():
    data_dir_list = [
        "/data0/hadoop/cache/hadoop/dfs/data",
        "/data1/hadoop/cache/hadoop/dfs/data",
        "/data2/hadoop/cache/hadoop/dfs/data",
        "/data3/hadoop/cache/hadoop/dfs/data",
        "/data4/hadoop/cache/hadoop/dfs/data",
        "/data5/hadoop/cache/hadoop/dfs/data",
    ]

    for data_dir in data_dir_list:
        sudo("mkdir -p %s" % data_dir)
        sudo("chown -R hdfs:hdfs %s" % data_dir)

    mapred_local_dir_list = [
        "/data0/hadoop/cache/hadoop/mapred/local",
        "/data1/hadoop/cache/hadoop/mapred/local",
        "/data2/hadoop/cache/hadoop/mapred/local",
        "/data3/hadoop/cache/hadoop/mapred/local",
        "/data4/hadoop/cache/hadoop/mapred/local",
        "/data5/hadoop/cache/hadoop/mapred/local",
    ]

    for local_dir in mapred_local_dir_list:
        sudo("mkdir -p %s" % local_dir)
        sudo("chown -R mapred:mapred %s" % local_dir)


def ntp():
    sudo("yum -y install ntp")
    sudo("chkconfig ntpd on")
    sudo("cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime")
    sudo("/sbin/service ntpd start")
    sudo("ntpdate -u  pool.ntp.org")