# coding=utf8

from fabric.api import *
from upgrade.utils import convert_str_to_bool


def uninstall_cdh4(remote):
    remote = convert_str_to_bool(remote)

    for package in ("hadoop-hdfs", "hadoop-0.20-mapreduce", "cdh4-repository"):
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

    # remove cdh4 repo
    #todo


def install_cdh3(remote):
    #todo
    pass