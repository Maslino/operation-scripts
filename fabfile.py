# -*- coding: utf-8 -*-

"""
fabfile for Fabric
"""

from fabric.api import *


env.roledefs = {
    "local": (
        "localhost",
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