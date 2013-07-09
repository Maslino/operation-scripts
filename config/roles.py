# coding=utf8

"""
define some roles for Fabric
"""
from fabric.api import env


PRODUCTION_NAMENODE = "pro-nn"
PRODUCTION_DATANODE = "pro-dn"
PRODUCTION_DATANODE_NEW = "pro-dn-new"

DEVELOPMENT_NAMENODE = "dev-nn"
DEVELOPMENT_DATANODE = "dev-dn"


env.roledefs = {
    PRODUCTION_NAMENODE: (
        "hadoop@pro-nn",
    ),
    PRODUCTION_DATANODE: (
        "hadoop@192.168.1.146"
        "hadoop@192.168.1.147",
        "hadoop@192.168.1.148",
        "hadoop@192.168.1.150",
        "hadoop@192.168.1.151",
        "hadoop@192.168.1.152",
        "hadoop@192.168.1.153"
        "hadoop@192.168.1.154",
    ),
    PRODUCTION_DATANODE_NEW: (
        "hadoop@192.168.1.23",
        "hadoop@192.168.1.24",
        "hadoop@192.168.1.29",
        "hadoop@192.168.1.30"
        "hadoop@192.168.1.31",
    ),
    DEVELOPMENT_NAMENODE: (
        "hadoop@10.1.16.221",
    ),
    DEVELOPMENT_DATANODE: (
        "hadoop@10.1.16.221",
        "hadoop@10.1.16.222",
    ),
}