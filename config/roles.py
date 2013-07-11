# coding=utf8

"""
define some roles for Fabric
"""
from fabric.api import env


PRODUCTION_NAMENODE = "pro-nn"
PRODUCTION_SECONDARY_NAMENODE = "pro-snn"
PRODUCTION_DATANODE_ONE = "pro-dn-1"    # 4 data partition, 2 quad core cpu
PRODUCTION_DATANODE_TWO = "pro-dn-2"    # 6 data partition, 2 quad core cpu, reserve 5G
PRODUCTION_DATANODE_THREE = "pro-dn-3"  # 6 data partition1 1 quad core cpu, reserve 100G
PRODUCTION_DATANODES = (PRODUCTION_DATANODE_ONE, PRODUCTION_DATANODE_TWO, PRODUCTION_DATANODE_THREE)

DEVELOPMENT_NAMENODE = "dev-nn"
DEVELOPMENT_DATANODE = "dev-dn"


env.roledefs = {
    PRODUCTION_NAMENODE: (
        "hadoop@pro-nn",
    ),
    PRODUCTION_SECONDARY_NAMENODE: (
        "hadoop@192.168.1.61",
    ),
    PRODUCTION_DATANODE_ONE: (
        "hadoop@192.168.1.146",
        "hadoop@192.168.1.147",
        "hadoop@192.168.1.148",
        "hadoop@192.168.1.150",
        "hadoop@192.168.1.151",
        "hadoop@192.168.1.152",
        "hadoop@192.168.1.153",
        "hadoop@192.168.1.154",
    ),
    PRODUCTION_DATANODE_TWO: (
        "hadoop@192.168.1.23",
        "hadoop@192.168.1.24",
        "hadoop@192.168.1.29",
        "hadoop@192.168.1.30",
        "hadoop@192.168.1.31",
        "hadoop@192.168.1.35",
    ),
    PRODUCTION_DATANODE_THREE: (
        "hadoop@192.168.1.26",
        "hadoop@192.168.1.32",
    ),
    DEVELOPMENT_NAMENODE: (
        "hadoop@10.1.16.221",
    ),
    DEVELOPMENT_DATANODE: (
        "hadoop@10.1.16.221",
        "hadoop@10.1.16.222",
    ),
}