# coding=utf8

from fabric.api import run, sudo, roles
from config.roles import *


@roles(PRODUCTION_DATANODE, PRODUCTION_DATANODE_NEW)
def set_swappiness_to_zero():
    sudo("sysctl vm.swappiness=0")