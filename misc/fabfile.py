# coding=utf8

from fabric.api import run, sudo, roles
from config.roles import *


def set_swappiness_to_zero():
    sudo("sysctl vm.swappiness=0")