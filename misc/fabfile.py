# coding=utf8

from fabric.api import *
from config.roles import *


def set_swappiness_to_zero():
    sudo("sysctl vm.swappiness=0")


def copy_limits_conf():
    limits_conf_path = "/etc/security/limits.conf"
    local("rsync %s %s@%s:/home/hadoop/backup/" % (limits_conf_path, env.user, env.host))
    sudo("cp /home/hadoop/backup/limits.conf /etc/security/")


def __arg2bool(arg):
    if isinstance(arg, bool):
        return arg
    if arg.lower() == "false":
        return False
    if arg.lower() == "true":
        return True

    raise Exception("Invalid arg: %s" % arg)


def command(command_string, use_sudo='false'):
    use_sudo = __arg2bool(use_sudo)
    if use_sudo:
        sudo(command_string)
    else:
        run(command_string)