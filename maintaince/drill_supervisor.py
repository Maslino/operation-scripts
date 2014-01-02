# coding: utf8

from config.roles import *

import os
import time
from fabric.api import *
from fabric.colors import red, green, yellow


DRILL_SCRIPT_DIR = "/home/hadoop/Drill/script"
DRILL_SERVICE_SCRIPT = "service.pl"

QM_BIN_DIR_141 = "/home/hadoop/catalina/apache-tomcat-7.0.42.8181/bin"
QM_BIN_DIR_61 = "/home/hadoop/catalina/apache-tomcat-7.0.42.8182/bin"
QM_SHUTDOWN_SCRIPT = "shutdown.sh"
QM_STARTUP_SCRIPT = "startup.sh"

START_QM_SIGNAL_FILE = "/home/hadoop/run/start_qm_signal.do_not_delete"
START_QM_SIGNAL = "DOWN"


def status_drill():
    print "status drill..."
    return local("perl %s status" % os.path.join(DRILL_SCRIPT_DIR, DRILL_SERVICE_SCRIPT), capture=True)


def stop_drill():
    print yellow("stop drill...")
    local("perl %s stop" % os.path.join(DRILL_SCRIPT_DIR, DRILL_SERVICE_SCRIPT))


def start_drill():
    print yellow("start drill...")
    local("perl %s start" % os.path.join(DRILL_SCRIPT_DIR, DRILL_SERVICE_SCRIPT))


def is_drill_down():
    return "not" in status_drill()


def shutdown_qm_141():
    print yellow("shutdown qm 141...")
    output = local("ps -ef | grep apache-tomcat-7.0.42.8181 | grep -v grep | awk '{print $2}'", capture=True)
    qm_pid = output.strip()
    assert qm_pid
    print "qm's pid:", qm_pid
    local("kill %s" % qm_pid)


def start_qm_141():
    print yellow("start qm 141...")
    local("sh %s" % os.path.join(QM_BIN_DIR_141, QM_STARTUP_SCRIPT))


def shutdown_qm_61():
    print yellow("shutdown qm 61...")
    output = run("ps -ef | grep apache-tomcat-7.0.42.8182 | grep -v grep | awk '{print $2}'")
    qm_pid = output.strip()
    assert qm_pid
    print "qm's pid:", qm_pid
    run("kill %s" % qm_pid)


def start_qm_61():
    # how to start tomcat remotely?
    print yellow("send start qm signal...")
    run("echo %s > %s" % (START_QM_SIGNAL, START_QM_SIGNAL_FILE))


def __arg2bool(arg):
    if isinstance(arg, bool):
        return arg
    if arg.lower() == "false":
        return False
    if arg.lower() == "true":
        return True

    raise Exception("Invalid arg: %s" % arg)


def supervisor_drill(restart=False):
    restart = __arg2bool(restart)
    down = is_drill_down()

    if not (down or restart):
        print green("Drill is not down.")
        return

    if down:
        print red("Drill is down.")

    print yellow("restart...")

    execute(shutdown_qm_141)
    time.sleep(1)
    execute(shutdown_qm_61, role=PRODUCTION_QM_61)
    time.sleep(1)

    execute(stop_drill)
    time.sleep(1)
    execute(start_drill)
    time.sleep(1)

    execute(start_qm_141)
    time.sleep(1)
    execute(start_qm_61, role=PRODUCTION_QM_61)
