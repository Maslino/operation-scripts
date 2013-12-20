# coding: utf8

from config.roles import *

import os, time
from fabric.api import *


DRILL_SCRIPT_DIR = "/home/hadoop/Drill/script"
DRILL_SERVICE_SCRIPT = "service.pl"

QM_BIN_DIR_141 = "/home/hadoop/catalina/apache-tomcat-7.0.42.8181/bin"
QM_BIN_DIR_61 = "/home/hadoop/catalina/apache-tomcat-7.0.42.8182/bin"
QM_SHUTDOWN_SCRIPT = "shutdown.sh"
QM_STARTUP_SCRIPT = "startup.sh"


def status_drill():
    return local("perl %s status" % os.path.join(DRILL_SCRIPT_DIR, DRILL_SERVICE_SCRIPT), capture=True)


def stop_drill():
    local("perl %s stop" % os.path.join(DRILL_SCRIPT_DIR, DRILL_SERVICE_SCRIPT))


def start_drill():
    local("perl %s start" % os.path.join(DRILL_SCRIPT_DIR, DRILL_SERVICE_SCRIPT))


def is_drill_down():
    return "not" in status_drill()


def shutdown_qm_141():
    local("sh %s" % os.path.join(QM_BIN_DIR_141, QM_SHUTDOWN_SCRIPT))


def start_qm_141():
    local("sh %s" % os.path.join(QM_BIN_DIR_141, QM_STARTUP_SCRIPT))


def shutdown_qm_61():
    run("sh %s" % os.path.join(QM_BIN_DIR_61, QM_SHUTDOWN_SCRIPT))


def start_qm_61():
    run("sh %s" % os.path.join(QM_BIN_DIR_61, QM_STARTUP_SCRIPT))


def supervisor_drill():
    if not is_drill_down():
        print "Drill is not down."
        return

    print "Drill is down."
    print "shutdown qm 141..."
    execute(shutdown_qm_141)
    time.sleep(1)
    print "shutdown qm 61..."
    execute(shutdown_qm_61, roles=PRODUCTION_QM_61)
    time.sleep(1)
    print "stop drill..."
    execute(stop_drill)
    time.sleep(1)
    print "start drill..."
    execute(start_drill)
    time.sleep(1)
    print "start qm 141..."
    execute(start_qm_141)
    time.sleep(1)
    print "start qm 61..."
    execute(start_qm_61, roles=PRODUCTION_QM_61)
