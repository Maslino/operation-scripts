# coding: utf8
from contextlib import closing
import os
import subprocess

QM_BIN_DIR_61 = "/home/hadoop/catalina/apache-tomcat-7.0.42.8182/bin"
QM_STARTUP_SCRIPT = "startup.sh"
START_QM_SIGNAL_FILE = "/home/hadoop/run/start_qm_signal.do_not_delete"
START_QM_SIGNAL = "DOWN"

with closing(open(START_QM_SIGNAL_FILE, "r+")) as f:
    lines = f.readlines()
    if len(lines) != 1:
        print "empty file or multiple lines."
        exit()

    f.truncate(0)
    signal = lines[0].strip()
    if signal == START_QM_SIGNAL:
        print "DOWN. start qm..."
        subprocess.call("sh " + os.path.join(QM_BIN_DIR_61, QM_STARTUP_SCRIPT), shell=True)