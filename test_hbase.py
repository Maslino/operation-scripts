# -*- coding: utf-8 -*-

import os
import subprocess
from contextlib import closing


def run(times=10):
    for i in range(times):
        current_dir = os.getcwd()
        print "cwd:", current_dir
        log_path = os.path.join(current_dir, "small_and_medium_test_%s.log" % i)
        print "logging to", log_path
        cmd_str = "mvn clean  -X test  -Dhadoop.profile=2.0"
        args = cmd_str.split()
        print args
        with closing(open(log_path, 'w')) as f:
            f.write(cmd_str + "\n")
            subprocess.call(args, stdout=f)


if __name__ == "__main__":
    run()