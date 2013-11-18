# coding: utf8

import datetime
from fabric.api import sudo, execute
from fabric.contrib.files import exists
from config.roles import *


def __do_rotate(log_path):
    yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d")

    if not exists(log_path):
        return

    yesterday_log = ".".join([log_path, yesterday])
    sudo("cp %s %s" % (log_path, yesterday_log))
    sudo("> %s" % log_path)


NGINX_ERROR_LOG = "/data/log/nginx/error.log"


def rotate_log():
    execute(__do_rotate, NGINX_ERROR_LOG, role=PRODUCTION_DATALOADER)
