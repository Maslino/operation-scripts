# coding: utf-8

import commands
from defines import *

MYSQL_USER = ""
MYSQL_PASSWD = ""
MYSQL_CMD_FORMAT = "mysql -u%s -p%s -e '%%s'" % (MYSQL_USER, MYSQL_PASSWD)


def remove_header(output):
    lines = output.strip().split("\n")
    return lines[1:]


def get_databases():
    databases = set()
    cmd = MYSQL_CMD_FORMAT % "show databases;"
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        print "Error! cmd:", cmd
        return set()

    for line in remove_header(output):
        if line.strip().startswith(DATABASE_PREFIX):
            databases.add(line)

    return databases


def count_table(db, table):
    sql = "use %s; select count(*) from %s;" % (db, table)
    cmd = MYSQL_CMD_FORMAT % sql
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        print "table(%s) not exists in db(%s)?" % (table, db)
        return -1

    for line in remove_header(output):
        return int(line.strip())


def drop_database(db, really_drop=False):
    sql = "drop database `%s`;" % db
    cmd = MYSQL_CMD_FORMAT % sql
    print "dropping database:", db
    if really_drop:
        status, output = commands.getstatusoutput(cmd)
        if status != 0:
            print "Error! cmd:", cmd


def get_distinct_values(db, table):
    values = set()
    sql = "use %s; select distinct(val) from %s;" % (db, table)
    cmd = MYSQL_CMD_FORMAT % sql
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        print "Error! cmd:", cmd
        return set()

    for line in remove_header(output):
        values.add(line.strip())

    return values


if __name__ == "__main__":
    pass