# coding: utf8

import commands
from defines import *
from db import get_databases, MYSQL_CMD_FORMAT


def pprint_meta(results):
    sorted_results = sorted(results, key=lambda e: e[3])
    header = (PROP_NAME, PROP_TYPE, PROP_FUNC, PROP_ORIG, PROP_ALIAS, PROP_SEGM)
    width = dict((i, len(x)) for i, x in enumerate(header))

    for r in sorted_results:
        width.update((i, max(width[i], len(x))) for i, x in enumerate(r))

    line = " | ".join(["%%-%ss" % width[i] for i in xrange(len(header))])
    print "\n".join([line % header,
                     "-|-".join([width[i] * '-' for i in xrange(len(header))]),
                     "\n".join(line % (a, b, c, d, e, f) for a, b, c, d, e, f in sorted_results)
                     ])


def export2txt(results):
    sorted_results = sorted(results, key=lambda e: e[3])
    header = (PROP_NAME, PROP_TYPE, PROP_FUNC, PROP_ORIG, PROP_ALIAS, PROP_SEGM)

    content = "\n".join(
        (
            "\t".join(header),
            "\n".join(["\t".join(r) for r in sorted_results])
        )
    )

    print content


def get_meta():
    meta = set()
    databases = get_databases()
    for db in databases:
        sql = "use %s; select * from %s;" % (db, SYS_META)
        cmd = MYSQL_CMD_FORMAT % sql
        status, output = commands.getstatusoutput(cmd)
        if status != 0:
            print "table(%s) not exists in db(%s)?" % (SYS_META, db)
            continue

        properties = set()
        for line in output.strip().split("\n"):
            items = line.strip().split()
            if len(items) != 6:
                print "table(%s) is empty in db(%s)? " % (SYS_META, db)
                continue

            if items[0] == PROP_NAME:
                continue

            properties.add(items[0])
            meta.add(MetaProp(**{
                PROP_DB: db,
                PROP_NAME: items[0],
                PROP_TYPE: items[1],
                PROP_FUNC: items[2],
                PROP_ORIG: items[3],
                PROP_ALIAS: items[4],
                PROP_SEGM: items[5],
            }
            ))

        sql = "use %s; show tables;" % db
        cmd = MYSQL_CMD_FORMAT % sql
        output = commands.getoutput(cmd)
        tables = set()
        for line in output.strip().split("\n"):
            if db in line or SYS_META in line:
                continue
            tables.add(line.strip())

        remain = tables - properties - set([COLD_USER_INFO, GAME_TIME])
        if len(remain) > 0:
            print "remain: ", remain

    return meta


def db_without_sys_meta():
    db_tables = {}
    databases = get_databases()
    for db in databases:
        sql = "use %s; show tables;" % db
        cmd = MYSQL_CMD_FORMAT % sql
        status, output = commands.getstatusoutput(cmd)
        if status != 0:
            print "Error! cmd:", cmd
            continue

        if SYS_META not in output:
            for line in output.strip().split("\n"):
                if db not in line:
                    table = line.strip()
                    if db in db_tables:
                        db_tables.get(db).append(table)
                    else:
                        db_tables.update({db: [table]})

    return db_tables