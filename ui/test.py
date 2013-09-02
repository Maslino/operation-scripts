# coding: utf8

from util.db import get_databases, get_distinct_values, count_table


def check_prop_values(prop_name):
    databases = get_databases()
    for db in databases:
        count = count_table(db, prop_name)
        values = get_distinct_values(db, prop_name)
        length = len(values)
        if length > 0:
            print "db: %s, table: %s, count: %s, distinct values: %s" % (db, prop_name, count, length)
        if length < 1000:
            for value in list(values)[0: 10]:
                print value


def test():
    for prop_name in ("identifier", "platform", "version"):
        check_prop_values(prop_name)


if __name__ == "__main__":
    test()