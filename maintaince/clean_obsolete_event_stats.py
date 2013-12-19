# coding: utf8
"""
清理事件统计中一个月前的数据
"""

import json
import datetime
from pymongo import MongoClient


DB_HOST = "192.168.1.144"
DB_PORT = 27021
DB_NAME = "user_info"
EVENT_COLLECTION_NAME = "event_count"
PROJECT_COLLECTION_NAME = "project"

PROJECT_ID_FIELD = "project_id"
DATE_FIELD = "date"


mongo_client = MongoClient(DB_HOST, DB_PORT)
db = mongo_client[DB_NAME]


def get_projects():
    project_collection = db[PROJECT_COLLECTION_NAME]
    project_doc = project_collection.find_one({"type": "project"})
    project_info = project_doc["info"]
    d = json.loads(project_info)
    return d.keys()


def clean_obsolete_stats_for_project(project, before_day, clean=False):
    stats_collection = db[EVENT_COLLECTION_NAME]
    query = {PROJECT_ID_FIELD: project, DATE_FIELD: {"$lt": before_day}}
    count = stats_collection.find(query).count()
    print "total " + str(count) + " obsolete documents for project " + project
    if clean:
        print "cleaning..."
        stats_collection.remove(query)


if __name__ == "__main__":
    today = datetime.date.today()
    today = long(str(today).replace("-", ""))
    for project in get_projects():
        clean_obsolete_stats_for_project(project, today, clean=False)
