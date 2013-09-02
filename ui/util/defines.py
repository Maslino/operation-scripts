# coding: utf8

from collections import namedtuple

PROP_DB = "db"
PROP_NAME = "prop_name"
PROP_TYPE = "prop_type"
PROP_FUNC = "prop_func"
PROP_ORIG = "prop_orig"
PROP_ALIAS = "prop_alias"
PROP_SEGM = "prop_segm"

SYS_META = "sys_meta"
DATABASE_PREFIX = "16_"
COLD_USER_INFO = "cold_user_info"
GAME_TIME = "game_time"

MetaProp = namedtuple("MetaProp", (PROP_DB, PROP_NAME, PROP_TYPE, PROP_FUNC, PROP_ORIG, PROP_ALIAS, PROP_SEGM))