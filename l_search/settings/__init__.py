# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""
from l_search.models import DBObjectType

SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://postgres:123456xxx@192.168.1.55:5432/l_search"

ODS_SCHEMA_NAME = "ods"

PROXIES_COUNT = 1

SQLALCHEMY_TRACK_MODIFICATIONS = False

CELERY_BROKER_URL = "redis://192.168.1.224:6379/2"
CELERY_RESULT_BACKEND = "redis://192.168.1.224:6379/2"

SOURCE_DB_CONNECTION_URL = {DBObjectType("greenplum").value: {"connect_prefix": "postgresql+psycopg2",
                                                              "remark": ""},
                            DBObjectType("postgresql").value: {"connect_prefix": "postgresql+psycopg2",
                                                               "remark": ""},
                            DBObjectType("mysql").value: {"connect_prefix": "mysql",
                                                          "remark": "?character_set_server=utf8mb4"},
                            DBObjectType("mysql").value: {"connect_prefix": "mysql",
                                                          "remark": "?charset=utf8"},
                            DBObjectType("mssql").value: {"connect_prefix": "mssql+pymssql",
                                                          "remark": ""},
                            }

STRING_COLUMN_TYPE = ["varchar", "string", "text", "char"]

EXTRACT_FILTER_COLUMN_NAME = ["update_ts"]

GEO_COLUMN_NAME = ["shape", "geom"]

SWITCH_DIFF_DB_COLUMN_TYPE_ACCORDING_PG = {"varchar": ["varchar", "char", "nvarchar"],
                                           "integer": ["int", "smallint", "integer", "bigint"],
                                           "numeric": ["float", "numeric"],
                                           "text": ["text", "xml"],
                                           "timestamp": ["timestamp", "datetime", "datetime2"],
                                           "geometry": ["geometry"]
                                           }
