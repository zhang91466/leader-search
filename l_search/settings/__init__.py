# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:simonzhang
@file:__init__.py
"""
import os

# app basic
PROXIES_COUNT = 1

LOGGER_LEVEL = os.environ.get("LSEARCH_LOGGER_LEVEL", "INFO")

CELERY_TASK_FORCE_EXPIRE_HOUR = 6
CELERY_TASK_FORCE_EXPIRE_SECOND = CELERY_TASK_FORCE_EXPIRE_HOUR * 3600

# db connect info
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "LSEARCH_DB_CONNECT_URL", "postgresql+psycopg2://postgres:123456xxx@192.168.1.225:6688/l_search")

SQLALCHEMY_POOL_SIZE = 5

SQLALCHEMY_POOL_TIMEOUT = 100

SQLALCHEMY_POOL_RECYCLE = 3600

_REDIS_URL = os.environ.get("REDIS_URL", "redis://192.168.1.225:6689/2")

REDIS_URL = os.environ.get("LSEARCH_REDIS_URL", _REDIS_URL)

CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", _REDIS_URL)

CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", _REDIS_URL)

# Query Runners
default_query_runners = [
    "l_search.query_runner.mssql"
]

# data extract
ODS_SCHEMA_NAME = "ods"

ODS_STAG_SCHEMA_NAME = "ods_stag"

SQLALCHEMY_TRACK_MODIFICATIONS = False

DATA_EXTRACT_CHUNK_SIZE = int(os.environ.get("EXTRACT_CHUNK_SIZE", 100000))

EXTRACT_FILTER_COLUMN_NAME = ["update_ts", "updatetime"]

GEO_COLUMN_NAME = ["shape", "geom"]

GEO_COLUMN_NAME_STAG = "geometry"

GEO_COLUMN_TYPE = ("geometry", "geometryZ")

PERIOD_COLUMN_NAME = "period"

# metadata column type mapping
from l_search.models import DBObjectType

SOURCE_DB_CONNECTION_URL = {DBObjectType("greenplum").value: {"connect_prefix": "postgresql+psycopg2",
                                                              "remark": ""},
                            DBObjectType("postgresql").value: {"connect_prefix": "postgresql+psycopg2",
                                                               "remark": ""},
                            DBObjectType("mariadb").value: {"connect_prefix": "mysql",
                                                            "remark": "?character_set_server=utf8mb4"},
                            DBObjectType("mysql").value: {"connect_prefix": "mysql",
                                                          "remark": "?charset=utf8"},
                            DBObjectType("mssql").value: {"connect_prefix": "mssql+pymssql",
                                                          "remark": ""},
                            }

GEO_CRS_CODE = int(os.environ.get("GEO_CRS_CODE", 4326))

SWITCH_DIFF_DB_COLUMN_TYPE_ACCORDING_PG = {"varchar": ["varchar", "char", "nvarchar"],
                                           "integer": ["int", "smallint", "integer", "bigint"],
                                           "numeric": ["float", "numeric"],
                                           "text": ["text", "xml", "uniqueidentifier", "varbinary"],
                                           "timestamp": ["timestamp", "datetime", "datetime2"],
                                           "geometry": ["geometry"],
                                           "geometryZ": ["geometryZ"],
                                           "tsrange": ["tsrange"]
                                           }

SWITCH_DIFF_DB_COLUMN_TYPE_ACCORDING_PD = {"varchar": ["object", ""],
                                           "text": ["object", ""],
                                           "integer": ["int64", 0],
                                           "numeric": ["float64", 0],
                                           "timestamp": ["datetime64[ns]", "1900-01-01 00:00:00"],
                                           "geometry": ["geometry", ""],
                                           "geometryZ": ["geometry", ""]
                                           }
