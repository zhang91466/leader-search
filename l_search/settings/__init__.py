# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""
from l_search.models import DBObjectType

SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://postgres:123456xxx@192.168.1.225:6688/l_search"

CELERY_BROKER_URL = "redis://192.168.1.224:6379/2"
CELERY_RESULT_BACKEND = "redis://192.168.1.224:6379/2"

ODS_SCHEMA_NAME = "ods"

ODS_STAG_SCHEMA_NAME = "ods_stag"

PROXIES_COUNT = 1

SQLALCHEMY_TRACK_MODIFICATIONS = False

DATA_EXTRACT_CHUNK_SIZE = 100000

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

STRING_COLUMN_TYPE = ["varchar", "string", "text", "char"]

EXTRACT_FILTER_COLUMN_NAME = ["update_ts", "updatetime"]

GEO_COLUMN_NAME = ["shape", "geom"]

GEO_COLUMN_NAME_STAG = "geometry"

PERIOD_COLUMN_NAME = "period"

GEO_CRS_CODE = 4326

SWITCH_DIFF_DB_COLUMN_TYPE_ACCORDING_PG = {"varchar": ["varchar", "char", "nvarchar"],
                                           "integer": ["int", "smallint", "integer", "bigint"],
                                           "numeric": ["float", "numeric"],
                                           "text": ["text", "xml", "uniqueidentifier", "varbinary"],
                                           "timestamp": ["timestamp", "datetime", "datetime2"],
                                           "geometry": ["geometry"],
                                           "tsrange": ["tsrange"]
                                           }

SWITCH_DIFF_DB_COLUMN_TYPE_ACCORDING_PD = {"varchar": ["object", ""],
                                           "text": ["object", ""],
                                           "integer": ["int64", 0],
                                           "numeric": ["float64", 0],
                                           "timestamp": ["datetime64[ns]", "1900-01-01 00:00:00"],
                                           "geometry": ["geometry", ""]
                                           }


# Query Runners
default_query_runners = [
    "l_search.query_runner.mssql"
]