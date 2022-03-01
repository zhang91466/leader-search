# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""
from l_search.models import DBObjectType

SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://postgres:123456xxx@192.168.1.55:5432/l_search"

PROXIES_COUNT = 1

SQLALCHEMY_TRACK_MODIFICATIONS = False

CELERY_BROKER_URL = "redis://192.168.1.224:6379/2"
CELERY_RESULT_BACKEND = "redis://192.168.1.224:6379/2"

STRING_COLUMN_TYPE = ["varchar", "string", "text", "char"]

EXTRACT_FILTER_COLUMN_NAME = ["update_ts"]

SOURCE_DB_CONNECTION_URL = {DBObjectType("greenplum"): {"connect_prefix": "postgresql+psycopg2",
                                                        "remark": ""}}
