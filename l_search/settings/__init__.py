# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""

import logging

SQLALCHEMY_DATABASE_URI = "mysql://metaadmin:meta4DWMM@192.168.1.222:3306/whoosh_index?charset=utf8"

MSEARCH_INDEX_NAME = "whoosh_index"
MSEARCH_BACKEND = "whoosh"
MSEARCH_PRIMARY_KEY = "id"
MSEARCH_ENABLE = True
MSEARCH_LOGGER = logging.INFO
SQLALCHEMY_TRACK_MODIFICATIONS = True

PROXIES_COUNT = 1

SOURCE_DB_HOST = "192.168.1.107"
SOURCE_DB_PORT = "7601"
SOURCE_DB_DB = "datatrust"
SOURCE_DB_USER = "root"
SOURCE_DB_PWD = "leadmap1102"
