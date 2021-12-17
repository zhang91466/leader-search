# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""

SQLALCHEMY_DATABASE_URI = "mysql://root:leadmap1102@192.168.1.107:7601/metadata_l_search?charset=utf8mb4"

PROXIES_COUNT = 1

SQLALCHEMY_TRACK_MODIFICATIONS = False

CELERY_BROKER_URL = "redis://localhost:6379/2"
CELERY_RESULT_BACKEND = "redis://localhost:6379/2"

STRING_COLUMN_TYPE = ["varchar", "string", "text", "char"]