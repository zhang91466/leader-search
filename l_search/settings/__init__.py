# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""

import logging

SQLALCHEMY_DATABASE_URI = "mysql://root:leadmap1102@192.168.1.107:7601/metadata_l_search?charset=utf8mb4"

META_DB_HOST = "192.168.1.222"
META_DB_PORT = "3306"
META_DB_DB = "metadata_l_search"
META_DB_USER = "metaadmin"
META_DB_PWD = "meta4DWMM"

PROXIES_COUNT = 1

SQLALCHEMY_TRACK_MODIFICATIONS = False