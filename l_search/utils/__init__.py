# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""
import datetime


def json_converter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()
