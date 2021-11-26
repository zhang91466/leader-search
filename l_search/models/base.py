# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:base
"""
import functools
from flask_sqlalchemy import SQLAlchemy
from flask_msearch import Search
from jieba.analyse import ChineseAnalyzer


db = SQLAlchemy()
db.configure_mappers()

Column = functools.partial(db.Column, nullable=False)

search = Search(db=db,
                analyzer=ChineseAnalyzer())