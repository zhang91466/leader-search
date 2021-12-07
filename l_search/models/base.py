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
from l_search.handlers.meta_operation import Meta
from l_search.utils.logger import Logger

logger = Logger()

db = SQLAlchemy()
db.configure_mappers()

Column = functools.partial(db.Column, nullable=False)

search = Search(db=db,
                analyzer=ChineseAnalyzer())

meta_info = Meta()


class InsertObject:
    @classmethod
    def create(cls, **kwargs):
        new = cls(**kwargs)
        db.session.add(new)
        db.session.commit()

    @classmethod
    def bulk_insert(cls, input_data):
        """
        数据批量插入到表
        :param input_data: df 或者 list of dict
        :return: None
        """
        try:
            if len(input_data) > 0 and isinstance(input_data, list):
                db.session.execute(cls.__table__.insert(), input_data)
                db.session.commit()
                logger.info("Table %s insert count %d" % (cls.__table__, len(input_data)))
                return True
        except Exception as e:
            logger.error("Bulk insert error: %s" % e)

        return False

