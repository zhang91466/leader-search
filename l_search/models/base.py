# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:base
"""
import functools
from flask_sqlalchemy import SQLAlchemy
from l_search.handlers.meta_operation import Meta
from l_search.utils.logger import Logger
from sqlalchemy.event import listens_for

logger = Logger()

db = SQLAlchemy()
db.configure_mappers()

Column = functools.partial(db.Column, nullable=False)


class InsertObject:
    @classmethod
    def create(cls, **kwargs):
        new = cls(**kwargs)
        db.session.add(new)
        db.session.commit()
        return new

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
                row_count = len(input_data)
                logger.info("Table %s insert count %d" % (cls.__table__, row_count))
                return row_count
        except Exception as e:
            logger.error("Bulk insert error: %s" % e)

        return False

class TimestampMixin(object):
    updated_at = Column(db.DateTime(True), default=db.func.now(), nullable=False)
    created_at = Column(db.DateTime(True), default=db.func.now(), nullable=False)


@listens_for(TimestampMixin, "before_update", propagate=True)
def timestamp_before_update(mapper, connection, target):
    # Check if we really want to update the updated_at value
    if hasattr(target, "skip_updated_at"):
        return

    target.updated_at = db.func.now()


