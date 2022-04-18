# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:simonzhang
@file:base
"""
import functools
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.event import listens_for

from l_search import settings
from l_search.utils.logger import Logger

logger = Logger()

db = SQLAlchemy()
db.configure_mappers()

Column = functools.partial(db.Column, nullable=False)


def create_ods_schema():
    db.session.execute("CREATE SCHEMA IF NOT EXISTS %s" % settings.ODS_SCHEMA_NAME)
    db.session.execute("CREATE SCHEMA IF NOT EXISTS %s" % settings.ODS_STAG_SCHEMA_NAME)
    db.session.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    db.session.execute("CREATE EXTENSION IF NOT EXISTS postgis_topology")
    db.session.execute("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch")
    db.session.execute("CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder")
    db.session.commit()


def drop_ods_schema():
    db.session.execute("DROP SCHEMA %s CASCADE" % settings.ODS_SCHEMA_NAME)
    db.session.execute("DROP SCHEMA %s CASCADE" % settings.ODS_STAG_SCHEMA_NAME)
    db.session.commit()


class InsertObject:
    @classmethod
    def create(cls, auto_commit=True, **kwargs):
        new = cls(**kwargs)
        db.session.add(new)

        if auto_commit:
            db.session.commit()
        else:
            db.session.flush()

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

    @classmethod
    def upsert_base(cls, input_data, col_not_in, update_index, is_commit=True):
        insert_stmt = insert(cls.__table__).values(input_data)

        update_columns = {col.name: col for col in insert_stmt.excluded if col.name not in col_not_in}

        # pg 特定写法
        upsert_stmt = insert_stmt.on_conflict_do_update(index_elements=update_index,
                                                        set_=update_columns).returning(cls.id)

        execute_result = db.session.execute(upsert_stmt)

        if is_commit:
            db.session.commit()
        else:
            db.session.flush()

        return_id_list = [row.id for row in execute_result]

        get_return_id_result = cls.query.filter(cls.id.in_(return_id_list)).all()

        return get_return_id_result


class TimestampMixin(object):
    updated_at = Column(db.DateTime(True), default=db.func.now(), nullable=False)
    created_at = Column(db.DateTime(True), default=db.func.now(), nullable=False)


@listens_for(TimestampMixin, "before_update", propagate=True)
def timestamp_before_update(mapper, connection, target):
    # Check if we really want to update the updated_at value
    if hasattr(target, "skip_updated_at"):
        return

    target.updated_at = db.func.now()
