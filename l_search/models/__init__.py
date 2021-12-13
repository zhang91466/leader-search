# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""
from .base import db, Column, InsertObject, TimestampMixin
import uuid
import enum
from sqlalchemy.inspection import inspect
from .mysql_full_text_search import FullText, FullTextSearch, FullTextMode
from sqlalchemy import or_, and_, func


class DBObjectType(enum.Enum):
    mysql = "mysql"
    postgres = "postgresql"


class ExtractDataInfo(db.Model, InsertObject, TimestampMixin):
    __tablename__ = "extract_data_info"

    id = Column(db.Integer, primary_key=True, autoincrement=True)
    domain = Column(db.String(150))
    db_object_type = Column(db.String(150))
    db_name = Column(db.String(500))
    table_name = Column(db.String(500))
    table_primary_id = Column(db.String(150), nullable=True)
    table_extract_col = Column(db.String(150), nullable=True)
    latest_table_primary_id = Column(db.String(150), nullable=True)
    latest_extract_date = Column(db.DateTime(True), nullable=True)

    @classmethod
    def get_by_table_name(cls, domain, db_object_type, db_name, table_name):
        select_query = cls.query.filter(and_(cls.domain == domain,
                                             cls.db_object_type == db_object_type,
                                             cls.db_name == db_name,
                                             cls.table_name == table_name))
        return select_query.first()

    @classmethod
    def upsert(cls, **kwargs):

        is_exists = cls.get_by_table_name(domain=kwargs["domain"],
                                          db_object_type=kwargs["db_object_type"],
                                          db_name=kwargs["db_name"],
                                          table_name=kwargs["table_name"])
        if is_exists:
            if "table_primary_id" in kwargs and is_exists.table_primary_id != kwargs["table_primary_id"]:
                is_exists.table_primary_id = kwargs["table_primary_id"]

            if "table_extract_col" in kwargs and is_exists.table_extract_col != kwargs["table_extract_col"]:
                is_exists.table_extract_col = kwargs["table_extract_col"]

            if "latest_table_primary_id" in kwargs and is_exists.latest_table_primary_id != kwargs[
                "latest_table_primary_id"]:
                is_exists.latest_table_primary_id = kwargs["latest_table_primary_id"]

            if "latest_extract_date" in kwargs and is_exists.latest_extract_date != kwargs["latest_extract_date"]:
                is_exists.latest_extract_date = kwargs["latest_extract_date"]

            db.session.add(is_exists)
            db.session.commit()
            return is_exists
        else:
            return cls.create(**kwargs)


class FullTextIndex(db.Model, InsertObject, FullText):
    __tablename__ = "full_text_index"
    __fulltext_columns__ = ("row_content",)

    id = Column(db.String(300), primary_key=True)
    extract_data_info_id = Column(db.Integer, db.ForeignKey("extract_data_info.id"))
    extract_data_info = db.relationship(ExtractDataInfo, backref="fulltextindex_extractdatainfo")
    block_name = Column(db.String(500))
    block_key = Column(db.String(500))
    row_content = Column(db.Text())

    @classmethod
    def search_index(cls,
                     domain,
                     db_object_type,
                     search_text,
                     block_name=None,
                     block_key=None):

        search_query = cls.query.filter(
            and_(
                cls.extract_data_info.has(ExtractDataInfo.domain == domain),
                cls.extract_data_info.has(ExtractDataInfo.db_object_type == db_object_type),
                FullTextSearch(search_text, cls, FullTextMode.DEFAULT))
        )

        if block_name:
            search_query = search_query.filter(cls.block_name == block_name)

        if block_key:
            search_query = search_query.filter(cls.block_key == block_key)

        return search_query.all()
