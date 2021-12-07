# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""
from .base import db, search, Column, InsertObject
import uuid
import enum
from sqlalchemy.inspection import inspect
from .mysql_full_text_search import FullText, FullTextSearch, FullTextMode
from sqlalchemy import or_, and_, func


class DBObjectType(enum.Enum):
    mysql = "mysql"
    postgres = "postgresql"


class FullTextIndex(db.Model, InsertObject, FullText):
    __tablename__ = "full_text_index"
    __fulltext_columns__ = ("row_content",)

    id = Column(db.String(300), primary_key=True)
    domain = Column(db.String(150))
    db_object_type = Column(db.String(150))
    block_name = Column(db.String(500))
    block_key = Column(db.String(500))
    db_name = Column(db.String(500))
    table_name = Column(db.String(500))
    table_primary_id = Column(db.String(150))
    table_extract_col = Column(db.String(150))
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
                cls.domain == domain,
                cls.db_object_type == db_object_type,
                FullTextSearch(search_text, cls, FullTextMode.DEFAULT))
        )

        if block_name:
            search_query = search_query.filter(cls.block_name == block_name)

        if block_key:
            search_query = search_query.filter(cls.block_key == block_key)

        return search_query.all()
