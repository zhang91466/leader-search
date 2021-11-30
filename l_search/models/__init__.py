# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""
from .base import db, search, Column, InsertObject
import uuid
from sqlalchemy.inspection import inspect


class FullTest(db.Model):
    __tablename__ = "full_test_index_test"
    __searchable__ = ["title", "content"]

    id = Column(db.String(50), primary_key=True)
    title = Column(db.String(50))
    content = Column(db.Text)

    def __repr__(self):
        return '<FullText:{}>'.format(self.title)

    @classmethod
    def create(cls, title, content):
        new = cls(id=uuid.uuid4().hex,
                  title=title,
                  content=content)
        db.session.add(new)
        db.session.commit()

    @classmethod
    def update_search_index(cls):
        search.create_index(FullTest, update=True)

    @classmethod
    def search_index(cls, search_text, limit=20):
        results = cls.query.msearch(query="{keyword}*".format(keyword=search_text), fields=['content'], limit=limit).all()
        return results


class FullTextIndex(db.Model, InsertObject):
    __tablename__ = "full_text_index"
    __searchable__ = ["row_content"]

    id = Column(db.String(50), primary_key=True)
    domain = Column(db.String(150))
    db_object_type = Column(db.String(150))
    block_name = Column(db.String(500))
    block_key = Column(db.String(500))
    db_name = Column(db.String(500))
    table_name = Column(db.String(500))
    table_primary_id = Column(db.String(150))
    table_extract_col = Column(db.String(150))
    row_content = Column(db.Text())
    #
    # def __repr__(self):
    #     return '<FullText:{}>'.format(self.title)
