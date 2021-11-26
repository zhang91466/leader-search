# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""
from .base import db, search
import uuid
from sqlalchemy.inspection import inspect


class FullTest(db.Model):
    __tablename__ = 'full_test_index'
    __searchable__ = ['title', 'content']

    id = db.Column(db.String(50), primary_key=True)
    title = db.Column(db.String(50))
    content = db.Column(db.Text)

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
        search.create_index(FullTest)

    @classmethod
    def search_index(cls, search_text, limit=20):
        results = cls.query.msearch(query="{keyword}*".format(keyword=search_text), fields=['content'], limit=limit).all()
        return results
