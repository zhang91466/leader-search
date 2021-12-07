# -*- coding: UTF-8 -*-
"""
@time:2021/12/6
@author:zhangwei
@file:mysql_full_text_search
"""
import re

from sqlalchemy import event
from sqlalchemy import Index
from sqlalchemy import literal
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm.mapper import Mapper
from sqlalchemy.schema import DDL
from sqlalchemy.sql.expression import ColumnClause


# ############################################
# DEPRECATED from SQLAlchemy>=1.4
# from sqlalchemy.sql.expression import ClauseElement
# https://github.com/mengzhuo/sqlalchemy-fulltext-search
# ############################################

class FullTextMode:
    BOOLEAN = "IN BOOLEAN MODE"
    NATURAL = "IN NATURAL LANGUAGE MODE"
    QUERY_EXPANSION = "WITH QUERY EXPANSION"
    DEFAULT = ""


MYSQL = "mysql"
MYSQL_BUILD_INDEX_QUERY = u"""ALTER TABLE {0.__tablename__} ADD FULLTEXT ({1}) WITH PARSER ngram"""
MYSQL_MATCH_AGAINST = u"""
                      MATCH ({0})
                      AGAINST ({1} {2})
                      """


def escape_quote(string):
    return re.sub(r"[\"\']+", "", string)


class FullTextSearch(ColumnClause):
    """
    Search FullText
    :param against: the search query
    :param table: the table needs to be query
    FullText support with in query, i.e.
    session.query(Foo).filter(FullTextSearch('Spam', Foo))
    """

    def __init__(self, against, model, mode=FullTextMode.DEFAULT):
        self.model = model
        self.against = literal(against)
        self.mode = mode


def get_table_name(element):
    if hasattr(element.model, "__table__"):
        return "`" + element.model.__table__.fullname + "`."
    return ""


@compiles(FullTextSearch, MYSQL)
def __mysql_fulltext_search(element, compiler, **kw):
    assert issubclass(element.model, FullText), "{0} not FullTextable".format(element.model)
    return MYSQL_MATCH_AGAINST.format(
        ", ".join([get_table_name(element) + column for column in element.model.__fulltext_columns__]),
        compiler.process(element.against),
        element.mode)


class FullText(object):
    """
    FullText Mixin object for SQLAlchemy
        class Foo(FullText, Base):
            __fulltext_columns__ = ('spam', 'ham')
            ...
    fulltext search spam and ham now
    """

    __fulltext_columns__ = tuple()

    @classmethod
    def build_fulltext(cls, table):
        """
        build up fulltext index after table is created
        """
        if FullText not in cls.__bases__:
            return
        if not cls.__fulltext_after_create__:
            return
        assert cls.__fulltext_columns__, "Model:{0.__name__} No FullText columns defined".format(cls)

        alter_ddl = DDL(MYSQL_BUILD_INDEX_QUERY.format(cls,
                                                       ", ".join((escape_quote(c)
                                                                  for c in cls.__fulltext_columns__)))
                        )

    """
    TODO: black magic in the future
    @classmethod
    @declared_attr
    def __contains__(*arg):
        return True
    """

    __fulltext_after_create__ = True


class FullTextForMigration(FullText):
    __fulltext_after_create__ = False

    @classmethod
    def index_fulltext(cls):
        """
        call like Index('idx_<__tablename__>_fulltext', *__fulltext_columns__, mysql_prefix='FULLTEXT')
        """
        assert cls.__tablename__, "Model:{0.__name__} No table name defined".format(cls)
        assert cls.__fulltext_columns__, "Model:{0.__name__} No FullText columns defined".format(cls)
        Index("idx_%s_fulltext" % cls.__tablename__,
              *(getattr(cls, c) for c in cls.__fulltext_columns__),
              mysql_prefix="FULLTEXT")

