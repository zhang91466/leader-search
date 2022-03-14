# -*- coding: UTF-8 -*-
"""
@time:2022/3/11
@author:zhangwei
@file:__init__.py
"""
from l_search.utils.logger import Logger
from l_search.models.base import db
from l_search import models
from l_search import settings

logger = Logger()


def register(query_runner_class):
    global query_runners
    if query_runner_class.enabled():
        logger.debug(
            "Registering %s (%s) query runner." % (
                query_runner_class.name(),
                query_runner_class.type())
        )
        query_runners[query_runner_class.type()] = query_runner_class
    else:
        logger.debug(
            "%s query runner enabled but not supported, not registering. Either disable or install missing "
            "dependencies." % (query_runner_class.name())
        )


def get_query_runner(query_runner_type, configuration={}):
    query_runner_class = query_runners.get(query_runner_type, None)
    if query_runner_class is None:
        return None

    return query_runner_class(configuration)


class BasicQueryRunner:

    def __init__(self, configuration):
        self.configuration = configuration
        self.db_engine = db.engine
        self.chunk_size = settings.DATA_EXTRACT_CHUNK_SIZE

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def type(cls):
        return cls.__name__.lower()

    @classmethod
    def enabled(cls):
        return True

    @staticmethod
    def table_name(table_info):

        if table_info.connection.db_schema:
            table_name = "%s.%s" % (table_info.connection.db_schema, table_info.table_name)
        else:
            table_name = table_info.table_name

        return table_name

    def select_stmt(self, table_info):

        col_list = models.TableDetail.get_table_detail(table_info=table_info,
                                                       is_extract=True)

        col_str = ""
        geo_col = None
        for col in col_list:
            column_name = str(col.column_name).lower()
            if col.column_type == "geometry":
                geo_col = column_name
                col_str += ", %(geo_col)s"
            else:
                col_str += ", %s" % column_name

        select_stmt = "select %(col_str)s from %(table_name)s" % {"col_str": col_str,
                                                                  "table_name": self.table_name(table_info=table_info)}
        return select_stmt, geo_col
