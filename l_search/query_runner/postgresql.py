# -*- coding: UTF-8 -*-
"""
@time:2022/4/20
@author:simonzhang
@file:postgresql
"""
from l_search.query_runner import BasicQueryRunner
from l_search.utils.logger import Logger
from l_search.query_runner import register
from l_search.query_runner import models, settings

logger = Logger()


class Postgresql(BasicQueryRunner):

    def select_stmt(self):

        col_list = models.TableDetail.get_table_detail(table_info=self.table_info,
                                                       is_extract=True)

        col_str = ""
        geo_col = None
        for col in col_list:
            column_name = col.column_name
            col_str += "\"%s\"," % column_name

        if self.table_info.connection.db_schema is None:
            pg_schema = "public"
        else:
            pg_schema = self.table_info.connection.db_schema

        select_stmt = "select %(col_str)s from %(schema)s.%(table_name)s" % {"col_str": col_str[:-1],
                                                                             "schema": pg_schema,
                                                                             "table_name": self.table_name()}

        return select_stmt, geo_col

    def extract(self, increment=True):
        logger.info("Postgresql: extracting")
        super().extract(increment=increment)


register(Postgresql)
