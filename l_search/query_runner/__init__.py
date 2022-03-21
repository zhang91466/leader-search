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
from l_search.models.extract_table_models import DBSession

logger = Logger()


def import_query_runners(query_runner_imports):
    for runner_import in query_runner_imports:
        __import__(runner_import)


query_runners = {}

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


def get_query_runner(query_runner_type, table_info):
    query_runner_class = query_runners.get(query_runner_type, None)
    if query_runner_class is None:
        return None

    return query_runner_class(table_info)


class BasicQueryRunner:

    def __init__(self, table_info):
        self.table_info = table_info
        self.db_engine = db.engine
        self.source_db_engine = None
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

    def set_connection(self):
        connection = DBSession(connection_info=self.table_info.connection)
        self.source_db_engine = connection.engine

    def table_name(self):

        if self.table_info.connection.db_schema:
            table_name = "%s.%s" % (self.table_info.connection.db_schema, self.table_info.table_name)
        else:
            table_name = self.table_info.table_name

        return table_name

    def select_stmt(self):

        col_list = models.TableDetail.get_table_detail(table_info=self.table_info,
                                                       is_extract=True)

        col_str = ""
        geo_col = None
        for col in col_list:
            column_name = col.column_name
            if col.column_type == "geometry":
                geo_col = column_name
                col_str += "%(geo_col)s,"
            else:
                col_str += "%s," % column_name

        select_stmt = "select top 100 %(col_str)s from %(table_name)s" % {"col_str": col_str[:-1],
                                                                  "table_name": self.table_name()}

        return select_stmt, geo_col

    def df_structure_arrangement(self, insert_data_df):
        table_schema = models.TableDetail.get_table_detail(table_info=self.table_info,
                                                           is_entity=True)
        table_schema_column_type = {}
        for col in table_schema:
            column_type = None
            for c_type_key, v in settings.SWITCH_DIFF_DB_COLUMN_TYPE_ACCORDING_PG.items():
                if col.column_type in v:
                    column_type = c_type_key
                    break

            if column_type is None:
                column_type = col.column_type

            table_schema_column_type[col.column_name] = column_type

        need_change_column_type = {}
        drop_column = []
        for c_name, c_type in insert_data_df.dtypes.items():

            if insert_data_df[c_name].isnull().all():
                drop_column.append(c_name)
                continue

            if c_name == "geometry":
                continue

            actual_type = settings.SWITCH_DIFF_DB_COLUMN_TYPE_ACCORDING_PD[table_schema_column_type[c_name]]
            if c_type != actual_type[0]:
                insert_data_df[c_name] = insert_data_df[c_name].fillna(actual_type[1])
                need_change_column_type[c_name] = actual_type[0]

        # 删除整列为nan的列
        insert_data_df = insert_data_df.drop(columns=drop_column)
        # 列格式与实际格式不符，进行转换, errors="ignore"
        insert_data_df = insert_data_df.astype(need_change_column_type)
        # 所有列命小写
        insert_data_df.columns = insert_data_df.columns.str.lower()

        return insert_data_df
