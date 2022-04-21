# -*- coding: UTF-8 -*-
"""
@time:2022/3/11
@author:simonzhang
@file:__init__.py
"""
from l_search.utils.logger import Logger
from l_search.models.base import db
from l_search import models
from l_search import settings
from l_search.models.extract_table_models import DBSession

from werkzeug.exceptions import BadRequest
import pandas as pd

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

    def increment_where_stmt(self):
        if self.table_info.latest_extract_date is not None:
            where_stmt = " where %(update_ts_col)s > '%(latest_update_ts)s'" % {
                "update_ts_col": self.table_info.table_extract_col,
                "latest_update_ts": self.table_info.latest_extract_date.strftime("%Y-%m-%d %H:%M:%S")}
        else:
            where_stmt = ""

        return where_stmt

    def df_structure_arrangement(self, insert_data_df):
        """
        when df column all value is nan, then column type is chaotic
        so need change df column type consistent with created table in pg

        step1: get table schema and change to pg schema
        step2: set df schema same as pg schema
        :param insert_data_df:
        :return:
        """
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

    def select_stmt(self):

        col_list = models.TableDetail.get_table_detail(table_info=self.table_info,
                                                       is_extract=True)

        col_str = ""
        geo_col = None
        for col in col_list:
            column_name = col.column_name
            if col.column_type in settings.GEO_COLUMN_TYPE:
                geo_col = column_name
                col_str += "%(geo_col)s,"
            else:
                col_str += "%s," % column_name

        select_stmt = "select %(col_str)s from %(table_name)s" % {"col_str": col_str[:-1],
                                                                  "table_name": self.table_name()}

        return select_stmt, geo_col

    def get_check_geo_z_stat(self, geo_col, table_name):
        return None

    def extract(self, increment=True):
        """
        通用方法不支持geo数据的抽取
        :param increment:
        :return:
        """
        table_name = self.table_info.entity_table_name()
        logger.info("%s start extract" % table_name)

        extract_stmt, geo_col = self.select_stmt()

        if geo_col is None:

            if increment is True:
                extract_stmt = extract_stmt + self.increment_where_stmt()

            logger.info("%s extract stmt %s" % (table_name, extract_stmt))

            try:
                for count, partial_df in enumerate(
                        pd.read_sql(extract_stmt, self.source_db_engine, chunksize=self.chunk_size)):

                    if len(partial_df) == 0:
                        # 没有数据直接退出
                        break

                    to_db_para = {"con": self.db_engine,
                                  "if_exists": "append",
                                  "schema": settings.ODS_STAG_SCHEMA_NAME,
                                  "name": table_name,
                                  "index": False}

                    logger.info("%s extracting loop %d" % (table_name, count))

                    partial_df = self.df_structure_arrangement(insert_data_df=partial_df)

                    partial_df.to_sql(**to_db_para)

                logger.info("%s extract end" % table_name)
            except Exception as e:
                error_message = "%s extract failed. Error Info: %s" % (table_name, e)
                logger.error(error_message)
                return error_message

    def row_count(self):
        count_stmt = "select count(*) as row_cnt from %s" % self.table_name()
        count_df = pd.read_sql(count_stmt, self.source_db_engine)
        return count_df.iloc[-1]["row_cnt"]

    def extract_primary_id(self):
        logger.info("%s start extract primary id" % self.table_name())
        primary_column_name = models.TableDetail.get_table_detail(table_info=self.table_info,
                                                                  table_primary=True)

        select_stmt = "select %(primary_id)s from %(table_name)s" % {
            "primary_id": ",".join([str(r.column_name).lower() for r in primary_column_name]),
            "table_name": self.table_name()}

        logger.info("%s start extract primary id sql: %s" % (self.table_name(), select_stmt))
        try:
            for count, partial_df in enumerate(
                    pd.read_sql(select_stmt, self.source_db_engine, chunksize=self.chunk_size)):
                logger.info("%s extracting loop %d" % (self.table_info.table_name, count))

                partial_df.to_sql(con=self.db_engine,
                                  if_exists="append",
                                  schema=settings.ODS_STAG_SCHEMA_NAME,
                                  name=self.table_info.entity_table_name(),
                                  index=False)
        except Exception as e:
            error_message = "%s extract failed with primary. Error Info: %s" % (self.table_info.table_name, e)
            logger.error(error_message)
            raise BadRequest(error_message)

    def check_source_table_exists(self):
        logger.info("Check table %s is exists in source db" % self.table_info.table_name)
        check_stat = "select 1 from %s" % self.table_info.table_name

        try:
            pd.read_sql(check_stat, self.source_db_engine)
            return True
        except Exception as e:
            models.TableInfo.delete_table_info(table_info=self.table_info)
            logger.warn("Table %s has been moved from source db" % self.table_info.table_name)
            return False
