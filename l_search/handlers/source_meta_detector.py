# -*- coding: utf-8 -*-
import re

from sqlalchemy import MetaData, create_engine
from sqlalchemy.sql.sqltypes import NullType
from contextlib import contextmanager
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from l_search import settings
from l_search import models
from l_search.handlers.meta_operation import Meta
from l_search.utils.logger import Logger

logger = Logger()


class DBSession:
    def __init__(self, connection_info):
        self.connect_info = connection_info

        if self.connect_info.db_type in settings.SOURCE_DB_CONNECTION_URL:
            get_part_of_connect_string = settings.SOURCE_DB_CONNECTION_URL[self.connect_info.db_type]
        else:
            raise "%s not support" % self.connect_info.print_name()

        engine_connect_string = '%s://%s:%s@%s:%s/%s%s' % (get_part_of_connect_string["connect_prefix"],
                                                           self.connect_info.account,
                                                           self.connect_info.pwd,
                                                           self.connect_info.host,
                                                           self.connect_info.port,
                                                           self.connect_info.default_db,
                                                           get_part_of_connect_string["remark"])

        self.engine = create_engine(engine_connect_string)
        self.session = Session(self.engine, future=True)


class MetaDetector:
    def __init__(self, connection_info):

        self.connection_info = connection_info
        self.connection = DBSession(connection_info=self.connection_info)
        logger.info("开始连接目标数据库")
        self.source_meta_data = MetaData()

        if connection_info.db_type in [models.DBObjectType("greenplum").value,
                                       models.DBObjectType("postgresql").value]:
            if connection_info.db_schema is None:
                self.db_schema = "public"
            else:
                self.db_schema = connection_info.db_schema

            self.source_meta_data.reflect(bind=self.connection.engine, schema=connection_info.db_schema)
        else:
            self.source_meta_data.reflect(bind=self.connection.engine)

        logger.info("目标数据库连接完成")

    @staticmethod
    def is_extract_filter(column_data):
        result = False
        if column_data.name in settings.EXTRACT_FILTER_COLUMN_NAME:
            result = True
        return result

    @staticmethod
    def init_column_type(column_data):
        column_type = ""
        column_length = ""

        if str(column_data.name).lower() in settings.GEO_COLUMN_NAME:
            column_type = "geometry"
            column_length = ""
        else:
            if not isinstance(column_data.type, NullType):
                try:
                    column_type = str(column_data.type).split(" ")[0]
                    column_type = str(re.sub("(\().*?(\))", "", column_type)).lower()
                    column_length = str(column_data.type.length)
                except:
                    pass
            else:
                column_type = "text"

        return column_type, column_length

    def detector_table(self, table_object):
        """
        查看表里的列信息
        :param table_object: sqlalchemy table model
        :return:
        """
        result = {"table_name": table_object.name}
        columns_info_list = []
        i = 1
        for c in table_object.columns:
            column_type, column_type_length = self.init_column_type(c)
            column_info = {
                "column_name": c.name,
                "column_type": column_type,
                "column_type_length": column_type_length,
                "column_comment": c.comment,
                "column_position": i,
                "is_primary": c.primary_key
            }
            i += 1
            columns_info_list.append(column_info)

            if self.is_extract_filter(c):
                result["table_extract_col"] = c.name

        result["columns"] = columns_info_list

        return result

    def detector_schema(self, tables=None, table_name_prefix=None):
        """
        查看库多少表,并写入数据库
        :param tables: 查询的表名 list
        :param table_name_prefix: 表名前缀里面有
        :return:
        """

        table_detail_info = []
        list_tables = self.source_meta_data.tables

        if tables is None and table_name_prefix is None:
            is_all = True
        else:
            is_all = False

        result = []

        for t in list_tables:
            if is_all:
                table_detail_info = self.detector_table(list_tables[t])
            else:
                if isinstance(tables, list) and t in tables:
                    table_detail_info = self.detector_table(list_tables[t])
                elif table_name_prefix and t.startswith(table_name_prefix):
                    table_detail_info = self.detector_table(list_tables[t])

            result.append(Meta.add_table_info(connection_info=self.connection_info,
                                              input_meta=table_detail_info))

        return result

    def check_meta(self):
        """
        遍历meta里面指定的表
	    查看结构是否存在问题
        """
        store_table_info = models.TableInfo.get_tables(connection_info=self.connection_info)
        list_tables = [x.table_info for x in store_table_info]
        return self.detector_schema(tables=list_tables)

    def execute_select_sql(self, sql_text):
        session = self.connection.session
        execute_data = session.execute(sql_text).all()
        return [dict(row) for row in execute_data]
