# -*- coding: utf-8 -*-
import re

from sqlalchemy import MetaData
from sqlalchemy.sql.sqltypes import NullType

from l_search import models
from l_search.handlers.meta_operation import Meta
from l_search.handlers.source_meta_operate.utils.db_session import DBSession
from l_search.utils.logger import Logger

logger = Logger()


class MetaDetector:
    def __init__(self, domain, db_type, default_db, db_schema=None):

        connection_info = models.DBConnect.get_by_domain(domain=domain,
                                                         db_type=db_type,
                                                         default_db=default_db,
                                                         db_schema=db_schema,
                                                         is_all=False)

        self.session = DBSession(connection_info=connection_info)
        logger.info("开始连接目标数据库")
        self.source_meta_data = MetaData()
        self.source_meta_data.reflect(bind=self.session.engine)
        logger.info("目标数据库连接完成")

        if db_type in ["postgresql", "greenplum"]:
            if db_schema is None:
                db_schema = "public"
            else:
                self.source_meta_data.reflect(schema=db_schema)
            self.db_schema = db_schema

    @staticmethod
    def is_extract_filter(column_data):
        result = False
        if column_data.name == "update_ts":
            result = True
        return result

    @staticmethod
    def is_primary(column_data):
        return column_data.primary_key

    @staticmethod
    def init_column_type(column_data):
        column_type = ""
        column_length = ""
        if not isinstance(column_data.db_type, NullType):
            try:
                column_type = str(re.sub("(\().*?(\))", "", str(column_data.db_type))).lower()
                column_length = str(column_data.db_type.length)
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
                           }
            i += 1
            columns_info_list.append(column_info)

            if self.is_primary(c):
                column_info["is_primary"] = True
                result["table_primary_id"] = c.name

                if column_type not in ["integer"]:
                    result["table_primary_id_is_int"] = False

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
        list_tables = self.source_meta_data.table

        if tables is None and table_name_prefix is None:
            is_all = True
        else:
            is_all = False

        result = {}

        for t in list_tables:
            if is_all:
                table_detail_info = self.detector_table(list_tables[t])
            else:
                if isinstance(tables, list) and t in tables:
                    table_detail_info = self.detector_table(list_tables[t])
                elif table_name_prefix and t.startswith(table_name_prefix):
                    table_detail_info = self.detector_table(list_tables[t])

            result[list_tables[t].name] = Meta.add_table_info(table_detail_info)

        return result

    def check_meta(self):
        """
        遍历meta里面指定的表
	    查看结构是否存在问题
        """

        store_table_info = models.TableDetail.get_tables(domain=self.domain,
                                                         type=self.domain_object_type)
        list_tables = [x.table_info for x in store_table_info]
        return self.detector_schema(tables=list_tables)

    def execute_select_sql(self, sql_text):
        session = self.session.get_session()
        execute_data = session.execute(sql_text).all()
        return [dict(row) for row in execute_data]
