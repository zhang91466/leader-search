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
    def __init__(self, domain, type, db_schema=None):
        self.domain = domain
        self.domain_object_type = type
        Meta.domain = domain
        Meta.db_object_type = type
        self.session = DBSession(self.domain, self.domain_object_type)
        logger.info("开始连接目标数据库")
        self.source_meta_data = MetaData()
        self.source_meta_data.reflect(bind=self.session.engine)
        logger.info("目标数据库连接完成")

        if type in ["postgresql", "greenplum"]:
            if not db_schema:
                db_schema = "public"
            else:
                self.source_meta_data.reflect(schema=db_schema)
            self.db_schema = db_schema
        else:
            self.db_schema = self.session.connect_info["db_name"]

    def is_extract_filter(self, column_data):
        result = 0
        if column_data.name == "update_ts":
            result = 1
        return result

    def is_primary(self, column_data):
        return int(column_data.primary_key)

    def init_column_type(self, column_data):
        ctype = ""
        clength = ""
        if not isinstance(column_data.type, NullType):
            try:
                ctype = str(re.sub("(\().*?(\))", "", str(column_data.type))).lower()
                clength = str(column_data.type.length)
            except:
                pass
        else:
            ctype = "text"

        return ctype, clength

    def detector_table(self, table_object):
        """
        查看表里的列信息
        :param table_object: sqlalchemy table model
        :return:
        """
        columns_info_list = []
        i = 1
        for c in table_object.columns:
            column_type, column_type_length = self.init_column_type(c)
            column_info = {"default_db": self.db_schema,
                           "table_name": str(c.table.fullname).replace("%s." % self.db_schema, ""),
                           "column_name": c.name,
                           "column_type": column_type,
                           "column_type_length": column_type_length,
                           "column_comment": c.comment,
                           "column_position": i,
                           "is_extract": 1,
                           "is_primary": self.is_primary(c),
                           "is_extract_filter": self.is_extract_filter(c),
                           }
            i += 1
            columns_info_list.append(column_info)

        return columns_info_list

    def detector_schema(self, tables=None, table_name_prefix=None):
        """
        查看库多少表
        :param tables: 查询的表名 list
        :param table_name_prefix: 表名前缀里面有
        :return:
        """

        def detector_table():
            column_info_list = self.detector_table(list_tables[t])
            table_list[list_tables[t].name] = column_info_list
            return column_info_list

        column_info_list = []
        list_tables = self.source_meta_data.tables
        table_list = {}

        if tables is None and table_name_prefix is None:
            is_all = True
        else:
            is_all = False

        for t in list_tables:
            if is_all:
                column_info_list = detector_table()
            else:
                if isinstance(tables, list) and t in tables:
                    column_info_list = detector_table()
                elif table_name_prefix and t.startswith(table_name_prefix):
                    column_info_list = detector_table()

            if len(column_info_list) > 0:
                logger.info("Got %s's meta" % t)
                Meta.add_table_info(column_info_list)
                column_info_list = []

        return table_list

    def check_meta(self):
        """
        遍历meta里面指定的表
	    查看结构是否存在问题
        """

        store_table_info = models.DBMetadata.get_tables(domain=self.domain,
                                                             type=self.domain_object_type)
        list_tables = [x.table_name for x in store_table_info]
        return self.detector_schema(tables=list_tables)

    def execute_select_sql(self, sql_text):
        session = self.session.get_session()
        execute_data = session.execute(sql_text).all()
        return [dict(row) for row in execute_data]