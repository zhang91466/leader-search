# -*- coding: utf-8 -*-
import re

from sqlalchemy import MetaData
from sqlalchemy.sql.sqltypes import NullType

from l_search import settings
from l_search import models
from l_search.handlers.meta_operation import Meta
from l_search.models.extract_table_models import DBSession
from l_search.utils.logger import Logger
from l_search.query_runner import get_query_runner

logger = Logger()


class MetaDetector:
    def __init__(self, connection_info):

        self.connection_info = connection_info
        self.connection = DBSession(connection_info=self.connection_info)
        logger.info("开始连接目标数据库")
        self.source_meta_data = MetaData()
        self.session = self.connection.session

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

        self.query_runner = get_query_runner(query_runner_type=connection_info.db_type,
                                             table_info=None)

    @staticmethod
    def is_extract_filter(column_data):
        result = False
        if str(column_data.name).lower() in settings.EXTRACT_FILTER_COLUMN_NAME:
            result = True
        return result

    def init_column_type(self, table_name, column_data):
        column_type = ""
        column_length = ""

        if str(column_data.name).lower() in settings.GEO_COLUMN_NAME:

            column_type = "geometry"
            column_length = ""

            # check geo z
            check_geo_z_stat = self.query_runner.get_check_geo_z_stat(geo_col=column_data.name,
                                                                      table_name=table_name)
            if check_geo_z_stat is not None:
                try:
                    one_row = self.session.execute(check_geo_z_stat).first()
                except Exception as e:
                    logger.error("Check geo z error: %s" % e)
                    one_row = None

                if one_row:
                    from shapely.wkt import loads
                    one_row_geo_load = loads(one_row[settings.GEO_COLUMN_NAME_STAG])
                    if one_row_geo_load.has_z:
                        column_type = "geometryZ"

        else:
            if not isinstance(column_data.type, NullType):
                try:
                    column_type = str(column_data.type).split(")")

                    if len(column_type) > 1:
                        column_type = column_type[0].lower() + ")"
                    else:
                        column_type = column_type[0].split(" ")[0].lower()

                    column_length = re.findall(r"\((.*?)\)", column_type)[0]
                    column_type = str(re.sub(r"(\().*?(\))", "", column_type))
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
        result = {"table_name": table_object.name
                  }
        columns_info_list = []
        i = 1
        for c in table_object.columns:
            column_type, column_type_length = self.init_column_type(table_name=table_object.name,
                                                                    column_data=c)
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
        logger.info("Detector schema start")
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
