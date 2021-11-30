# -*- coding: UTF-8 -*-
"""
@time:2021/11/30
@author:zhangwei
@file:meta_operation
"""
from l_search.utils.logger import Logger
from l_search import models
from DWMM import set_connect
from DWMM.operate.connect_info import ConnectionOperate
from DWMM.operate.metadata_info import MetadataOperate
from DWMM.source_meta_operate.handle.meta_handle import MetaDetector

logger = Logger()


class Meta:
    domain = ""
    db_object_type = ""
    db_name = ""

    def init_app(self, app):
        """
        元数据管理系统的对应数据库必须是mysql
        :param app:
        :return:
        """
        app.config.setdefault("SOURCE_DB_HOST", None)
        app.config.setdefault("SOURCE_DB_PORT", None)
        app.config.setdefault("SOURCE_DB_DB", None)
        app.config.setdefault("SOURCE_DB_USER", None)
        app.config.setdefault("SOURCE_DB_PWD", None)

        set_connect(host=app.config["SOURCE_DB_HOST"],
                    port=app.config["SOURCE_DB_PORT"],
                    db=app.config["SOURCE_DB_DB"],
                    user=app.config["SOURCE_DB_USER"],
                    pwd=app.config["SOURCE_DB_PWD"])

    @classmethod
    def get_connection_info(cls, domain, db_object_type):
        return ConnectionOperate.get_info(subject_domain=domain, object_type=db_object_type)

    @classmethod
    def create_extract_table_sql(cls,
                                 table_name,
                                 primary_column_name=None,
                                 extract_column_name=None):
        operate = MetadataOperate(subject_domain=cls.domain, object_type=cls.db_object_type)

        table_schema = operate.get_table_info(db_name=cls.db_name,
                                              table_name=table_name,
                                              is_extract=None
                                              )
        concat_str = ""
        for col in table_schema:
            concat_str.join("%s, " % col["column_name"])

            if col["is_primary"] == 1:
                primary_column_name = col["column_name"]

            if col["is_extract_filter"] == 1:
                extract_column_name = col["column_name"]

            concat_str = concat_str[:-1]

        if primary_column_name and extract_column_name:
            logger.debug("(%s.%s.%s) 表主键和抽取列都明确，生成抽取sql" % (cls.domain, cls.db_name, table_name))

            sql_select = """
            select 
            '%(domain)s' as domain
            ,'%(db_object_type)s' as db_object_type
            ,'%(db_name)s' as db_name
            ,'%(table_name)s' as table_name
            ,'%(table_primary_id)s' as table_primary_id
            ,'%(table_extract_col)s' as table_extract_col
            ,concat(%(row_content)s) as row_content
            
            """ % {
                "domain": cls.domain,
                "db_object_type": cls.db_object_type,
                "db_name": cls.db_name,
                "table_name": table_name,
                "table_primary_id": primary_column_name,
                "table_extract_col": extract_column_name,
                "row_content": concat_str
            }

            sql_from = """
            from %(table_name)s
            
            """

            return {"select": sql_select,
                    "from": sql_from}

        else:
            if primary_column_name is None:
                logger.info("(%s.%s.%s) 表主键不明确，无法生成抽取sql" % (cls.domain, cls.db_name, table_name))

            if extract_column_name is None:
                logger.info("(%s.%s.%s) 表抽取列不明确，无法生成抽取sql" % (cls.domain, cls.db_name, table_name))

            return None

    @classmethod
    def extract_data(cls,
                     table_name,
                     block_name="",
                     block_key="",
                     primary_column_name=None,
                     extract_column_name=None):

        execute_sql = cls.create_extract_table_sql(table_name=table_name,
                                                   primary_column_name=primary_column_name,
                                                   extract_column_name=extract_column_name)
        if execute_sql:
            meta_detector = MetaDetector(subject_domain=cls.domain, object_type=cls.db_object_type)

            sql_text = """
            %(select)s
            ,'%(block_name)s' as block_name
            ,'%(block_key)s' as block_key
            %(from)s
            """ % {"select": execute_sql["select"],
                   "block_name": block_name,
                   "block_key": block_key,
                   "from": execute_sql["from"]}
            logger.debug("(%s)表抽取sql: %s" % (table_name, sql_text))

            execute_data = meta_detector.execute_select_sql(sql_text=sql_text)

            models.FullTextIndex.bulk_insert(input_data=execute_data)
