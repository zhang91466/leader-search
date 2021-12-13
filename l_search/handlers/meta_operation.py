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
import hashlib

logger = Logger()


class Meta:
    domain = ""
    db_object_type = ""
    db_name = ""

    def init_app(self, app):
        """
        元数据管理系统的对应数据库必须是mysql
        :param app: flask app
        :return: None
        """
        app.config.setdefault("META_DB_HOST", None)
        app.config.setdefault("META_DB_PORT", None)
        app.config.setdefault("META_DB_DB", None)
        app.config.setdefault("META_DB_USER", None)
        app.config.setdefault("META_DB_PWD", None)

        set_connect(host=app.config["META_DB_HOST"],
                    port=app.config["META_DB_PORT"],
                    db=app.config["META_DB_DB"],
                    user=app.config["META_DB_USER"],
                    pwd=app.config["META_DB_PWD"])

    @classmethod
    def add_connection_info(cls, input_data):

        connection_id = None

        if "connection_id" in input_data:
            connection_id = input_data["connection_id"]

        if connection_id:

            for k in input_data.copy():
                if input_data[k] is None:
                    input_data.pop(k)

            connection_id = ConnectionOperate.modify_info(connection_id=connection_id, infos=input_data)
            add_result_info = ""
        else:

            infos = {"domain": input_data["domain"],
                     "type": models.DBObjectType[input_data["db_object_type"]].value,
                     "host": input_data["host"],
                     "port": input_data["port"],
                     "account": input_data["account"],
                     "pwd": input_data["pwd"],
                     "default_db": input_data["default_db"]}
            connection_id, add_result_info = ConnectionOperate.add_row(infos=infos)

        return {"connection_id": connection_id,
                "add_result_info": add_result_info}

    @classmethod
    def get_connection_info(cls, connection_id=None, domain=None, db_object_type=None):

        def change_key_name(data):
            data["connection_id"] = data.pop("id")
            data["domain"] = data.pop("subject_domain")
            data["db_object_type"] = data.pop("object_type")
            data["host"] = data.pop("object_host")
            data["port"] = data.pop("object_port")
            data["account"] = data.pop("object_account")
            data["pwd"] = data.pop("object_pwd")
            data["default_db"] = data.pop("db_name")

            data["db_object_type"] = models.DBObjectType(data["db_object_type"]).name
            return data

        if db_object_type:
            db_object_type = models.DBObjectType[db_object_type].value

        connection_info = ConnectionOperate.get_info(subject_domain=domain,
                                                     object_type=db_object_type,
                                                     connection_id=connection_id)
        if isinstance(connection_info, dict):
            connection_info = change_key_name(connection_info)
        else:
            connection_info = [change_key_name(c_data) for c_data in connection_info]

        return connection_info

    @classmethod
    def sync_table_schema(cls, table_list=None, table_name_prefix=None):
        meta_detector = MetaDetector(subject_domain=cls.domain,
                                     object_type=models.DBObjectType[cls.db_object_type].value)
        detector_schema_info = meta_detector.detector_schema(tables=table_list, table_name_prefix=table_name_prefix)
        return detector_schema_info

    @classmethod
    def get_table_meta(cls, db_name, table_name=None):
        operate = MetadataOperate(subject_domain=cls.domain, object_type=models.DBObjectType[cls.db_object_type].value)

        if table_name:
            get_info = {"table_name": table_name,
                        "data": operate.get_table_info(db_name=db_name, table_name=table_name, is_extract=None)}
        else:
            tables_in_db = operate.get_tables(db_name=db_name)

            get_info = []

            for table in tables_in_db:
                get_info.append({"table_name": table["table_name"],
                                 "data": operate.get_table_info(db_name=db_name, table_name=table["table_name"],
                                                                is_extract=None)}
                                )

        return get_info

    @classmethod
    def modify_column_info(cls, db_name, table_name, input_data):
        operate = MetadataOperate(subject_domain=cls.domain, object_type=models.DBObjectType[cls.db_object_type].value)
        if isinstance(input_data, dict):
            input_data = [input_data]
        return operate.modify_column_subsidiary_info(db_name=db_name,
                                                     table_name=table_name,
                                                     input_data=input_data)

    @classmethod
    def create_extract_table_sql_to_full_index(cls,
                                               table_name,
                                               primary_column_name=None,
                                               extract_column_name=None):
        """
        为了将数据导入全局检索表中，根据元数据管理系统中已经记录的表结构，组织数据抽取sql
        :param table_name: 抽取表表名
        :param primary_column_name: 抽取表主键
        :param extract_column_name: 抽取表中的时间字段，通过该字段进行增量的数据抽取
        :return: dict {select:select column sql, from: from table sql}
        """
        operate = MetadataOperate(subject_domain=cls.domain, object_type=models.DBObjectType[cls.db_object_type].value)

        table_schema = operate.get_table_info(db_name=cls.db_name,
                                              table_name=table_name,
                                              is_extract=None
                                              )
        select_case_str = ""
        concat_str = ""
        for col in table_schema:

            if col["is_primary"] == 1:
                primary_column_name = col["column_name"]
                continue

            if col["is_extract_filter"] == 1:
                extract_column_name = col["column_name"]

            if col["column_type"].lower() in ["varchar", "string", "text", "char"] \
                    and "geo" not in col["column_name"] \
                    and col["is_extract"] == 1:
                select_case_str = select_case_str + """
                ,case when %(column_name)s is null then '*' else concat(%(column_name)s, '*') end as %(column_name)s """ % {
                    "column_name": col["column_name"]}

                concat_str = concat_str + "%s, " % col["column_name"]

        concat_str = concat_str.strip()[:-1]

        if primary_column_name and extract_column_name and len(concat_str) > 1:
            logger.debug("(%s.%s.%s) 表主键和抽取列都明确，生成抽取sql" % (cls.domain, cls.db_name, table_name))

            sql_select = """
            select 
            concat('%(id_tag)s','-',%(table_primary_id)s) as id
            ,concat(%(row_content)s) as row_content
            
            """ % {
                "id_tag": hashlib.md5(
                    str("%s-%s-%s-%s" % (cls.domain, cls.db_object_type, cls.db_name, table_name)).encode(
                        "utf-8")).hexdigest(),
                "table_primary_id": primary_column_name,
                "row_content": concat_str
            }

            sql_from = """
            from (
                select
                %(table_primary_id)s
                %(case_col)s
                from %(table_name)s
            ) t1
            """ % {"table_primary_id": primary_column_name,
                   "case_col": select_case_str,
                   "table_name": table_name}

            extract_data_info = {
                "domain": cls.domain,
                "db_object_type": cls.db_object_type,
                "db_name": cls.db_name,
                "table_name": table_name,
                "table_primary_id": primary_column_name,
                "table_extract_col": extract_column_name,
            }

            extract_data_info_insert = models.ExtractDataInfo.upsert(**extract_data_info)

            return {"select": sql_select,
                    "from": sql_from,
                    "extract_data_info_id": extract_data_info_insert.id}

        else:
            if primary_column_name is None:
                logger.info("(%s.%s.%s) 表主键不明确，无法生成抽取sql" % (cls.domain, cls.db_name, table_name))

            if extract_column_name is None:
                logger.info("(%s.%s.%s) 表抽取列不明确，无法生成抽取sql" % (cls.domain, cls.db_name, table_name))

            return None

    @classmethod
    def extract_data_to_full_index(cls,
                                   table_name,
                                   block_name="",
                                   block_key="",
                                   primary_column_name=None,
                                   extract_column_name=None):
        """
        将数据导入全局检索表中
        :param table_name: 抽取表表名
        :param block_name: 该数据对应的业务域
        :param block_key: 该数据在业务域中的关键词
        :param primary_column_name: 抽取表主键
        :param extract_column_name: 抽取表中的时间字段，通过该字段进行增量的数据抽取
        :return: True or False 代表抽取成功或失败
        """

        execute_sql = cls.create_extract_table_sql_to_full_index(table_name=table_name,
                                                                 primary_column_name=primary_column_name,
                                                                 extract_column_name=extract_column_name)
        if execute_sql:
            meta_detector = MetaDetector(subject_domain=cls.domain,
                                         object_type=models.DBObjectType[cls.db_object_type].value)

            sql_text = """
            %(select)s
            ,%(extract_data_info_id)s as extract_data_info_id
            ,'%(block_name)s' as block_name
            ,'%(block_key)s' as block_key
            %(from)s
            """ % {"select": execute_sql["select"],
                   "extract_data_info_id": execute_sql["extract_data_info_id"],
                   "block_name": block_name,
                   "block_key": block_key,
                   "from": execute_sql["from"]}
            logger.debug("(%s)表抽取sql: %s" % (table_name, sql_text))

            execute_data = meta_detector.execute_select_sql(sql_text=sql_text)

            return models.FullTextIndex.bulk_insert(input_data=execute_data)
