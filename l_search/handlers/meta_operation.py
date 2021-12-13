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

