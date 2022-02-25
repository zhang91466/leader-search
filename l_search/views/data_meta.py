# -*- coding: UTF-8 -*-
"""
@time:2021/12/8
@author:zhangwei
@file:data_meta
"""
from flask_restx import Namespace, Resource, fields, marshal
from l_search.handlers.meta_operation import Meta
from l_search.tasks import sync_table_meta
from l_search import models

api_meta = Namespace('data_meta', description='Source data metabase')

connection_info_schema = {
    "id": fields.Integer(description="数据库连接ID,修改连接信息时使用", required=False),
    "domain": fields.String(description="系统域"),
    "db_type": fields.String(description="数据库类型", enum=models.DBObjectType._member_names_),
    "host": fields.String(description="数据库地址"),
    "port": fields.Integer(description="数据库端口"),
    "account": fields.String(description="数据库登入账户"),
    "default_db": fields.String(description="登入的数据库"),
    "db_schema": fields.String(description="登入的数据库(适用于pg)")
}

connection_info_model = api_meta.model("connection_info_schema", connection_info_schema)

connection_info_insert_schema = connection_info_schema.copy()
connection_info_insert_schema["pwd"] = fields.String(description="数据库登入密码")
connection_info_insert_schema.pop("id")
connection_info_insert_model = api_meta.model("connection_info_insert_schema", connection_info_insert_schema)


class ConnectionInfoUpsert(Resource):

    @api_meta.expect(connection_info_insert_model)
    def post(self):
        """
        目标库链接信息创建
        :return:
        """
        request_data = marshal(api_meta.payload, connection_info_insert_model)
        upsert_data = Meta.upsert_connection_info(request_data)
        return marshal(upsert_data, connection_info_model), 200


class ConnectionInfo(Resource):

    @api_meta.marshal_with(connection_info_model)
    def get(self, connection_id=None, domain=None, db_type=None, db_name=None):
        """
        目标库链接信息获取
        :param connection_id:
        :param domain:
        :param db_type:
        :return:
        """
        connection_info = Meta.get_connection_info(connection_id=connection_id,
                                                   domain=domain,
                                                   db_type=db_type,
                                                   default_db=db_name)
        return marshal(connection_info, connection_info_model), 200


sync_meta_schema = {
    "domain": fields.String(description="系统域"),
    "db_object_type": fields.String(description="数据库类型", enum=models.DBObjectType._member_names_),
    "db_name": fields.String(description="数据库名称"),
    "table_list": fields.List(fields.String(description="需要抽取表名称清单", default=None)),
    "table_name_prefix": fields.String(description="需要抽取表名称前缀", default=None)
}

sync_meta_model = api_meta.model("sycn_meta_schema", sync_meta_schema)


class SyncMeta(Resource):

    @api_meta.expect(sync_meta_model)
    def post(self):
        """
        异步 -- 同步目标库表结构信息
        :return:
        """
        request_data = marshal(api_meta.payload, sync_meta_model)

        task = sync_table_meta.delay(domain=request_data["domain"],
                                     db_object_type=models.DBObjectType[request_data["db_object_type"]].value,
                                     db_name=request_data["db_name"],
                                     table_list=request_data["table_list"],
                                     table_name_prefix=request_data["table_name_prefix"]
                                     )
        return {"task_id": task.id}, 200


meta_info_schema = {
    "id": fields.Integer(description="列id"),
    "column_name": fields.String(description="列名"),
    "column_type": fields.String(description="列格式"),
    "column_type_length": fields.String(description="字段长度"),
    "column_comment": fields.String(description="列备注"),
    "is_primary": fields.Boolean(description="该列是否是主键"),
    "is_extract": fields.Boolean(description="该列是否要抽取")
}

meta_info_model = api_meta.model("meta_info_schema", meta_info_schema)

meta_info_list_model = api_meta.model("meta_info_list_model", {
    "table_name": fields.String(description="表名"),
    "data": fields.List(fields.Nested(meta_info_model))})

meta_info_modify_schema = {"id": meta_info_model["id"],
                           "column_comment": meta_info_model["column_comment"],
                           "is_primary": meta_info_model["is_primary"],
                           "is_extract": meta_info_model["is_extract"]}

meta_info_modify_model = api_meta.model("meta_info_modify_schema", meta_info_modify_schema)


class MetaInfo(Resource):

    @api_meta.marshal_with(meta_info_list_model)
    def get(self, connection_id, table_name=None):
        """
        已收集的目标库表结构信息获取
        :param connection_id:
        :param table_name:
        :return:
        """
        get_result = Meta.get_table_meta(connection_id=connection_id, table_name=table_name)
        return_data = marshal(get_result, meta_info_list_model)
        return return_data, 200

    @api_meta.expect(meta_info_modify_model)
    def post(self, connection_id, table_name):
        """
        已收集的目标库表结构信息修改
        :param connection_id:
        :param table_name:
        :return:
        """
        request_data = marshal(api_meta.payload, meta_info_modify_model)
        modify_status = Meta.modify_column_info(connection_id=connection_id,
                                                table_name=table_name,
                                                input_data=request_data)
        return modify_status, 200
