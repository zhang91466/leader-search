# -*- coding: UTF-8 -*-
"""
@time:2021/12/8
@author:zhangwei
@file:data_meta
"""
from flask_restx import Namespace, Resource, fields, marshal
from l_search.handlers.meta_operation import Meta
from l_search import models

api_meta = Namespace('data_meta', description='Source data metabase')

connection_info_schema = {
    "connection_id": fields.Integer(description="数据库连接ID"),
    "domain": fields.String(description="系统域"),
    "db_object_type": fields.String(description="数据库类型", enum=models.DBObjectType._member_names_),
    "host": fields.String(description="数据库地址"),
    "port": fields.String(description="数据库端口"),
    "account": fields.String(description="数据库登入账户"),
    "pwd": fields.String(description="数据库登入账户密码"),
    "default_db": fields.String(description="登入的数据库")
}

connection_info_model = api_meta.model("connection_info_schema", connection_info_schema)


connection_create_schema = connection_info_schema.copy()
connection_create_schema.pop("connection_id")
connection_create_model = api_meta.model("connection_create_schema", connection_create_schema)


class ConnectionInfoCreate(Resource):

    @api_meta.expect(connection_create_model)
    def post(self):
        request_data = marshal(api_meta.payload, connection_create_model)
        request_data.pop("connection_id")
        add_result = Meta.add_connection_info(**request_data)
        return "新链接信息添加状态 %s" % add_result, 200


class ConnectionInfo(Resource):

    @api_meta.marshal_with(connection_info_model)
    def get(self, connection_id=None, domain=None, db_object_type=None):
        connection_info = Meta.get_connection_info(connection_id=connection_id,
                                                   domain=domain,
                                                   db_object_type=db_object_type)
        return marshal(connection_info, connection_info_model), 200


sync_meta_schema = {
    "domain": fields.String(description="系统域"),
    "db_object_type": fields.String(description="数据库类型", enum=models.DBObjectType._member_names_),
    "table_list": fields.List(fields.String(description="需要抽取表名称清单", default=None)),
    "table_name_prefix": fields.String(description="需要抽取表名称前缀", default=None)
}

sync_meta_model = api_meta.model("sycn_meta_schema", sync_meta_schema)


class SyncMeta(Resource):

    @api_meta.expect(sync_meta_model)
    def post(self):
        request_data = marshal(api_meta.payload, sync_meta_model)
        Meta.domain = request_data["domain"]
        Meta.db_object_type = request_data["db_object_type"]
        schema_info = Meta.sync_table_schema(table_list=request_data["table_list"],
                                             table_name_prefix=request_data["table_name_prefix"])
        return schema_info, 200
