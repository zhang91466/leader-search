# -*- coding: UTF-8 -*-
"""
@time:2021/11/26
@author:zhangwei
@file:mirror_data
"""
from flask_restx import Namespace, Resource, fields, marshal
from l_search import models
from l_search.models import db, search
from l_search.handlers.meta_operation import Meta

api_mirror = Namespace('mirror_data', description='main function')

mirror_data_schema = {
    "domain": fields.String,
    "db_object_type": fields.String,
    "db_name": fields.String,
    "table_name": fields.String,
    "block_name": fields.String,
    "block_key": fields.String,
    "primary_column_name": fields.String,
    "extract_column_name": fields.String
}

mirror_data_model = api_mirror.model("mirror_data", mirror_data_schema)


class ExtractToFullTextIndexTable(Resource):

    @api_mirror.expect(mirror_data_model)
    def post(self):
        request_data = marshal(api_mirror.payload, mirror_data_model)
        Meta.domain = request_data["domain"]
        Meta.db_object_type = request_data["db_object_type"]
        Meta.db_name = request_data["db_name"]
        extract_result = Meta.extract_data_to_full_index(table_name=request_data["table_name"],
                                                         block_name=request_data["block_name"],
                                                         block_key=request_data["block_key"],
                                                         primary_column_name=request_data["primary_column_name"],
                                                         extract_column_name=request_data["extract_column_name"]
                                                         )
        return {"is-ok": extract_result}, 200


add_connection_info_schema = {
    "domain": fields.String(description="系统域"),
    "db_object_type": fields.String(description="数据库类型", enum=models.DBObjectType._member_names_),
    "host": fields.String(description="数据库地址"),
    "port": fields.String(description="数据库端口"),
    "account": fields.String(description="数据库登入账户"),
    "pwd": fields.String(description="数据库登入账户密码"),
    "default_db": fields.String(description="登入的数据库")
}

add_connection_info_model = api_mirror.model("connection_info_schema", add_connection_info_schema)


class ConnectionInfo(Resource):

    @api_mirror.expect(add_connection_info_model)
    def post(self):
        request_data = marshal(api_mirror.payload, add_connection_info_model)
        add_result = Meta.add_connection_info(**request_data)
        return "新链接信息添加状态 %s" % add_result, 200


sync_meta_schema = {
    "domain": fields.String(description="系统域"),
    "db_object_type": fields.String(description="数据库类型", enum=models.DBObjectType._member_names_),
    "table_list": fields.List(fields.String(description="需要抽取表名称清单", default=None)),
    "table_name_prefix": fields.String(description="需要抽取表名称前缀", default=None)
}

sync_meta_model = api_mirror.model("sycn_meta_schema", sync_meta_schema)


class SyncMeta(Resource):

    @api_mirror.expect(sync_meta_model)
    def post(self):
        request_data = marshal(api_mirror.payload, sync_meta_model)
        Meta.domain = request_data["domain"]
        Meta.db_object_type = request_data["db_object_type"]
        schema_info = Meta.sync_table_schema(table_list=request_data["table_list"],
                                             table_name_prefix=request_data["table_name_prefix"])
        return schema_info, 200
