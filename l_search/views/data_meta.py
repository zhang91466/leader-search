# -*- coding: UTF-8 -*-
"""
@time:2021/12/8
@author:zhangwei
@file:data_meta
"""
from flask_restx import Namespace, Resource, fields, marshal
from l_search.handlers.meta_operation import Meta
from l_search.handlers.source_meta_operate.handle.meta_handle import MetaDetector
from l_search import models

api_meta = Namespace('data_meta', description='Source data metabase')

connection_info_schema = {
    "connection_id": fields.Integer(description="数据库连接ID,修改连接信息时使用", required=False),
    "domain": fields.String(description="系统域"),
    "db_object_type": fields.String(description="数据库类型", enum=models.DBObjectType._member_names_),
    "host": fields.String(description="数据库地址"),
    "port": fields.String(description="数据库端口"),
    "account": fields.String(description="数据库登入账户"),
    "pwd": fields.String(description="数据库登入账户密码"),
    "default_db": fields.String(description="登入的数据库")
}

connection_info_model = api_meta.model("connection_info_schema", connection_info_schema)


class ConnectionInfoCreate(Resource):

    @api_meta.expect(connection_info_model)
    def post(self):
        request_data = marshal(api_meta.payload, connection_info_model)
        add_result = Meta.add_connection_info(request_data)
        return add_result, 200


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

        meta_detector = MetaDetector(domain=request_data["domain"],
                                     type=models.DBObjectType[request_data["db_object_type"]].value)
        schema_info = meta_detector.detector_schema(tables=request_data["table_list"],
                                                    table_name_prefix=request_data["table_name_prefix"])
        return schema_info, 200


meta_info_schema = {
    "id": fields.Integer(description="列id"),
    "table_name": fields.String(description="表名"),
    "column_name": fields.String(description="列名"),
    "column_type": fields.String(description="列格式"),
    "column_type_length": fields.String(description="字段长度"),
    "column_comment": fields.String(description="列备注"),
    "is_primary": fields.Integer(description="该列是否是主键", enum=[0, 1]),
    "is_extract": fields.Integer(description="该列是否要抽取", enum=[0, 1]),
    "is_extract_filter": fields.Integer(description="该列是否是增量抽取的标志，必须是时间列", enum=[0, 1]),
    "filter_default": fields.String(description="时间列格式化方式"),
}

meta_info_model = api_meta.model("meta_info_schema", meta_info_schema)

meta_info_list_model = api_meta.model("meta_info_list_model", {
    "table_name": fields.String(description="表名"),
    "data": fields.List(fields.Nested(meta_info_model))})

meta_info_modify_schema = {"id": meta_info_model["id"],
                           "column_comment": meta_info_model["column_comment"],
                           "is_primary": meta_info_model["is_primary"],
                           "is_extract": meta_info_model["is_extract"],
                           "is_extract_filter": meta_info_model["is_extract_filter"],
                           "filter_default": meta_info_model["filter_default"]}

meta_info_modify_model = api_meta.model("meta_info_modify_schema", meta_info_modify_schema)


class MetaInfo(Resource):

    @api_meta.marshal_with(meta_info_list_model)
    def get(self, domain, db_object_type, db_name, table_name=None):
        Meta.domain = domain
        Meta.db_object_type = db_object_type
        get_result = Meta.get_table_meta(default_db=db_name, table_name=table_name)
        return_data = marshal(get_result, meta_info_list_model)
        return return_data, 200

    @api_meta.expect(meta_info_modify_model)
    def post(self, domain, db_object_type, db_name, table_name):
        request_data = marshal(api_meta.payload, meta_info_modify_model)
        Meta.domain = domain
        Meta.db_object_type = db_object_type
        modify_status = Meta.modify_column_info(default_db=db_name, table_name=table_name, input_data=request_data)
        return modify_status, 200
