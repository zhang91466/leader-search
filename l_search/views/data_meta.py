# -*- coding: UTF-8 -*-
"""
@time:2021/12/8
@author:zhangwei
@file:data_meta
"""
from flask_restx import Namespace, Resource, fields, marshal
from l_search.handlers.meta_operation import Meta
from l_search.tasks import celery_sync_table_meta
from l_search import models

api_meta = Namespace("data_meta", description="Source data metabase")

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


# fields.List(fields.Nested(meta_info_model))})
table_info_schema = {
    "id": fields.String(description="表ID", ),
    "connection_id": fields.Integer(description="数据库连接ID"),
    "table_name": fields.String(description="表名"),
    "table_extract_col": fields.String(description="表用于抽取的列名"),
    "need_extract": fields.Boolean(description="表是否需要抽取"),
    "latest_extract_date": fields.String(description="记录最新的抽取时间")
}
table_info_model = api_meta.model("table_info_schema", table_info_schema)


class TableInfo(Resource):

    @api_meta.marshal_with(table_info_model)
    def get(self, connection_id, table_name=None):
        """
        已收集的目标库表信息获取
        :param connection_id:
        :param table_name:
        :return:
        """
        get_result = Meta.get_table_info(connection_id=connection_id, table_name=table_name)
        return_data = marshal(get_result, table_info_model)
        return return_data, 200

    @api_meta.expect(table_info_model)
    def post(self):
        """
        已收集的目标库表信息修改
        :return:
        """
        request_data = marshal(api_meta.payload, table_info_model)
        upsert_data = Meta.upsert_table_info(input_data=request_data)
        return_data = marshal(upsert_data, table_info_model)
        return return_data, 200


table_detail_schema = {
    "id": fields.String(description="列ID", ),
    "table_info_id": fields.String(description="表ID"),
    "column_name": fields.String(description="列名"),
    "column_type": fields.String(description="列类型"),
    "column_type_length": fields.String(description="列长度"),
    "column_comment": fields.String(description="列备注"),
    "column_position": fields.Integer(description="列在表中的顺序"),
    "is_extract": fields.Boolean(description="列是否抽取", default=True),
    "is_primary": fields.Boolean(description="列是否是主键", default=False)
}
table_detail_model = api_meta.model("table_detail_schema", table_detail_schema)


class TableDetail(Resource):

    @api_meta.marshal_with(table_detail_model)
    def get(self, table_info_id):
        """
        已收集的目标库表结构信息获取
        :param table_info_id:
        :return:
        """
        get_result = Meta.get_table_detail(table_info_id=table_info_id)
        return_data = marshal(get_result, table_detail_model)
        return return_data, 200

    @api_meta.expect(table_detail_model)
    def post(self):
        """
        已收集的目标库表结构信息修改
        :return:
        """
        request_data = marshal(api_meta.payload, table_detail_model)
        upsert_data = Meta.upsert_table_detail(input_data=request_data)
        return_data = marshal(upsert_data, table_detail_model)
        return return_data, 200


sync_meta_schema = {
    "domain": fields.String(description="系统域"),
    "db_object_type": fields.String(description="数据库类型", enum=models.DBObjectType._member_names_),
    "db_name": fields.String(description="数据库名称"),
    "db_schema": fields.String(description="数据库名称 针对pg"),
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

        task = celery_sync_table_meta.delay(domain=request_data["domain"],
                                            db_object_type=models.DBObjectType[request_data["db_object_type"]].value,
                                            db_name=request_data["db_name"],
                                            db_schema=request_data["db_schema"],
                                            table_list=request_data["table_list"],
                                            table_name_prefix=request_data["table_name_prefix"]
                                            )
        return {"task_id": task.id}, 200

        # task = sync_table_meta(domain=request_data["domain"],
        #                        db_object_type=models.DBObjectType[request_data["db_object_type"]].value,
        #                        db_name=request_data["db_name"],
        #                        db_schema=request_data["db_schema"],
        #                        table_list=request_data["table_list"],
        #                        table_name_prefix=request_data["table_name_prefix"]
        #                        )
        # return task, 200
