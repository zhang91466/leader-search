# -*- coding: UTF-8 -*-
"""
@time:2021/11/26
@author:zhangwei
@file:mirror_data
"""
from flask_restx import Namespace, Resource, fields, marshal
from l_search.tasks import full_text_index_extract
from l_search.handlers.extract_data import ExtractData

api_mirror = Namespace('mirror_data', description='Extract data from source')

mirror_data_schema = {
    "block_name": fields.String(description="业务标签分组名称"),
    "block_key": fields.String(description="业务标签"),
    "is_full": fields.Integer(description="是否全量抽取", enum=[0, 1])
}

mirror_data_model = api_mirror.model("mirror_data", mirror_data_schema)


class ExtractToFullTextIndexTable(Resource):

    @api_mirror.expect(mirror_data_model)
    def post(self, domain, db_object_type, db_name, table_name):
        """
        异步 -- 抽取目标数据至全文检索数据库

        :param domain: 主题域
        :param db_object_type: mysql, postgres
        :param db_name: 登入的数据库名称
        :param table_name: 抽取的表名
        :return:
        """

        request_data = marshal(api_mirror.payload, mirror_data_model)
        task = full_text_index_extract.delay(domain=domain,
                                             db_object_type=db_object_type,
                                             db_name=db_name,
                                             is_full=request_data["is_full"],
                                             table_name=table_name,
                                             block_name=request_data["block_name"],
                                             block_key=request_data["block_key"]
                                             )
        return {"task_id": task.id}, 202


entity_data_schema = {
    "is_entity": fields.Boolean(description="是否是克隆", default=False)
}

entity_data_model = api_mirror.model("entity_data_schema", entity_data_schema)

class ExtractToEntityInit(Resource):

    @api_mirror.expect(entity_data_model)
    def post(self, domain, db_object_type, db_name, table_name):
        request_data = marshal(api_mirror.payload, entity_data_model)
        ExtractData.domain = domain
        ExtractData.db_object_type = db_object_type
        ExtractData.db_name = db_name
        ExtractData.init(table_name=table_name, need_drop=request_data["is_entity"])