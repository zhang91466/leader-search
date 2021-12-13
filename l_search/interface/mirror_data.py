# -*- coding: UTF-8 -*-
"""
@time:2021/11/26
@author:zhangwei
@file:mirror_data
"""
from flask_restx import Namespace, Resource, fields, marshal
from l_search import models
from l_search.models import db
from l_search.handlers.whole_db_search import WholeDbSearch

api_mirror = Namespace('mirror_data', description='Extract data from source')

mirror_data_schema = {
    "domain": fields.String(description="主题域"),
    "db_object_type": fields.String(description="数据库类型", enum=models.DBObjectType._member_names_),
    "db_name": fields.String(description="登入的数据库名称"),
    "table_name": fields.String(description="抽取的表名"),
    "block_name": fields.String(description="业务标签分组名称"),
    "block_key": fields.String(description="业务标签"),
    "is_full": fields.Integer(description="是否全量抽取", enum=[0, 1])
    # "primary_column_name": fields.String,
    # "extract_column_name": fields.String
}

mirror_data_model = api_mirror.model("mirror_data", mirror_data_schema)


class ExtractToFullTextIndexTable(Resource):

    @api_mirror.expect(mirror_data_model)
    def post(self):
        request_data = marshal(api_mirror.payload, mirror_data_model)
        WholeDbSearch.domain = request_data["domain"]
        WholeDbSearch.db_object_type = request_data["db_object_type"]
        WholeDbSearch.db_name = request_data["db_name"]
        extract_result = WholeDbSearch.extract_data_to_full_index(is_full=request_data["is_full"],
                                                                  table_name=request_data["table_name"],
                                                                  block_name=request_data["block_name"],
                                                                  block_key=request_data["block_key"],
                                                                  # primary_column_name=request_data["primary_column_name"],
                                                                  # extract_column_name=request_data["extract_column_name"]
                                                                  )
        return {"insert": extract_result}, 200
