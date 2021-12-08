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

api_mirror = Namespace('mirror_data', description='Extract data from source')

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
