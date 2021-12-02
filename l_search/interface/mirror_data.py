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

demo_data_schema = {
    "title": fields.String,
    "content": fields.String,
}

demo_data_schema_model = api_mirror.model('data_schema', demo_data_schema)


class ImportData(Resource):

    @api_mirror.doc("add new")
    @api_mirror.expect(demo_data_schema_model)
    @api_mirror.marshal_with(demo_data_schema_model)
    def post(self):
        request_data = marshal(api_mirror.payload, demo_data_schema_model)
        models.FullTest.create(**request_data)
        search.create_index(update=True)
        return {"title": "11", "content": "ddd"}, 200


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

mirror_data_model = api_mirror.model('mirror_data', mirror_data_schema)


class MirrorData(Resource):

    @api_mirror.doc("add all")
    @api_mirror.expect(mirror_data_model)
    def post(self):
        request_data = marshal(api_mirror.payload, mirror_data_model)
        Meta.domain = request_data["domain"]
        Meta.db_object_type = request_data["db_object_type"]
        Meta.db_name = request_data["db_name"]
        extract_result = Meta.extract_data(table_name=request_data["table_name"],
                                           block_name=request_data["block_name"],
                                           block_key=request_data["block_key"],
                                           primary_column_name=request_data["primary_column_name"],
                                           extract_column_name=request_data["extract_column_name"]
                                           )
        return {"is-ok": extract_result}, 200
