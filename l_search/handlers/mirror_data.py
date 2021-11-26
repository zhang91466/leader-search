# -*- coding: UTF-8 -*-
"""
@time:2021/11/26
@author:zhangwei
@file:mirror_data
"""
from flask_restx import Namespace, Resource, fields, marshal
from l_search import models
from l_search.models import db, search
from flask_msearch.backends import get_tables

api_mirror = Namespace('mirror_data', description='main function')


data_schema = {
    "title": fields.String,
    "content": fields.String,
}

data_schema_model = api_mirror.model('data_schema', data_schema)


class ImportData(Resource):

    @api_mirror.doc("add new")
    @api_mirror.expect(data_schema_model)
    @api_mirror.marshal_with(data_schema_model)
    def post(self):
        request_data = marshal(api_mirror.payload, data_schema_model)
        models.FullTest.create(**request_data)
        search.create_index(update=True)
        return {"title": "11", "content": "ddd"}, 200
