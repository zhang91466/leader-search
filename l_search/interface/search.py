# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:search
"""
from flask_restx import Namespace, Resource, fields, marshal
from l_search import models
from l_search.models import db, search
from flask_msearch.backends import get_tables

api_search = Namespace("search", description="Search index")

query_index_input_schema = {
    "domain": fields.String,
    "db_object_type": fields.String,
    "block_name": fields.String,
    "block_key": fields.String,
    "search_text": fields.String,
}

query_index_input = api_search.model('query_index_request_schema', query_index_input_schema)

# query_index_output_schema = {
#     'title': fields.String,
#     'content': fields.String,
# }
#
# query_index_output = api_search.model('query_index_response_schema', query_index_output_schema)


class QueryIndex(Resource):

    @api_search.doc("Query")
    @api_search.expect(query_index_input)
    # @api_search.marshal_with(query_index_output)
    def post(self):
        request_data = marshal(api_search.payload, query_index_input)
        search_data = models.FullTextIndex.search_index(**request_data)
        return {"data_length": len(search_data)}, 200
