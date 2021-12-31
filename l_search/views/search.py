# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:search
"""
from flask_restx import Namespace, Resource, fields, marshal
from l_search.handlers.whole_db_search import WholeDbSearch

api_search = Namespace("search", description="Search data with full text index")

query_index_input_schema = {
    "block_name": fields.String(description="业务标签分组名称"),
    "block_key": fields.String(description="业务标签"),
    "search_text": fields.String(description="搜索内容"),
    "page": fields.Integer(description="分页"),
    "page_size": fields.Integer(description="展示数", default=20)
}

query_index_input = api_search.model('query_index_request_schema', query_index_input_schema)

query_index_output_schema = {
    "db_name": fields.String(description="登入的数据库名称"),
    "table_name": fields.String(description="抽取的表名"),
    "row_id": fields.String(description="抽取表的主键id"),
    "block_name": fields.String(description="业务标签分组名称"),
    "block_key": fields.String(description="业务标签"),
    "hits": fields.List(fields.String(description="命中的单词"))
}

query_index_output = api_search.model('query_index_response_schema', query_index_output_schema)


class QueryIndex(Resource):

    @api_search.expect(query_index_input)
    @api_search.marshal_with(query_index_output)
    def post(self, domain, db_object_type=None, db_name=None):
        """
        全文检索
        :param domain:
        :param db_object_type: mysql,postgres
        :return:
        """
        WholeDbSearch.domain = domain
        if db_object_type:
            WholeDbSearch.db_object_type = db_object_type
        if db_name:
            WholeDbSearch.db_name = db_name
        request_data = marshal(api_search.payload, query_index_input)
        search_data = WholeDbSearch.search(**request_data)
        return marshal(search_data, query_index_output), 200


group_index_input_schema = {
    "search_text": fields.String(description="搜索内容"),
    "page": fields.Integer(description="分页"),
    "page_size": fields.Integer(description="展示数", default=20)
}

group_index_input = api_search.model('group_index_input_schema', group_index_input_schema)

group_index_output_schema = {
    "block_name": fields.String(description="业务标签分组名称"),
    "block_key": fields.String(description="业务标签"),
    "hits_num": fields.Integer(description="命中的单词的总数")
}

group_index_output = api_search.model('group_index_output_schema', group_index_output_schema)


class GroupIndex(Resource):

    @api_search.expect(group_index_input)
    @api_search.marshal_with(group_index_output)
    def post(self, domain, db_object_type=None, db_name=None):
        """
        全文检索总览
        :param domain:
        :param db_object_type: mysql,postgres
        :return:
        """
        WholeDbSearch.domain = domain
        if db_object_type:
            WholeDbSearch.db_object_type = db_object_type
        if db_name:
            WholeDbSearch.db_name = db_name
        request_data = marshal(api_search.payload, group_index_input)
        search_data = WholeDbSearch.search_group(**request_data)
        return marshal(search_data, group_index_output), 200
