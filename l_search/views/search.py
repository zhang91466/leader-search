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
    # "domain": fields.String(description="系统域"),
    # "db_object_type": fields.String(description="数据库类型", enum=models.DBObjectType._member_names_),
    "block_name": fields.String(description="业务标签分组名称"),
    "block_key": fields.String(description="业务标签"),
    "search_text": fields.String(description="""检索方法:  + stands for AND - stands for NOT
                       detail:https://dev.mysql.com/doc/refman/8.0/en/fulltext-boolean.html"""),
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
    def post(self, domain, db_object_type):
        """
        全文检索
        :param domain:
        :param db_object_type: mysql,postgres
        :return:
        """
        WholeDbSearch.domain = domain
        WholeDbSearch.db_object_type = db_object_type
        request_data = marshal(api_search.payload, query_index_input)
        search_data = WholeDbSearch.search(**request_data)
        return marshal(search_data, query_index_output), 200
