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
    "search_text": fields.String(description="""
    搜索语法规则：
     +   一定要有(不含有该关键词的数据条均被忽略)。 
     -   不可以有(排除指定关键词，含有该关键词的均被忽略)。 
     >   提高该条匹配数据的权重值。 
     <   降低该条匹配数据的权重值。
     ~   将其相关性由正转负，表示拥有该字会降低相关性(但不像-将之排除)，只是排在较后面权重值降低。 
     *   万用字，不像其他语法放在前面，这个要接在字符串后面。 
     " " 用双引号将一段句子包起来表示要完全相符，不可拆字。
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
