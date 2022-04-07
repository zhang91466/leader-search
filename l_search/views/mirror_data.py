# -*- coding: UTF-8 -*-
"""
@time:2021/11/26
@author:zhangwei
@file:mirror_data
"""
from flask_restx import Namespace, Resource, fields, marshal
from l_search.tasks import (celery_extract_data_from_source,
                            celery_select_entity_table)

api_mirror = Namespace("extract_and_select", description="Extract data from source")

extract_para_schema = {
    "connections": fields.List(fields.Integer(description="连接id")),
    "tables": fields.List(fields.String(description="表id")),
    "extract_type": fields.String(description="抽取方式", enum=["i", "f"])
}

extract_para_model = api_mirror.model("extract_para_schema", extract_para_schema)


class ExtractAndLoad(Resource):

    @api_mirror.expect(extract_para_model)
    def post(self):
        """
        异步 -- 对实体表进行数据更新
        :param connection_id:
        :param table_info_id:
        :return:
        """
        request_data = marshal(api_mirror.payload, extract_para_model)
        task = celery_extract_data_from_source.delay(connection_info_list=request_data["connections"],
                                                                 table_info_list=request_data["tables"],
                                                                 is_full=request_data["extract_type"])
        return {"task_id": task.id}, 200

        # insert_success = celery_extract_data_from_source(connection_info_list=request_data["connections"],
        #                                                  table_id_list=request_data["tables"],
        #                                                  is_full=request_data["extract_type"])
        # return {"etl_success_row_count": insert_success}, 200


extract_data_select_schema = {
    "sql": fields.String(description="需要执行的sql"),
    "connection_id": fields.List(fields.Integer(description="sql中表对应的连接id组合,同名不在连接id内的表会被排除"), default=[])
}

extract_data_select_model = api_mirror.model("extract_data_select_schema", extract_data_select_schema)


class ExtractDataSelect(Resource):

    @api_mirror.expect(extract_data_select_model)
    def post(self):
        """
        异步 -- 对实体表进行sql查询
        :return:
        """

        request_data = marshal(api_mirror.payload, extract_data_select_model)
        task = celery_select_entity_table.delay(execute_sql=request_data["sql"],
                                                connection_id=request_data["connection_id"])
        # select_return_data = celery_select_entity_table(execute_sql=request_data["sql"],
        #                                          connection_id=request_data["connection_id"])
        # return select_return_data, 200
        return {"task_id": task.id}, 200
