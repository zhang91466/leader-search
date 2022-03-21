# -*- coding: UTF-8 -*-
"""
@time:2021/11/26
@author:zhangwei
@file:mirror_data
"""
from flask_restx import Namespace, Resource, fields, marshal
from l_search.handlers.data_extract_batch import DataExtractLoad
from l_search import models

api_mirror = Namespace("extract_and_select", description="Extract data from source")

extract_para_schema = {
    "extract_type": fields.String(description="抽取方式", enum=["i", "f"])
}

extract_para_model = api_mirror.model("extract_para_schema", extract_para_schema)


class ExtractAndLoad(Resource):

    @api_mirror.expect(extract_para_model)
    def post(self, connection_id, table_info_id):
        """
        异步 -- 对实体表进行数据更新
        :param connection_id:
        :param table_info_id:
        :return:
        """
        table_info = models.TableInfo.get_tables(connection_id=connection_id,
                                                 table_id=table_info_id)
        request_data = marshal(api_mirror.payload, extract_para_model)
        etl = DataExtractLoad(table_info=table_info[0])

        increment = False
        if request_data["extract_type"] == "i":
            increment = True
        insert_success = etl.run(increment=increment)
        return {"etl_success_row_count": insert_success}, 200