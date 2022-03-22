# -*- coding: UTF-8 -*-
"""
@time:2022/3/22
@author:zhangwei
@file:test_mirror_data
"""
from tests import BaseTestCase
from l_search.handlers.data_extract_batch import DataExtractLoad

class TestExtractTableSelect(BaseTestCase):

    def test_select(self):
        table_info = self.table_init(increment_table=True)
        etl_model = DataExtractLoad(table_info=table_info)
        etl_model.run(increment=False)

        query_data = {
            "sql": "select l_flowpipe.pipematerial,l_flowpipe.classcode from l_flowpipe "
        }
        rv = self.make_request("post", "/entity/execute_sql", data=query_data)
        self.assertEqual(rv.status_code, 200)

        query_data = {
            "sql": "insert into l_flowpipe select l_flowpipe.pipematerial,l_flowpipe.classcode from l_flowpipe "
        }
        rv = self.make_request("post", "/entity/execute_sql", data=query_data)
        self.assertEqual(rv.status_code, 400)

        query_data = {
            "sql": "select * from l_flowpipe ",
            "connection_id": [2,3,4]
        }
        rv = self.make_request("post", "/entity/execute_sql", data=query_data)
        self.assertEqual(rv.status_code, 400)