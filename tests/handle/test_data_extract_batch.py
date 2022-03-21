# -*- coding: UTF-8 -*-
"""
@time:2022/3/17
@author:zhangwei
@file:test_data_extract_batch
"""
from tests import BaseTestCase
from l_search.handlers.data_extract_batch import DataExtractLoad

class TestDataExtractLoad(BaseTestCase):

    def test_etl(self):
        table_info = self.table_init(increment_table=True)
        etl_model = DataExtractLoad(table_info=table_info)
        etl_model.run(increment=False)
        etl_model.run(increment=True)