# -*- coding: UTF-8 -*-
"""
@time:2022/3/3
@author:zhangwei
@file:test_extract_table_models
"""
from tests import BaseTestCase
from l_search.models.extract_table_models import TableOperate
from l_search import models


class TestDataExtract(BaseTestCase):

    def test_table_init(self):
        query = self.factory.create_table_detail()
        table_info = models.TableInfo.get_tables(table_id=query[0]["table_info_id"])
        TableOperate.create_table(table_info=table_info[0],
                                  is_stag=False)
        return table_info[0]

    def test_insert_table_to_table(self):
        table_info = self.test_table_init()
        table_columns = self.factory.insert_data_to_stag(table_info=table_info)
        table_columns_str = ",".join(table_columns)

        TableOperate.insert_table_to_table(
            source_table_name=TableOperate.get_real_table_name(table_name=table_info.table_name, is_stag=True),
            target_table_name=TableOperate.get_real_table_name(table_name=table_info.table_name, is_stag=False),
            table_columns_str=table_columns_str)
