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
        table_name = TableOperate.create_table(table_info=table_info[0])