# -*- coding: UTF-8 -*-
"""
@time:2022/4/18
@author:simonzhang
@file:test_meta_operation
"""
from tests.handle.test_source_meta_detector import TestMetaDetector
from l_search import models
from l_search.handlers.meta_operation import Meta


class TestMetaData(TestMetaDetector):

    def test_get_table_by_crontab(self):
        table_detail = self.factory.create_table_detail()
        update_data = {"id": table_detail[0]["table_info_id"],
                       "crontab_str": "* * * * *"}
        table_info = models.TableInfo.upsert(input_data=update_data)
        need_crontab_table_list = Meta.get_table_by_crontab()
        self.assertEqual(len(need_crontab_table_list), 1)