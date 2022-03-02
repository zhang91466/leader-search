# -*- coding: UTF-8 -*-
"""
@time:2022/3/1
@author:zhangwei
@file:test_source_meta_detector
"""
from tests import BaseTestCase
from l_search.handlers.source_meta_detector import MetaDetector
from l_search import models


class TestMetaDetector(BaseTestCase):
    def setUp(self):
        super().setUp()
        connection_info = self.factory.create_mssql_connect()
        self.meta_detector = MetaDetector(connection_info=connection_info)

    def test_detector_schema(self):
        sync_result = self.meta_detector.detector_schema()

        for table in sync_result:
            column_count = models.TableDetail.get_table_detail(table_info_id=table["table_info_id"])
            self.assertEqual(table["column_count"], len(column_count))
