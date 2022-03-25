# -*- coding: UTF-8 -*-
"""
@time:2022/3/17
@author:zhangwei
@file:test_data_extract_batch
"""
from tests.handle.test_source_meta_detector import TestMetaDetector
from l_search.handlers.data_extract_batch import DataExtractLoad, extract_tables


class TestDataExtractLoad(TestMetaDetector):

    def test_etl_single(self):
        table_info = self.table_init(increment_table=True)
        etl_model = DataExtractLoad(table_info=table_info)
        etl_model.run(increment=False)
        etl_model.run(increment=True)

    def test_etl_all(self):
        self.meta_detector.detector_schema()
        extract_tables()
        extract_tables(is_full=False)
