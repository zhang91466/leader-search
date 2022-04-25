# -*- coding: UTF-8 -*-
"""
@time:2022/3/17
@author:simonzhang
@file:test_data_extract_batch
"""
from tests.handle.test_source_meta_detector import TestMetaDetector



class TestDataExtractLoad(TestMetaDetector):

    def test_etl_single(self):
        from l_search.handlers.data_extract_batch import DataExtractLoad
        table_info = self.table_init(increment_table=True)
        etl_model = DataExtractLoad(table_info=table_info)
        etl_model.run(increment=False)
        etl_model.run(increment=True)

    def test_etl_all(self):
        from l_search.handlers.data_extract_batch import  extract_tables
        self.meta_detector.detector_schema()
        extract_tables()
        extract_tables(is_full=False)
