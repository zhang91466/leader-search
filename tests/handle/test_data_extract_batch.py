# -*- coding: UTF-8 -*-
"""
@time:2022/3/17
@author:simonzhang
@file:test_data_extract_batch
"""
from tests.handle.test_source_meta_detector import TestMetaDetector
from tests import BaseTestCase


class TestMssqlDataExtractLoad(TestMetaDetector):

    def test_etl_single(self):
        from l_search.handlers.data_extract_batch import DataExtractLoad
        table_info = self.table_init(increment_table=True)
        etl_model = DataExtractLoad(table_info=table_info)
        etl_model.run(increment=False)
        etl_model.run(increment=True)

    def test_etl_all(self):
        from l_search.handlers.data_extract_batch import extract_tables
        self.meta_detector.detector_schema()
        extract_tables()
        extract_tables(is_full=False)


class TestMysqlDataExtractLoad(BaseTestCase):
    def test_etl_mysql_single(self):
        from l_search.handlers.data_extract_batch import DataExtractLoad
        table_info = self.table_init(increment_table=True,
                                     db_type="mysql",
                                     has_geo=False)
        mock_data_cnt = self.factory.insert_mock_data_to_source_db(table_info=table_info)

        etl_model = DataExtractLoad(table_info=table_info)
        insert_data_cnt, delete_data_cnt, error_message = etl_model.run(increment=False)
        print(error_message)
        self.assertEqual(mock_data_cnt, insert_data_cnt)
