# -*- coding: UTF-8 -*-
"""
@time:2022/3/17
@author:simonzhang
@file:test_data_extract_batch
"""
from tests import BaseTestCase
from l_search.models.extract_table_models import DBSession


class TestSingleDataExtractLoad(BaseTestCase):

    def etl_single(self, db_type, drop_table_stmt):
        from l_search.handlers.data_extract_batch import DataExtractLoad
        table_info = self.table_init(increment_table=True,
                                     db_type=db_type,
                                     has_geo=False)

        connection = DBSession(connection_info=table_info.connection)
        try:
            connection.session.execute(drop_table_stmt % str(table_info.table_name))
            connection.session.commit()
        except Exception as e:
            pass

        mock_data_cnt = self.factory.insert_mock_data_to_source_db(engine=connection.engine,
                                                                   table_info=table_info)

        etl_model = DataExtractLoad(table_info=table_info)
        insert_data_cnt, delete_data_cnt, error_message = etl_model.run(increment=False)
        print(error_message)
        self.assertEqual(mock_data_cnt, insert_data_cnt)

    def test_etl_mssql_single(self):
        print("Test mssql etl single")
        drop_table_stmt = "drop table %s"
        self.etl_single(db_type="mssql",
                        drop_table_stmt=drop_table_stmt)

    def test_etl_mysql_single(self):
        print("Test mysql etl single")
        drop_table_stmt = "drop table %s"
        self.etl_single(db_type="mysql",
                        drop_table_stmt=drop_table_stmt)

    def test_etl_pg_single(self):
        print("Test pg etl single")
        drop_table_stmt = "drop table public.%s"
        self.etl_single(db_type="postgresql",
                        drop_table_stmt=drop_table_stmt)
