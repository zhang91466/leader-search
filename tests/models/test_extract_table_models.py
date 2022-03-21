# -*- coding: UTF-8 -*-
"""
@time:2022/3/3
@author:zhangwei
@file:test_extract_table_models
"""
from tests import BaseTestCase
from l_search.models.extract_table_models import TableOperate
from l_search import models
from l_search.models.base import db
from l_search import settings


class TestDataExtract(BaseTestCase):

    def insert_table_to_table(self):
        table_info = self.table_init(geo_name="geometry")
        table_columns, df_row_count = self.factory.insert_data_to_stag(table_info=table_info)
        table_columns_str = ",".join(table_columns)

        insert_row_count = TableOperate.insert_table_to_table(
            source_table_name=TableOperate.get_real_table_name(table_name=table_info.table_name, is_stag=True),
            target_table_name=TableOperate.get_real_table_name(table_name=table_info.table_name, is_stag=False),
            source_table_columns_str=table_columns_str + """, tsrange(now()::timestamp,NULL, '[)')""",
            target_table_columns_str=table_columns_str + ",period",
            is_commit=False)

        self.assertEqual(df_row_count, insert_row_count)

        TableOperate.drop_table(table_info=table_info, is_stag=True, is_commit=True)
        return table_info

    def test_1_alter_table_add_column(self):
        table_info = self.insert_table_to_table()
        column_info = models.TableDetail.get_table_detail(table_info=table_info)
        new_column = {"table_info_id": table_info.id,
                      "column_name": "update_ts",
                      "column_type": "timestamp",
                      "column_type_length": "",
                      "column_position": len(column_info) + 1,
                      "is_extract": True,
                      "is_primary": False}
        result = models.TableDetail.upsert(new_column)
        TableOperate.alter_table(table_info=table_info)

    def test_2_alter_table_drop_column(self):
        table_info = self.insert_table_to_table()
        drop_column_info = models.TableDetail.get_table_detail(table_info=table_info,
                                                               column_name="INSTALLUNIT")
        self.factory.close_extract_on_column(column_info=drop_column_info[0])
        TableOperate.alter_table(table_info=table_info)


    def test_3_insert(self):
        table_info = self.table_init(geo_name="geometry")
        table_data_df = self.factory.get_pickle_data(table_info=table_info)
        table_data_df = table_data_df.set_crs(crs=settings.GEO_CRS_CODE, allow_override=True)
        table_data_df.to_postgis(
            con=db.engine,
            name=str(table_info.table_name).lower(),
            if_exists="append",
            schema=settings.ODS_SCHEMA_NAME
        )

