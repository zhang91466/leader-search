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

    def table_init(self):
        query = self.factory.create_table_detail()
        table_info = models.TableInfo.get_tables(table_id=query[0]["table_info_id"])
        TableOperate.create_table(table_info=table_info[0],
                                  is_stag=False)
        return table_info[0]

    def insert_table_to_table(self):
        table_info = self.table_init()
        table_columns, df_row_count = self.factory.insert_data_to_stag(table_info=table_info)
        table_columns_str = ",".join(table_columns)

        insert_row_count = TableOperate.insert_table_to_table(
            source_table_name=TableOperate.get_real_table_name(table_name=table_info.table_name, is_stag=True),
            target_table_name=TableOperate.get_real_table_name(table_name=table_info.table_name, is_stag=False),
            source_table_columns_str=table_columns_str + """, tsrange(now()::timestamp,NULL, '[)')""",
            target_table_columns_str=table_columns_str.replace("geometry", "shape") + ",period",
            is_commit=False)

        self.assertEqual(df_row_count, insert_row_count)

        TableOperate.drop_table(table_info=table_info, is_stag=True, is_commit=True)
        return table_info

    def test_2_alter_table_add_column(self):
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
