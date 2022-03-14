# -*- coding: UTF-8 -*-
"""
@time:2022/3/11
@author:zhangwei
@file:data_extract_batch
"""
from l_search.models.extract_table_models import TableOperate
from l_search.query_runner import get_query_runner


class DataExtractLoad:

    def check_table(self, table_info):

        if table_info.is_entity:
            TableOperate.alter_table(table_info=table_info)
        else:
            TableOperate.create_table(table_info=table_info)

    @classmethod
    def merge(cls):
        """

        :return:
        """
        pass

    def increment(self):
        """
        判断表是否存在
        存在
            判断表结构是否需要更新
            需求
                更新
        不存在
            创建

        按数据更新时间条件抽取数据

        合并数据，因为有数据更新时间条件，故直接插入，并修改历史相同主键id的行tzrange列的时间区间
        :return:
        """

    def full(self, table_info):
        """
        表结构的更新 同增量

        抽取全量数据到stag

        删除原有数据

        插入新增数据
        :return:
        """
        self.check_table(table_info=table_info)

        TableOperate.truncate(table_info=table_info)

        query_runner = get_query_runner(query_runner_type=table_info.connection.db_type)

        geo_col = query_runner.extract(table_info=table_info)

        if geo_col:
            target_table_columns_str = table_columns_str.replace("geometry", geo_col))


        TableOperate.insert_table_to_table(
            source_table_name=TableOperate.get_real_table_name(table_name=table_info.table_name, is_stag=True),
            target_table_name=TableOperate.get_real_table_name(table_name=table_info.table_name, is_stag=False),
            source_table_columns_str=table_columns_str + """, tsrange(now()::timestamp,NULL, '[)')""",
            target_table_columns_str=table_columns_str.replace("geometry", "shape") + ",period", )

    def set_schedule(self):
        pass
