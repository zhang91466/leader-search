# -*- coding: UTF-8 -*-
"""
@time:2022/3/11
@author:zhangwei
@file:data_extract_batch
"""
from l_search.models.extract_table_models import TableOperate
from l_search.query_runner import get_query_runner
from l_search import models
from l_search.models.base import db
from l_search import settings

from werkzeug.exceptions import BadRequest
from datetime import datetime


class DataExtractLoad:

    def __init__(self, table_info):
        self.table_info = table_info

    def check_table(self, table_info):

        if table_info.is_entity:
            TableOperate.alter_table(table_info=table_info)
        else:
            TableOperate.create_table(table_info=table_info)

    def put_in_storage(self, increment):
        """
        需强制设置主键id,要不然无法做时态数据存储
        :return:
        """

        column_info_list = models.TableDetail.get_table_detail(table_info=self.table_info,
                                                               is_entity=True)

        target_col_str = ""
        source_col_str = ""
        primary_column_name = None

        now = datetime.now()
        now = now.strftime("%Y-%m-%d, %H:%M:%S")

        for col in column_info_list:

            column_name = str(col.column_name).lower()
            target_append_column_name = column_name
            source_append_column_name = column_name

            if col.column_type == "geometry":
                source_append_column_name = settings.GEO_COLUMN_NAME_STAG

            if col.column_name == settings.PERIOD_COLUMN_NAME:
                source_append_column_name = """tsrange('%s'::timestamp,NULL, '[)')""" % now

            target_col_str += " %s," % target_append_column_name
            source_col_str += " %s," % source_append_column_name

            if col.is_primary:
                primary_column_name = column_name

        if primary_column_name:
            # 通过primary id 更新 tsrange 把当前数据变为历史数据
            TableOperate.update_tsrange(table_info=self.table_info,
                                        primary_col_name=primary_column_name,
                                        upper_datetime=now,
                                        is_commit=False
                                        )
            # 插入新数据
            insert_row_count = TableOperate.insert_table_to_table(
                target_table_name=TableOperate.get_real_table_name(table_name=self.table_info.entity_table_name(),
                                                                   is_stag=False),
                source_table_name=TableOperate.get_real_table_name(table_name=self.table_info.entity_table_name(),
                                                                   is_stag=True),
                target_table_columns_str=target_col_str[:-1],
                source_table_columns_str=source_col_str[:-1],
                is_commit=False)

            if self.table_info.table_extract_col is not None:
                # 需求拿到最新的时间 然后更新到表中
                max_update_ts_in_stag = TableOperate.get_max_update_ts(table_info=self.table_info,
                                                                       update_ts_col=str(
                                                                           self.table_info.table_extract_col).lower())

                self.table_info.latest_extract_date = max_update_ts_in_stag

                db.session.commit()
                return insert_row_count
            elif increment is False:
                db.session.commit()
                return insert_row_count
            elif increment is True and self.table_info.table_extract_col is None:
                raise BadRequest("Increment etl need update timestamp column, otherwise can't increment extract data.")

    def run(self, increment=True):
        """
        判断表是否存在
        存在
            判断表结构是否需要更新
            需求
                更新
        不存在
            创建

        抽取全量数据到stag

        删除原有数据

        插入新增数据

        增量：
        按数据更新时间条件抽取数据

        合并数据，因为有数据更新时间条件，故直接插入，并修改历史相同主键id的行tzrange列的时间区间

        获取更新时间的最大值 并记录
        :return:
        """
        if self.table_info.need_extract is True:

            self.check_table(table_info=self.table_info)

            if increment is False:
                TableOperate.truncate(table_info=self.table_info)

            TableOperate.drop_table(table_info=self.table_info, is_stag=True)
            TableOperate.create_table(table_info=self.table_info, is_stag=True)

            query_runner = get_query_runner(query_runner_type=self.table_info.connection.db_type,
                                            table_info=self.table_info)

            query_runner.set_connection()
            query_runner.extract(increment=increment)
            return self.put_in_storage(increment=increment)

        else:
            raise BadRequest("Table %s doesn't need extract" % self.table_info.table_name)

    def set_schedule(self):
        pass


def extract_tables(is_full=False, table_info_list=None):
    if table_info_list is None:
        table_info_list = models.TableInfo.get_tables()

    execute_result = {}

    for table_info in table_info_list:

        if is_full is True:
            increment = False
        else:
            if table_info.latest_extract_date is not None:
                increment = True
            else:
                increment = False

        etl = DataExtractLoad(table_info=table_info)
        execute_result[table_info.table_name] = etl.run(increment=increment)

    return execute_result
