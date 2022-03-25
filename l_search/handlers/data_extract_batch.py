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
from l_search.utils.logger import Logger

from werkzeug.exceptions import BadRequest
from datetime import datetime

logger = Logger()


class DataExtractLoad:

    def __init__(self, table_info):
        self.table_info = table_info
        self.query_runner = get_query_runner(query_runner_type=self.table_info.connection.db_type,
                                             table_info=self.table_info)
        self.query_runner.set_connection()

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
            if increment is True:
                # 通过primary id 更新 tsrange 把当前数据变为历史数据
                TableOperate.update_tsrange(table_info=self.table_info,
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

                if max_update_ts_in_stag is not None:
                    if self.table_info.latest_extract_date is None:
                        self.table_info.latest_extract_date = max_update_ts_in_stag
                    elif max_update_ts_in_stag > self.table_info.latest_extract_date:
                        self.table_info.latest_extract_date = max_update_ts_in_stag

                db.session.commit()
                return insert_row_count
            elif increment is False:
                db.session.commit()
                return insert_row_count
            elif increment is True and self.table_info.table_extract_col is None:
                raise BadRequest("Increment etl need update timestamp column, otherwise can't increment extract data.")
        else:
            raise BadRequest("Table (%s) in %d doesn't have primary col" % (self.table_info.table_name,
                                                                            self.table_info.connection_id))

    def check_row_count(self):
        entity_table_count = TableOperate.row_count(table_info=self.table_info)
        table_in_source_db_count = self.query_runner.row_count()
        logger.info("Check table %s row count source(%d) local(%d)" % (self.table_info.entity_table_name(),
                                                                       table_in_source_db_count,
                                                                       entity_table_count))
        if entity_table_count == table_in_source_db_count:
            return True
        else:
            return False

    def for_delete_data_to_update_period(self):
        """
        通过数据行对比(源头与本地),本地的大于源头数据(现只要两个不对等)
        要发现不对等
            去源头抽取主键id
            然后用主键id和现存的表进行对比
            现存的表不存在该主键id则代表源头已经删除了
        :return:
        """
        if self.check_row_count() is False:
            self.query_runner.extract_primary_id()

            now = datetime.now()
            now = now.strftime("%Y-%m-%d, %H:%M:%S")

            delete_row_count = TableOperate.update_tsrange_for_delete_data(table_info=self.table_info,
                                                                           upper_datetime=now,
                                                                           is_commit=False
                                                                           )
            return delete_row_count

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
        logger.info("Table %s start with increment is %s" % (self.table_info.table_name, str(increment)))
        self.check_table(table_info=self.table_info)

        if increment is False:
            TableOperate.truncate(table_info=self.table_info)

        TableOperate.drop_table(table_info=self.table_info, is_stag=True)
        TableOperate.create_table(table_info=self.table_info, is_stag=True)

        self.query_runner.extract(increment=increment)
        put_row_count = self.put_in_storage(increment=increment)

        if increment is True:
            delete_row_count = self.for_delete_data_to_update_period()
        else:
            delete_row_count = None

        return put_row_count, delete_row_count

    def set_schedule(self):
        pass


def extract_tables(table_info_list=None, is_full=True):
    all_table = []
    if table_info_list is None:
        all_table = models.TableInfo.get_tables()
    else:
        for t_list in table_info_list:
            select_table = models.TableInfo.get_tables(connection_id=t_list["connection_id"],
                                                       table_name=t_list["table_list"])
            all_table.extend(select_table)

    execute_result = {}

    for table_info in all_table:

        if is_full is True:
            increment = False
        else:
            if table_info.latest_extract_date is not None:
                increment = True
            else:
                increment = False

        if str(table_info.table_name).lower() == "p_addpress_bak":
            print("sss")

        etl = DataExtractLoad(table_info=table_info)
        try:
            input_cnt, delete_cnt = etl.run(increment=increment)
        except Exception as e:
            pass

        execute_result[table_info.table_name] = {"input_count": input_cnt,
                                                 "delete_count": delete_cnt}

    return execute_result
