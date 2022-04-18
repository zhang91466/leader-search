# -*- coding: UTF-8 -*-
"""
@time:2022/3/11
@author:simonzhang
@file:data_extract_batch
"""
from l_search.models.extract_table_models import TableOperate
from l_search.query_runner import get_query_runner
from l_search import models
from l_search.models.base import db
from l_search import settings
from l_search.utils import get_now
from l_search.utils.logger import Logger
from l_search.tasks.monitor import JobLock

from werkzeug.exceptions import BadRequest

logger = Logger()


class DataExtractLoad:

    def __init__(self, table_info):
        self.table_info = table_info
        self.query_runner = get_query_runner(query_runner_type=self.table_info.connection.db_type,
                                             table_info=self.table_info)
        self.query_runner.set_connection()

    def check_table(self, table_info):

        if self.query_runner.check_source_table_exists() is False:
            return False

        if table_info.is_entity:
            TableOperate.alter_table(table_info=table_info)
        else:
            TableOperate.create_table(table_info=table_info)

        return True

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

        now = get_now(is_str=True)

        for col in column_info_list:

            column_name = str(col.column_name).lower()
            target_append_column_name = column_name
            source_append_column_name = column_name

            if col.column_type in settings.GEO_COLUMN_TYPE:
                source_append_column_name = settings.GEO_COLUMN_NAME_STAG

            if col.column_name == settings.PERIOD_COLUMN_NAME:
                source_append_column_name = """tsrange('%s'::timestamp,NULL, '[)')""" % now

            target_col_str += " %s," % target_append_column_name
            source_col_str += " %s," % source_append_column_name

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
        get_primary = models.TableDetail.get_table_detail(table_info_id=self.table_info.id,
                                                          table_primary=True)
        if len(get_primary) > 0 and self.check_row_count() is False:

            TableOperate.drop_table(table_info=self.table_info, is_stag=True)
            TableOperate.create_table(table_info=self.table_info,
                                      just_primary=True,
                                      is_stag=True)

            self.query_runner.extract_primary_id()

            now = get_now(is_str=True)

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
        if self.check_table(table_info=self.table_info):

            if increment is False:
                TableOperate.truncate(table_info=self.table_info)

            TableOperate.drop_table(table_info=self.table_info, is_stag=True)
            TableOperate.create_table(table_info=self.table_info, is_stag=True)

            error_message = self.query_runner.extract(increment=increment)

            if error_message is None:
                put_row_count = self.put_in_storage(increment=increment)

                if increment is True:
                    delete_row_count = self.for_delete_data_to_update_period()
                else:
                    delete_row_count = None

                return put_row_count, delete_row_count, None
            else:
                return 0, 0, error_message
        else:
            return 0, 0, "Table has been moved from source db"

    def set_schedule(self):
        pass


def extract_tables(connection_info_list=None, table_id_list=None, is_full=True):
    if connection_info_list is None and table_id_list is None:
        all_table = models.TableInfo.get_tables(source_table_exists=True)
    else:
        all_table = models.TableInfo.get_tables(connection_id=connection_info_list,
                                                table_id=table_id_list,
                                                source_table_exists=True)
    execute_result = {}

    for table_info in all_table:

        job_lock_name = "data_extract_%s" % table_info.id

        while True:
            if JobLock.set_job_lock(job_name=job_lock_name):
                try:
                    if is_full is True:
                        increment = False
                    else:
                        if table_info.latest_extract_date is not None:
                            increment = True
                        else:
                            increment = False

                    etl = DataExtractLoad(table_info=table_info)

                    input_cnt, delete_cnt, error_message = etl.run(increment=increment)

                    execute_result[table_info.table_name] = {"input_count": input_cnt,
                                                             "delete_count": delete_cnt,
                                                             "error_info": error_message}
                    # update crontab datetime
                    if error_message is None and table_info.crontab_str is not None:
                        models.TableInfo.upsert(input_data={"id": table_info.id,
                                                            "update_crontab_last_ts": True})
                except Exception as e:
                    logger.error("extract_tables: %s" % e)
                finally:
                    JobLock.del_job_lock(job_name=job_lock_name)
                    break
            else:
                JobLock.wait()

    return execute_result
