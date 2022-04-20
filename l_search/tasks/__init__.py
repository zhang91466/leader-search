# -*- coding: UTF-8 -*-
"""
@time:12/15/2021
@author:
@file:__init__.py
"""
from l_search import celeryapp
from l_search import settings
from l_search.tasks.monitor import JobLock
from l_search.utils import json_dumps
from l_search.utils.logger import Logger

logger = Logger()

celery = celeryapp.celery


@celery.task(time_limit=settings.CELERY_TASK_FORCE_EXPIRE_SECOND)
def celery_sync_table_meta(connection_id, table_list=None, table_name_prefix=None):
    from l_search.handlers.source_meta_detector import MetaDetector
    from l_search import models

    job_lock_name = "sysn_table_meta_connection_%d" % connection_id
    schema_sync_info = "error"

    while True:
        if JobLock.set_job_lock(job_name=job_lock_name):

            try:
                connection_info = models.DBConnect.get_by_domain(connection_id=connection_id)
                meta_detector = MetaDetector(connection_info=connection_info[0])
                schema_sync_info = meta_detector.detector_schema(tables=table_list,
                                                                 table_name_prefix=table_name_prefix)
            except Exception as e:
                logger.error("celery_sync_table_meta: %s" % e)
            finally:
                JobLock.del_job_lock(job_name=job_lock_name)
                return schema_sync_info

        else:
            JobLock.wait()


@celery.task(time_limit=settings.CELERY_TASK_FORCE_EXPIRE_SECOND)
def celery_extract_data_from_source(connection_info_list, table_id_list, is_full):
    from l_search.handlers.data_extract_batch import extract_tables
    insert_success = extract_tables(connection_info_list=connection_info_list,
                                    table_id_list=table_id_list,
                                    is_full=is_full)
    return {"etl_success_row_count": insert_success}


@celery.task(time_limit=settings.CELERY_TASK_FORCE_EXPIRE_SECOND)
def celery_select_entity_table(execute_sql, connection_id, period_time):
    from l_search.models.extract_table_models import TableOperate
    import hashlib

    job_lock_name = "select_%s" % hashlib.md5(execute_sql.encode("utf-8")).hexdigest()
    select_return_data = "error"

    while True:

        if JobLock.set_job_lock(job_name=job_lock_name,
                                expire_time=10*60):

            try:
                select_return_data = TableOperate.select(sql=execute_sql,
                                                         connection_id=connection_id,
                                                         period_time=period_time)
                JobLock.del_job_lock(job_name=job_lock_name)
                select_return_data = {"select_data": json_dumps(select_return_data)}
            finally:
                JobLock.del_job_lock(job_name=job_lock_name)
                return select_return_data
        else:
            JobLock.wait()


@celery.task(time_limit=settings.CELERY_TASK_FORCE_EXPIRE_SECOND)
def schedule_main():
    from l_search import redis_connection
    from l_search.utils import get_now, datetime, pytz
    from l_search.handlers.meta_operation import Meta
    from l_search import models
    from l_search.tasks.monitor import JobLock

    from croniter import croniter
    from time import mktime

    scheduler_main_time_name = JobLock.set_job_name("scheduler_sync_meta_benchmark_time")
    current_time = get_now()

    def set_sync_meta_to_redis():
        meta_sync_crontab = croniter(settings.SYNC_META_CRONTAB, current_time)
        next_crontab = meta_sync_crontab.get_next(datetime.datetime)
        next_crontab_str = next_crontab.strftime("%Y-%m-%d %H:%M:%S")
        logger.info("Meta sync next time %s" % next_crontab_str)
        redis_connection.set(scheduler_main_time_name, next_crontab_str)
        redis_connection.expire(scheduler_main_time_name, 3 * 24 * 3600)
        return next_crontab_str

    scheduler_main_time_str = redis_connection.get(scheduler_main_time_name)
    if scheduler_main_time_str is None:
        scheduler_main_time_str = set_sync_meta_to_redis()
    else:
        scheduler_main_time_str = scheduler_main_time_str.decode("utf8")

    scheduler_main_time_unix = mktime(datetime.datetime.strptime(scheduler_main_time_str, "%Y-%m-%d %H:%M:%S").timetuple())
    current_time_unix = mktime(current_time.timetuple())
    logger.info("Meta sync latest time %s" % str(scheduler_main_time_unix))
    logger.info("Meta sync current time %s" % str(current_time_unix))
    # sync meta

    if current_time_unix > scheduler_main_time_unix:
        logger.info("Meta sync start")
        connections = models.DBConnect.get_by_domain()
        for c_info in connections:
            celery_sync_table_meta.delay(connection_id=c_info.id)
        set_sync_meta_to_redis()
        logger.info("Meta sync end")

    # extract data
    need_execute_tables = Meta.get_table_by_crontab()
    if len(need_execute_tables) > 0:
        logger.info("Table data extract start: %s" % str(need_execute_tables))
        celery_extract_data_from_source.delay(connection_info_list=None,
                                              table_id_list=need_execute_tables,
                                              is_full=False)
        logger.info("Table data extract end")
