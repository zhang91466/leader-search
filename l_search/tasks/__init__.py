# -*- coding: UTF-8 -*-
"""
@time:12/15/2021
@author:
@file:__init__.py
"""
from l_search import celeryapp
from l_search.utils import json_dumps
from l_search import settings
from l_search.tasks.monitor import JobLock

celery = celeryapp.celery


@celery.task(time_limit=settings.CELERY_TASK_FORCE_EXPIRE_SECOND)
def celery_sync_table_meta(connection_id, table_list=None, table_name_prefix=None):
    from l_search.handlers.source_meta_detector import MetaDetector
    from l_search import models

    job_lock_name = "sysn_table_meta_connection_%d" % connection_id
    
    while True:
        if JobLock.set_job_lock(job_name=job_lock_name):
            connection_info = models.DBConnect.get_by_domain(connection_id=connection_id)
            meta_detector = MetaDetector(connection_info=connection_info[0])
            schema_sync_info = meta_detector.detector_schema(tables=table_list,
                                                             table_name_prefix=table_name_prefix)
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
def celery_select_entity_table(execute_sql, connection_id):
    from l_search.models.extract_table_models import TableOperate
    select_return_data = TableOperate.select(sql=execute_sql,
                                             connection_id=connection_id)
    return {"select_data": json_dumps(select_return_data)}
