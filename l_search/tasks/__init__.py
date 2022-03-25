# -*- coding: UTF-8 -*-
"""
@time:12/15/2021
@author:
@file:__init__.py
"""
from l_search import celeryapp
from l_search.utils import json_dumps

celery = celeryapp.celery


@celery.task()
def celery_sync_table_meta(domain, db_object_type, db_name, db_schema=None, table_list=None, table_name_prefix=None):
    from l_search.handlers.source_meta_detector import MetaDetector
    from l_search import models
    connection_info = models.DBConnect.get_by_domain(domain=domain,
                                                     db_type=db_object_type,
                                                     default_db=db_name,
                                                     db_schema=db_schema)
    meta_detector = MetaDetector(connection_info=connection_info[0])
    schema_sync_info = meta_detector.detector_schema(tables=table_list,
                                                     table_name_prefix=table_name_prefix)
    return schema_sync_info


@celery.task()
def celery_extract_data_from_source(table_info_list, is_full):
    from l_search.handlers.data_extract_batch import extract_tables
    insert_success = extract_tables(table_info_list=table_info_list,
                                    is_full=is_full)
    return {"etl_success_row_count": insert_success}


@celery.task()
def celery_select_entity_table(execute_sql, connection_id):
    from l_search.models.extract_table_models import TableOperate
    select_return_data = TableOperate.select(sql=execute_sql,
                                             connection_id=connection_id)
    return {"select_data": json_dumps(select_return_data)}
