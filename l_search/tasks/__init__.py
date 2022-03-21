# -*- coding: UTF-8 -*-
"""
@time:12/15/2021
@author:
@file:__init__.py
"""
from l_search import celeryapp

celery = celeryapp.celery


@celery.task()
def sync_table_meta(domain, db_object_type, db_name, db_schema=None, table_list=None, table_name_prefix=None):
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

