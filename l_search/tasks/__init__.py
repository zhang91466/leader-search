# -*- coding: UTF-8 -*-
"""
@time:12/15/2021
@author:
@file:__init__.py
"""
from l_search import celeryapp

celery = celeryapp.celery


@celery.task()
def sync_table_meta(domain, db_object_type, db_name, table_list=None, table_name_prefix=None):
    from l_search.handlers.source_meta_operate.handle.meta_handle import MetaDetector
    meta_detector = MetaDetector(domain=domain,
                                 db_type=db_object_type,
                                 default_db=db_name)
    schema_sync_info = meta_detector.detector_schema(tables=table_list,
                                                     table_name_prefix=table_name_prefix)

    return schema_sync_info


@celery.task()
def full_text_index_extract(domain, db_object_type, db_name, is_full, table_name, block_name, block_key):
    from l_search.handlers.whole_db_search import WholeDbSearch
    WholeDbSearch.domain = domain
    WholeDbSearch.db_object_type = db_object_type
    WholeDbSearch.db_name = db_name
    extract_result = WholeDbSearch.extract_and_store(is_full=is_full,
                                                     table_name=table_name,
                                                     block_name=block_name,
                                                     block_key=block_key
                                                     )
    return {"extract_data": extract_result}


@celery.task()
def table_extract_init(domain, db_object_type, db_name, table_name, need_drop):
    from l_search.handlers.extract_data import ExtractData
    ExtractData.domain = domain
    ExtractData.db_object_type = db_object_type
    ExtractData.db_name = db_name
    init_result = ExtractData.init(table_name=table_name, need_drop=need_drop)
    return init_result


@celery.task()
def table_extract_upsert(domain, db_object_type, db_name, table_name):
    from l_search.handlers.extract_data import ExtractData
    ExtractData.domain = domain
    ExtractData.db_object_type = db_object_type
    ExtractData.db_name = db_name
    upsert_result = ExtractData.upsert(table_name=table_name)
    return upsert_result
