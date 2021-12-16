# -*- coding: UTF-8 -*-
"""
@time:12/15/2021
@author:
@file:__init__.py
"""
from l_search import celeryapp

celery = celeryapp.celery


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
