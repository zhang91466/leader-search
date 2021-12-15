# -*- coding: UTF-8 -*-
"""
@time:12/15/2021
@author:
@file:__init__.py
"""
from l_search import settings
from celery.signals import task_prerun
from flask import g
from celery import Celery


class FlaskCelery(Celery):

    def __init__(self, *args, **kwargs):
        super(FlaskCelery, self).__init__(*args, **kwargs)
        # self.patch_task()

        if 'app' in kwargs:
            self.init_app(kwargs["app"])

    def init_app(self, app):
        self.app = app
        self.config_from_object(app.config)
        self.conf.update(app.config)

        class ContextTask(celery.Task):
            def __call__(self, *args, **kwargs):
                with app.app_context():
                    return self.run(*args, **kwargs)

        self.Task = ContextTask


celery = FlaskCelery("l-search-celery",
                     broker=settings.CELERY_BROKER_URL,
                     backend=settings.CELERY_RESULT_BACKEND
                     )

@celery.task()
def full_text_index_extract(domain, db_object_type, db_name, is_full, table_name, block_name, block_key):
    from l_search.handlers.whole_db_search import WholeDbSearch
    from l_search.handlers.meta_operation import Meta
    Meta.set_dwmm_connect()
    WholeDbSearch.domain = domain
    WholeDbSearch.db_object_type = db_object_type
    WholeDbSearch.db_name = db_name
    extract_result = WholeDbSearch.extract_and_store(is_full=is_full,
                                                     table_name=table_name,
                                                     block_name=block_name,
                                                     block_key=block_key
                                                     )
    return extract_result
