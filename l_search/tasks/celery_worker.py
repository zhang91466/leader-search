# -*- coding: UTF-8 -*-
"""
@time:12/15/2021
@author:
@file:celery_worker
"""
"""
Run using the command:
celery -A l_search.tasks.celery_worker.celery worker --concurrency=2 -E --loglevel=info --logfile=logs/celery.log
"""
from l_search.app import celery_app, create_app

app = create_app()
celery = celery_app.create_celery_app(app)
celery_app.celery = celery