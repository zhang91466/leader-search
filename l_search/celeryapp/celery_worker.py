# -*- coding: UTF-8 -*-
"""
@time:12/15/2021
@author:
@file:celery_worker
"""
"""
Run using the command:
celery -A l_search.celeryapp.celery_worker.celery worker --concurrency=2 -E --loglevel=info --logfile=logs/celery.log
celery -A l_search.celeryapp.celery_worker.celery flower --port=5566
"""
from l_search.app import celeryapp, create_app

app = create_app()
celery = celeryapp.create_celery_app(app)
celeryapp.celery = celery

# celery.control.revoke(redis_connection.hget(job_name, "task"), terminate=True)