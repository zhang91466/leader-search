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
from celery.signals import worker_process_init
from l_search.models.base import db

app = create_app()
celery = celeryapp.create_celery_app(app)
celeryapp.celery = celery


# celery.control.revoke(redis_connection.hget(job_name, "task"), terminate=True)


@worker_process_init.connect
def prep_db_pool(**kwargs):
    """
        When Celery fork's the parent process, the db engine & connection pool is included in that.
        But, the db connections should not be shared across processes, so we tell the engine
        to dispose of all existing connections, which will cause new ones to be opend in the child
        processes as needed.
        More info: https://docs.sqlalchemy.org/en/latest/core/pooling.html#using-connection-pools-with-multiprocessing
    """
    # The "with" here is for a flask app using Flask-SQLAlchemy.  If you don't
    # have a flask app, just remove the "with" here and call .dispose()
    # on your SQLAlchemy db engine.
    with app.app_context():
        db.engine.dispose()
