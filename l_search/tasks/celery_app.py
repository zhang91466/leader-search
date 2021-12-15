# -*- coding: UTF-8 -*-
"""
@time:12/15/2021
@author:
@file:celery_app
"""
import ssl
import sys

from celery import Celery
CELERY_TASK_LIST = [
    'l_search.tasks',
]

db_session = None
celery = None


def create_celery_app(_app=None):
    """
    Create a new Celery object and tie together the Celery config to the app's config.
    Wrap all tasks in the context of the Flask application.
    :param _app: Flask app
    :return: Celery app
    """
    # New Relic integration
    # if os.environ.get('NEW_RELIC_CELERY_ENABLED') == 'True':
    #     _app.initialize('celery')

    from l_search.models.base import db

    celery = Celery(_app.import_name,
                    broker=_app.config["CELERY_BROKER_URL"],
                    include=CELERY_TASK_LIST)
    celery.conf.update(_app.config)
    always_eager = _app.config['TESTING'] or False
    celery.conf.update({'CELERY_ALWAYS_EAGER': always_eager,
                        'CELERY_RESULT_BACKEND': f"db+{_app.config['SQLALCHEMY_DATABASE_URI']}"})
    # if _app.config['CELERY_REDIS_USE_SSL']:
    #     broker_use_ssl = {'ssl_cert_reqs': ssl.CERT_NONE}
    #     celery.conf.update({'BROKER_USE_SSL': broker_use_ssl})

    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            if not celery.conf.CELERY_ALWAYS_EAGER:
                with _app.app_context():
                    return TaskBase.__call__(self, *args, **kwargs)
            else:
                # special pytest setup
                # db.session = models.db.session = db_session
                db.session = db_session
                return TaskBase.__call__(self, *args, **kwargs)

        def after_return(self, status, retval, task_id, args, kwargs, einfo):
            """
            After each Celery task, teardown our db session.
            FMI: https://gist.github.com/twolfson/a1b329e9353f9b575131
            Flask-SQLAlchemy uses create_scoped_session at startup which avoids any setup on a
            per-request basis. This means Celery can piggyback off of this initialization.
            """
            if _app.config['SQLALCHEMY_COMMIT_ON_TEARDOWN']:
                if not isinstance(retval, Exception):
                    db.session.commit()
            # If we aren't in an eager request (i.e. Flask will perform teardown), then teardown
            if not celery.conf.CELERY_ALWAYS_EAGER:
                db.session.remove()

    celery.Task = ContextTask

    return celery