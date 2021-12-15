# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:app
"""
from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix
from celery import Celery
from . import settings
from .tasks import celery_app



class LSearch(Flask):
    """A custom Flask app for LSearch"""

    def __init__(self, *args, **kwargs):
        super(LSearch, self).__init__(__name__, *args, **kwargs)
        self.wsgi_app = ProxyFix(self.wsgi_app, x_for=settings.PROXIES_COUNT, x_host=1)
        self.config.from_object("l_search.settings")


def create_app():
    from .models.base import db
    from .interface import api
    from . import migrate



    app = LSearch()


    db.init_app(app)
    migrate.init_app(app, db)
    api.init_app(app)

    celery = celery_app.create_celery_app(app)
    celery_app.celery = celery

    return app



