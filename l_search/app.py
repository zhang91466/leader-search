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
from . import celeryapp



class LSearch(Flask):
    """A custom Flask app for LSearch"""

    def __init__(self, *args, **kwargs):
        super(LSearch, self).__init__(__name__, *args, **kwargs)
        self.wsgi_app = ProxyFix(self.wsgi_app, x_for=settings.PROXIES_COUNT, x_host=1)
        self.config.from_object("l_search.settings")


def create_app():
    from .models.base import db
    from . import migrate
    app = LSearch()

    db.init_app(app)
    migrate.init_app(app, db)

    celery = celeryapp.create_celery_app(app)
    celeryapp.celery = celery

    from .views import api
    api.init_app(app)

    return app



