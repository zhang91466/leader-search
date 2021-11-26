# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:app
"""
from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix

from . import settings


class LSearch(Flask):
    """A custom Flask app for LSearch"""

    def __init__(self, *args, **kwargs):
        super(LSearch, self).__init__(__name__, *args, **kwargs)
        self.wsgi_app = ProxyFix(self.wsgi_app, x_for=settings.PROXIES_COUNT, x_host=1)
        self.config.from_object("l_search.settings")


def create_app():
    from .models.base import db, search
    from .handlers.api import api
    from . import migrate

    app = LSearch()

    search.init_app(app)
    db.init_app(app)
    migrate.init_app(app, db)
    api.init_app(app)

    return app
