# -*- coding: UTF-8 -*-
"""
@time:2022/2/25
@author:zhangwei
@file:factories
"""

from l_search.models import db
from l_search import models


class ModelFactory:
    def __init__(self, model, **kwargs):
        self.model = model
        self.kwargs = kwargs

    def _get_kwargs(self, override_kwargs):
        kwargs = self.kwargs.copy()
        kwargs.update(override_kwargs)

        for key, arg in kwargs.items():
            if callable(arg):
                kwargs[key] = arg()

        return kwargs

    def create(self, **override_kwargs):
        kwargs = self._get_kwargs(override_kwargs)
        obj = self.model(**kwargs)
        db.session.add(obj)
        db.session.commit()
        return obj


db_connection_factory = ModelFactory(
    model=models.DBConnect,
    domain="l_search_test",
    db_type="mysql",
    host="192.168.1.107",
    port="7601",
    account="root",
    pwd="leadmap1102",
    default_db="operation"
)


class Factory:

    def create_db_connect(self):
        create_data = db_connection_factory.create()
        result_data = {"domain": create_data.domain,
                       "db_type": create_data.db_type,
                       "host": create_data.host,
                       "port": create_data.port,
                       "account": create_data.account,
                       "default_db": create_data.default_db}
        return result_data
