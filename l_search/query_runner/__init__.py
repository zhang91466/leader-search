# -*- coding: UTF-8 -*-
"""
@time:2022/3/11
@author:zhangwei
@file:__init__.py
"""
from l_search.utils.logger import Logger

logger = Logger()


def register(query_runner_class):
    global query_runners
    if query_runner_class.enabled():
        logger.debug(
            "Registering %s (%s) query runner." % (
                query_runner_class.name(),
                query_runner_class.type())
        )
        query_runners[query_runner_class.type()] = query_runner_class
    else:
        logger.debug(
            "%s query runner enabled but not supported, not registering. Either disable or install missing "
            "dependencies." % (query_runner_class.name())
        )


def get_query_runner(query_runner_type, configuration={}):
    query_runner_class = query_runners.get(query_runner_type, None)
    if query_runner_class is None:
        return None

    return query_runner_class(configuration)


class BasicQueryRunner:

    def __init__(self, configuration):
        self.configuration = configuration

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def type(cls):
        return cls.__name__.lower()

    @classmethod
    def enabled(cls):
        return True
