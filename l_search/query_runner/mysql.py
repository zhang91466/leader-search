# -*- coding: UTF-8 -*-
"""
@time:2022/4/20
@author:simonzhang
@file:mysql
"""
from l_search.query_runner import BasicQueryRunner
from l_search.utils.logger import Logger
from l_search.query_runner import register

logger = Logger()


class Mysql(BasicQueryRunner):

    def extract(self, increment=True):
        logger.info("Mysql: extracting")
        super().extract(increment=increment)


register(Mysql)
