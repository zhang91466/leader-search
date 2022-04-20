# -*- coding: UTF-8 -*-
"""
@time:2022/4/20
@author:simonzhang
@file:postgresql
"""
from l_search.query_runner import BasicQueryRunner
from l_search.utils.logger import Logger

logger = Logger()


class Postgresql(BasicQueryRunner):

    def extract(self, increment=True):
        logger.info("Postgresql: extracting")
        super().extract(increment=increment)
