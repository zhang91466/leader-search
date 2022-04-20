# -*- coding: UTF-8 -*-
"""
@time:2022/4/20
@author:simonzhang
@file:mariadb
"""
from l_search.query_runner import BasicQueryRunner
from l_search.utils.logger import Logger

logger = Logger()


class Mariadb(BasicQueryRunner):

    def extract(self, increment=True):
        logger.info("Mariadb: extracting")
        super().extract(increment=increment)