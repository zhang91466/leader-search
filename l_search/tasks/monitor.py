# -*- coding: UTF-8 -*-
"""
@time:2022/4/8
@author:simonzhang
@file:monitor
"""
from l_search import redis_connection
from l_search import settings
from l_search.utils import get_now
from l_search.utils.logger import Logger

import time

logger = Logger()

class JobLock:
    LOCK_NAME = "Lock_&_%s"

    @classmethod
    def set_job_name(cls, job_name):
        return cls.LOCK_NAME % job_name

    @classmethod
    def set_job_lock(cls, job_name):
        job_name = cls.set_job_name(job_name=job_name)
        last_time = redis_connection.get(job_name)

        if last_time is None:
            redis_connection.set(job_name, get_now(is_str=True))
            redis_connection.expire(job_name, settings.CELERY_TASK_FORCE_EXPIRE_SECOND)
            return True
        else:
            return False

    @classmethod
    def del_job_lock(cls, job_name):
        redis_connection.delete(cls.set_job_name(job_name=job_name))

    @classmethod
    def wait(cls):
        time.sleep(1)

    @classmethod
    def del_all_job(cls):
        keys = redis_connection.keys("%s*" % cls.LOCK_NAME % "")
        cnt = len(keys)
        if cnt > 0:
            redis_connection.delete(*keys)
            logger.info("Delete all job count: %d" % cnt)
