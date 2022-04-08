# -*- coding: UTF-8 -*-
"""
@time:2022/4/8
@author:simonzhang
@file:monitor
"""
from l_search import redis_connection
from l_search import settings

from datetime import datetime
import time


class JobLock:

    @classmethod
    def set_job_lock(cls, job_name):
        now = datetime.now()
        last_time = redis_connection.get(job_name)

        if last_time is None:
            redis_connection.set(job_name, now.strftime("%Y-%m-%d %H:%M:%S"))
            redis_connection.expire(job_name, settings.CELERY_TASK_FORCE_EXPIRE_SECOND)
            return True
        else:
            return False

    @classmethod
    def del_job_lock(cls, job_name):
        redis_connection.delete(job_name)

    @classmethod
    def wait(cls):
        time.sleep(1)
