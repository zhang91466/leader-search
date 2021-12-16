# -*- coding: UTF-8 -*-
"""
@time:12/16/2021
@author:
@file:tasks_info
"""
from flask_restx import Namespace, Resource, fields, marshal
from l_search import celeryapp

api_task = Namespace('task_info', description='Get task info from celery')


class TaskStatus(Resource):

    def get(self, task_id):
        """
        从调度获取任务执行情况
        :param task_id:
        :return:
        """
        task_result = celeryapp.celery.AsyncResult(task_id)
        result = {
            "task_id": task_id,
            "task_status": task_result.status,
            "task_result": task_result.result
        }
        return result, 200
