# -*- coding: UTF-8 -*-
"""
@time:2022/2/25
@author:zhangwei
@file:__init__.py
"""
import simplejson
import datetime
from unittest import TestCase

from l_search.app import create_app
from l_search.models import db, convert_to_dict
from tests.factories import Factory


class BaseTestCase(TestCase):
    def setUp(self):
        self.app = create_app()
        self.db = db
        self.app.config["TESTING"] = True
        self.app_ctx = self.app.app_context()
        self.app_ctx.push()
        db.session.close()
        db.drop_all()
        db.create_all()
        self.client = self.app.test_client()
        self.factory = Factory()

    def tearDown(self):
        db.session.remove()
        db.get_engine(self.app).dispose()
        self.app_ctx.pop()

    def make_request(
            self,
            method,
            path,
            data=None,
            is_json=True,
            follow_redirects=False,
    ):

        method_fn = getattr(self.client, method.lower())
        headers = {}

        if data and is_json:
            data = simplejson.dumps(data)

        if is_json:
            content_type = "application/json"
        else:
            content_type = None

        response = method_fn(
            path,
            data=data,
            headers=headers,
            content_type=content_type,
            follow_redirects=follow_redirects,
        )
        return response

    def assertResponseEqual(self, expected, actual):
        for k, v in expected.items():
            if isinstance(v, datetime.datetime) or isinstance(actual[k], datetime.datetime):
                continue

            if isinstance(v, list):
                continue

            if isinstance(v, dict):
                self.assertResponseEqual(v, actual[k])
                continue

            self.assertEqual(
                v,
                actual[k],
                "{} not equal (expected: {}, actual: {}).".format(k, v, actual[k]),
            )