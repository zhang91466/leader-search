# -*- coding: UTF-8 -*-
"""
@time:2022/2/25
@author:zhangwei
@file:test_data_meta
"""
from tests import BaseTestCase


class TestConnectionInfo(BaseTestCase):

    def test_connection_get(self):
        query = self.factory.create_db_connect()
        rv = self.make_request("get", "/meta/connection/%s/%s/%s" % (query["domain"],
                                                                     query["db_type"],
                                                                     query["default_db"]))
        self.assertResponseEqual(query, rv.json[0])

    def test_connection_post(self):
        query_data = {
            "domain": "test_mysql222",
            "db_type": "mysql",
            "host": "192.168.1.222",
            "port": "3306",
            "account": "root",
            "pwd": "root1",
            "default_db": "airflow"
        }
        rv = self.make_request("post", "/meta/connection/create", data=query_data)
        self.assertEqual(rv.status_code, 200)
        self.assertEqual(type(rv.json["connection_id"]), int)
