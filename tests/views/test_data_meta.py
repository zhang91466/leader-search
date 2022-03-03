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
            "port": 3306,
            "account": "root",
            "pwd": "root1",
            "default_db": "airflow"
        }
        rv = self.make_request("post", "/meta/connection/upsert", data=query_data)
        self.assertEqual(rv.status_code, 200)
        self.assertEqual(type(rv.json[0]["id"]), int)
        query_data.pop("pwd")
        self.assertResponseEqual(query_data, rv.json[0])


class TestTableInfo(BaseTestCase):

    def test_get_tables(self):
        query = self.factory.create_table_info()
        rv = self.make_request("get", "/meta/%d/%s/info" % (query["connection_id"],
                                                            query["table_name"]))
        self.assertResponseEqual(query, rv.json[0])

    def test_upsert_table_info(self):
        create_connection = self.factory.create_db_connect()
        query_data = {
            "connection_id": create_connection["id"],
            "table_name": "test_insert_table",
            "table_extract_col": "update_time",
            "need_extract": True
        }
        rv = self.make_request("post", "/meta/table/info/upsert", data=query_data)
        self.assertEqual(rv.status_code, 200)
        self.assertEqual(type(rv.json[0]["id"]), str)
        self.assertResponseEqual(query_data, rv.json[0])


class TestTableDetail(BaseTestCase):
    test_data = [{
            "column_name": "id",
            "column_type": "integer",
            "column_type_length": "4",
            "column_position": 1,
            "is_primary": True
        },
        {
            "column_name": "test_col_1",
            "column_type": "varhcar",
            "column_type_length": "155",
            "column_position": 2
        }]

    def test_get_table_detail(self):
        query = self.factory.create_table_detail(column_info_list=self.test_data)
        rv = self.make_request("get", "/meta/%s/detail" % (query[0]["table_info_id"]))

        self.assertEqual(len(query), len(rv.json))
        self.assertResponseEqual(query[0], rv.json[0])

    def test_upsert_table_detail(self):
        create_table_info = self.factory.create_table_info()

        for d in self.test_data:
            d["table_info_id"] = create_table_info["id"]

        rv = self.make_request("post", "/meta/table/detail/upsert", data=self.test_data)
        self.assertEqual(rv.status_code, 200)
        self.assertEqual(type(rv.json[0]["id"]), str)
        self.assertResponseEqual(self.test_data[1], rv.json[1])