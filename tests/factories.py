# -*- coding: UTF-8 -*-
"""
@time:2022/2/25
@author:zhangwei
@file:factories
"""

from l_search.models import db
from l_search import models


class ModelFactory:
    def __init__(self, model, **kwargs):
        self.model = model
        self.kwargs = kwargs

    def _get_kwargs(self, override_kwargs):
        kwargs = self.kwargs.copy()
        kwargs.update(override_kwargs)

        for key, arg in kwargs.items():
            if callable(arg):
                kwargs[key] = arg()

        return kwargs

    def create(self, **override_kwargs):
        kwargs = self._get_kwargs(override_kwargs)
        obj = self.model(**kwargs)
        db.session.add(obj)
        db.session.commit()
        return obj


db_connection_factory = ModelFactory(
    model=models.DBConnect,
    domain="l_search_test",
    db_type="mysql",
    host="192.168.1.107",
    port="7601",
    account="root",
    pwd="leadmap1102",
    default_db="operation"
)

db_table_info_factory = ModelFactory(
    model=models.TableInfo,
    table_name="test_table",
    table_primary_col="id",
    table_primary_col_is_int=True,
    table_extract_col="update_ts"
)


class Factory:

    def create_db_connect(self):
        create_data = db_connection_factory.create()
        result_data = {"id": create_data.id,
                       "domain": create_data.domain,
                       "db_type": create_data.db_type,
                       "host": create_data.host,
                       "port": create_data.port,
                       "account": create_data.account,
                       "default_db": create_data.default_db}
        return result_data

    def create_table_info(self):
        create_connection = db_connection_factory.create()
        table_info_data = db_table_info_factory
        table_info_data.kwargs["connection_id"] = create_connection.id
        create_table_info = table_info_data.create()

        result_data = {"id": create_table_info.id,
                       "connection_id": create_table_info.connection_id,
                       "table_name": create_table_info.table_name,
                       "table_primary_col": create_table_info.table_primary_col,
                       "table_primary_col_is_int": create_table_info.table_primary_col_is_int,
                       "table_extract_col": create_table_info.table_extract_col,
                       "need_extract": create_table_info.need_extract,
                       "latest_table_primary_id": create_table_info.latest_table_primary_id,
                       "latest_extract_date": create_table_info.latest_extract_date
                       }
        return result_data

    def create_table_detail(self, column_info_list):
        create_table_info = self.create_table_info()

        result = []

        for i in column_info_list:
            column_new_data = ModelFactory(model=models.TableDetail, **i)
            column_new_data.kwargs["table_info_id"] = create_table_info["id"]
            column_new_data.create()
            column_new_data_dict = column_new_data.kwargs
            result.append(column_new_data_dict)

        return result
