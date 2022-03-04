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


mssql_connection_factory = ModelFactory(
    model=models.DBConnect,
    domain="l_search_test",
    db_type="mssql",
    host="192.168.1.31",
    port="2433",
    account="sa",
    pwd="m?~9nfhqZR%TXzY",
    default_db="LM_XS_ARC_WATER"
)

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
    table_name="P_WATERMETER",
    has_geo_col=True
)

db_table_detail_dict = [
    {"column_name": "OBJECTID", "column_type": "integer", "column_type_length": "", "column_position": 1,
     "is_extract": True, "is_primary": True},
    {"column_name": "LAT", "column_type": "numeric", "column_type_length": "38, 8", "column_position": 2,
     "is_extract": True, "is_primary": False},
    {"column_name": "LON", "column_type": "numeric", "column_type_length": "38, 8", "column_position": 3,
     "is_extract": True, "is_primary": False},
    {"column_name": "CALIBER", "column_type": "integer", "column_type_length": "", "column_position": 4,
     "is_extract": True, "is_primary": False},
    {"column_name": "MATERIAL", "column_type": "nvarchar", "column_type_length": "50", "column_position": 5,
     "is_extract": True, "is_primary": False},
    {"column_name": "CLASSCODE", "column_type": "integer", "column_type_length": "", "column_position": 6,
     "is_extract": True, "is_primary": False},
    {"column_name": "UPDATEUSER", "column_type": "nvarchar", "column_type_length": "50", "column_position": 7,
     "is_extract": True, "is_primary": False},
    {"column_name": "METERBOXNO", "column_type": "nvarchar", "column_type_length": "50", "column_position": 8,
     "is_extract": True, "is_primary": False},
    {"column_name": "WMETERUSERID", "column_type": "integer", "column_type_length": "", "column_position": 9,
     "is_extract": True, "is_primary": False},
    {"column_name": "WMSELFNO", "column_type": "nvarchar", "column_type_length": "50", "column_position": 10,
     "is_extract": True, "is_primary": False},
    {"column_name": "USERNAME", "column_type": "nvarchar", "column_type_length": "50", "column_position": 11,
     "is_extract": True, "is_primary": False},
    {"column_name": "CHECKCYCLE", "column_type": "nvarchar", "column_type_length": "50", "column_position": 12,
     "is_extract": True, "is_primary": False},
    {"column_name": "GISNO", "column_type": "nvarchar", "column_type_length": "50", "column_position": 13,
     "is_extract": True, "is_primary": False},
    {"column_name": "UPDATETIME", "column_type": "datetime2", "column_type_length": "", "column_position": 14,
     "is_extract": True, "is_primary": False},
    {"column_name": "EDITUSERIP", "column_type": "nvarchar", "column_type_length": "50", "column_position": 15,
     "is_extract": True, "is_primary": False},
    {"column_name": "IMPORTTIME", "column_type": "datetime2", "column_type_length": "", "column_position": 16,
     "is_extract": True, "is_primary": False},
    {"column_name": "IMPORTER", "column_type": "nvarchar", "column_type_length": "50", "column_position": 17,
     "is_extract": True, "is_primary": False},
    {"column_name": "ID", "column_type": "integer", "column_type_length": "", "column_position": 18, "is_extract": True,
     "is_primary": False},
    {"column_name": "METERADDRESS", "column_type": "nvarchar", "column_type_length": "50", "column_position": 19,
     "is_extract": True, "is_primary": False},
    {"column_name": "FILENAME", "column_type": "nvarchar", "column_type_length": "100", "column_position": 20,
     "is_extract": True, "is_primary": False},
    {"column_name": "PROJECTNAME", "column_type": "nvarchar", "column_type_length": "100", "column_position": 21,
     "is_extract": True, "is_primary": False},
    {"column_name": "PROJECTNO", "column_type": "nvarchar", "column_type_length": "50", "column_position": 22,
     "is_extract": True, "is_primary": False},
    {"column_name": "FINISHDATE", "column_type": "datetime2", "column_type_length": "", "column_position": 23,
     "is_extract": True, "is_primary": False},
    {"column_name": "PROJECTHEADER", "column_type": "nvarchar", "column_type_length": "50", "column_position": 24,
     "is_extract": True, "is_primary": False},
    {"column_name": "PROJECTADMIN", "column_type": "nvarchar", "column_type_length": "50", "column_position": 25,
     "is_extract": True, "is_primary": False},
    {"column_name": "ADMINNAME", "column_type": "nvarchar", "column_type_length": "50", "column_position": 26,
     "is_extract": True, "is_primary": False},
    {"column_name": "PROJECTTYPE", "column_type": "nvarchar", "column_type_length": "50", "column_position": 27,
     "is_extract": True, "is_primary": False},
    {"column_name": "REMARKS", "column_type": "nvarchar", "column_type_length": "50", "column_position": 28,
     "is_extract": True, "is_primary": False},
    {"column_name": "IMPORTNO", "column_type": "nvarchar", "column_type_length": "255", "column_position": 29,
     "is_extract": True, "is_primary": False},
    {"column_name": "SHAPE", "column_type": "geometry", "column_type_length": "", "column_position": 30,
     "is_extract": True, "is_primary": False}]


class Factory:

    def create_db_connect(self, return_dict=True):
        create_data = mssql_connection_factory.create()
        if return_dict:
            result_data = {"id": create_data.id,
                           "domain": create_data.domain,
                           "db_type": create_data.db_type,
                           "host": create_data.host,
                           "port": create_data.port,
                           "account": create_data.account,
                           "default_db": create_data.default_db}
        else:
            result_data = create_data
        return result_data

    def create_table_info(self):
        create_connection = db_connection_factory.create()
        table_info_data = db_table_info_factory
        table_info_data.kwargs["connection_id"] = create_connection.id
        create_table_info = table_info_data.create()

        result_data = {"id": create_table_info.id,
                       "connection_id": create_table_info.connection_id,
                       "table_name": create_table_info.table_name,
                       "table_extract_col": create_table_info.table_extract_col,
                       "need_extract": create_table_info.need_extract,
                       "latest_extract_date": create_table_info.latest_extract_date
                       }
        return result_data

    def create_table_detail(self, column_info_list=None):

        if column_info_list is None:
            column_info_list = db_table_detail_dict

        create_table_info = self.create_table_info()

        result = []

        for i in column_info_list:
            column_new_data = ModelFactory(model=models.TableDetail, **i)
            column_new_data.kwargs["table_info_id"] = create_table_info["id"]
            column_new_data.create()
            column_new_data_dict = column_new_data.kwargs
            result.append(column_new_data_dict)

        return result
