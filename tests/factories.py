# -*- coding: UTF-8 -*-
"""
@time:2022/2/25
@author:zhangwei
@file:factories
"""

from l_search.models import db
from l_search import models
import pandas as pd
from l_search import settings


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
    table_name="l_flowpipe",
    has_geo_col=True
)

db_table_detail_dict = [
    {"column_name": "OBJECTID", "column_type": "integer", "column_type_length": "", "column_position": 1,
     "is_extract": True, "is_primary": True},
    {"column_name": "FIRSTNO", "column_type": "nvarchar", "column_type_length": "50", "column_position": 2,
     "is_extract": True, "is_primary": False},
    {"column_name": "ENDNO", "column_type": "nvarchar", "column_type_length": "50", "column_position": 3,
     "is_extract": True, "is_primary": False},
    {"column_name": "PIPELENGTH", "column_type": "numeric", "column_type_length": "38, 8", "column_position": 4,
     "is_extract": True, "is_primary": False},
    {"column_name": "PIPEDIAMETER", "column_type": "integer", "column_type_length": "", "column_position": 5,
     "is_extract": True, "is_primary": False},
    {"column_name": "PIPEMATERIAL", "column_type": "nvarchar", "column_type_length": "50", "column_position": 6,
     "is_extract": True, "is_primary": False},
    {"column_name": "CLASSCODE", "column_type": "integer", "column_type_length": "", "column_position": 7,
     "is_extract": True, "is_primary": False},
    {"column_name": "LOCALITYROAD", "column_type": "nvarchar", "column_type_length": "50", "column_position": 8,
     "is_extract": True, "is_primary": False},
    {"column_name": "OVERHAULDATE", "column_type": "datetime2", "column_type_length": "", "column_position": 9,
     "is_extract": True, "is_primary": False},
    {"column_name": "DEVICESTATE", "column_type": "nvarchar", "column_type_length": "50", "column_position": 10,
     "is_extract": True, "is_primary": False},
    {"column_name": "INSTALLUNIT", "column_type": "nvarchar", "column_type_length": "50", "column_position": 11,
     "is_extract": True, "is_primary": False},
    {"column_name": "MANUFACTURER", "column_type": "nvarchar", "column_type_length": "50", "column_position": 12,
     "is_extract": True, "is_primary": False},
    {"column_name": "PROJECTNAME", "column_type": "nvarchar", "column_type_length": "50", "column_position": 13,
     "is_extract": True, "is_primary": False},
    {"column_name": "BROKENTIMES", "column_type": "integer", "column_type_length": "", "column_position": 14,
     "is_extract": True, "is_primary": False},
    {"column_name": "FINISHDATE", "column_type": "datetime2", "column_type_length": "", "column_position": 15,
     "is_extract": True, "is_primary": False},
    {"column_name": "MAINTAINTIMES", "column_type": "integer", "column_type_length": "", "column_position": 16,
     "is_extract": True, "is_primary": False},
    {"column_name": "PIPEADDRESS", "column_type": "nvarchar", "column_type_length": "50", "column_position": 17,
     "is_extract": True, "is_primary": False},
    {"column_name": "EMBEDMODE", "column_type": "nvarchar", "column_type_length": "50", "column_position": 18,
     "is_extract": True, "is_primary": False},
    {"column_name": "REMARKS", "column_type": "nvarchar", "column_type_length": "50", "column_position": 19,
     "is_extract": True, "is_primary": False},
    {"column_name": "CASENO", "column_type": "nvarchar", "column_type_length": "50", "column_position": 20,
     "is_extract": True, "is_primary": False},
    {"column_name": "UPDATEUSER", "column_type": "nvarchar", "column_type_length": "50", "column_position": 21,
     "is_extract": True, "is_primary": False},
    {"column_name": "PROJECTNO", "column_type": "nvarchar", "column_type_length": "50", "column_position": 22,
     "is_extract": True, "is_primary": False},
    {"column_name": "ADMINNAME", "column_type": "nvarchar", "column_type_length": "50", "column_position": 23,
     "is_extract": True, "is_primary": False},
    {"column_name": "PIPETYPE", "column_type": "nvarchar", "column_type_length": "50", "column_position": 24,
     "is_extract": True, "is_primary": False},
    {"column_name": "PROJECTHEADER", "column_type": "nvarchar", "column_type_length": "50", "column_position": 25,
     "is_extract": True, "is_primary": False},
    {"column_name": "PROJECTADMIN", "column_type": "nvarchar", "column_type_length": "50", "column_position": 26,
     "is_extract": True, "is_primary": False},
    {"column_name": "LATECASENO", "column_type": "nvarchar", "column_type_length": "50", "column_position": 27,
     "is_extract": True, "is_primary": False},
    {"column_name": "LATEUPDATEUSER", "column_type": "nvarchar", "column_type_length": "50", "column_position": 28,
     "is_extract": True, "is_primary": False},
    {"column_name": "LATEUPDATETIME", "column_type": "datetime2", "column_type_length": "", "column_position": 29,
     "is_extract": True, "is_primary": False},
    {"column_name": "USEDYEAR", "column_type": "integer", "column_type_length": "", "column_position": 30,
     "is_extract": True, "is_primary": False},
    {"column_name": "ROUGHCOEF", "column_type": "numeric", "column_type_length": "38, 8", "column_position": 31,
     "is_extract": True, "is_primary": False},
    {"column_name": "FLOW", "column_type": "numeric", "column_type_length": "38, 8", "column_position": 32,
     "is_extract": True, "is_primary": False},
    {"column_name": "VEL", "column_type": "numeric", "column_type_length": "38, 8", "column_position": 33,
     "is_extract": True, "is_primary": False},
    {"column_name": "UNITLOSS", "column_type": "numeric", "column_type_length": "38, 8", "column_position": 34,
     "is_extract": True, "is_primary": False},
    {"column_name": "QUALITY", "column_type": "numeric", "column_type_length": "38, 8", "column_position": 35,
     "is_extract": True, "is_primary": False},
    {"column_name": "REACTIONRATE", "column_type": "numeric", "column_type_length": "38, 8", "column_position": 36,
     "is_extract": True, "is_primary": False},
    {"column_name": "STATUS", "column_type": "numeric", "column_type_length": "38, 8", "column_position": 37,
     "is_extract": True, "is_primary": False},
    {"column_name": "FRICTIONFACTOR", "column_type": "numeric", "column_type_length": "38, 8", "column_position": 38,
     "is_extract": True, "is_primary": False},
    {"column_name": "PIPEDIAMETERHYD", "column_type": "integer", "column_type_length": "", "column_position": 39,
     "is_extract": True, "is_primary": False},
    {"column_name": "INITALSTATUS", "column_type": "nvarchar", "column_type_length": "50", "column_position": 40,
     "is_extract": True, "is_primary": False},
    {"column_name": "PROBESTATIONSNO", "column_type": "nvarchar", "column_type_length": "50", "column_position": 41,
     "is_extract": True, "is_primary": False},
    {"column_name": "GISNO", "column_type": "nvarchar", "column_type_length": "50", "column_position": 42,
     "is_extract": True, "is_primary": False},
    {"column_name": "UPDATETIME", "column_type": "datetime2", "column_type_length": "", "column_position": 43,
     "is_extract": True, "is_primary": False},
    {"column_name": "EDITUSERIP", "column_type": "nvarchar", "column_type_length": "50", "column_position": 44,
     "is_extract": True, "is_primary": False},
    {"column_name": "IMPORTTIME", "column_type": "datetime2", "column_type_length": "", "column_position": 45,
     "is_extract": True, "is_primary": False},
    {"column_name": "IMPORTER", "column_type": "nvarchar", "column_type_length": "50", "column_position": 46,
     "is_extract": True, "is_primary": False},
    {"column_name": "ID", "column_type": "integer", "column_type_length": "", "column_position": 47, "is_extract": True,
     "is_primary": False},
    {"column_name": "FILENAME", "column_type": "nvarchar", "column_type_length": "100", "column_position": 48,
     "is_extract": True, "is_primary": False},
    {"column_name": "SPECIALPIPETYPE", "column_type": "nvarchar", "column_type_length": "50", "column_position": 49,
     "is_extract": True, "is_primary": False},
    {"column_name": "PROJECTTYPE", "column_type": "nvarchar", "column_type_length": "50", "column_position": 50,
     "is_extract": True, "is_primary": False},
    {"column_name": "PIPENO", "column_type": "nvarchar", "column_type_length": "50", "column_position": 51,
     "is_extract": True, "is_primary": False},
    {"column_name": "FIRSTID", "column_type": "nvarchar", "column_type_length": "50", "column_position": 52,
     "is_extract": True, "is_primary": False},
    {"column_name": "ENDID", "column_type": "nvarchar", "column_type_length": "50", "column_position": 53,
     "is_extract": True, "is_primary": False},
    {"column_name": "SHAPE", "column_type": "geometry", "column_type_length": "", "column_position": 54,
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

    def insert_data_to_stag(self, table_info):
        """
        pickle里的数据结构和上面的数据结构保持统一
        :return:
        """
        import os
        ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        pickle_path = os.path.join(ROOT_DIR, "./mock_data/geo_mock_data.pkl")
        insert_data_df = pd.read_pickle(pickle_path)



        table_schema = models.TableDetail.get_table_detail(table_info=table_info,
                                            is_entity=True)
        table_schema_column_type = {}
        for col in table_schema:
            column_type = None
            for c_type_key,v in settings.SWITCH_DIFF_DB_COLUMN_TYPE_ACCORDING_PG.items():
                if col.column_type in v:
                    column_type = c_type_key
                    break

            if column_type is None:
                column_type = col.column_type

            table_schema_column_type[col.column_name] = column_type

        column_type = {}
        drop_column = []
        for c_name,c_type in insert_data_df.dtypes.items():

            if insert_data_df[c_name].isnull().all():
                drop_column.append(c_name)
                continue

            if c_name == "geometry":
                continue

            actual_type = settings.SWITCH_DIFF_DB_COLUMN_TYPE_ACCORDING_PD[table_schema_column_type[c_name]]
            if c_type != actual_type:
                column_type[c_name] = actual_type

        # 删除整列为nan的列
        insert_data_df = insert_data_df.drop(columns=drop_column)
        # 列格式与实际格式不符，进行转换
        insert_data_df = insert_data_df.astype(column_type, errors="ignore")
        # 所有列命小写
        insert_data_df.columns = insert_data_df.columns.str.lower()

        insert_data_df.to_postgis(
            con=db.engine,
            name=str(table_info.table_name).lower(),
            if_exists="append",
            schema=settings.ODS_STAG_SCHEMA_NAME
        )
        return insert_data_df.columns
