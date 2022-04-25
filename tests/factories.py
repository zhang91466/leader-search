# -*- coding: UTF-8 -*-
"""
@time:2022/2/25
@author:simonzhang
@file:factories
"""

from l_search.models import db
from l_search import models
import pandas as pd
from l_search import settings
import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
PICKLE_PATH = os.path.join(ROOT_DIR, "./mock_data/geo_mock_data.pkl")


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

mysql_connection_factory = ModelFactory(
    model=models.DBConnect,
    domain="l_search_test",
    db_type="mysql",
    host="192.168.1.107",
    port="7601",
    account="root",
    pwd="leadmap1102",
    default_db="test"
)

pg_connection_factory = ModelFactory(
    model=models.DBConnect,
    domain="l_search_test",
    db_type="postgresql",
    host="192.168.1.55",
    port="5432",
    account="postgres",
    pwd="123456xxx",
    default_db="l_search"
)

db_table_info_factory = ModelFactory(
    model=models.TableInfo,
    table_name="l_flowpipe"
)

db_table_info_increment_factory = ModelFactory(
    model=models.TableInfo,
    table_name="l_flowpipe",
    table_extract_col="updatetime"
)

db_table_detail_dict = [
    {"column_name": "OBJECTID", "column_type": "integer", "column_type_length": "", "column_position": 1,
     "is_extract": True, "is_primary": True},
    {"column_name": "PIPEDIAMETER", "column_type": "integer", "column_type_length": "", "column_position": 5,
     "is_extract": True, "is_primary": False},
    {"column_name": "CLASSCODE", "column_type": "integer", "column_type_length": "", "column_position": 7,
     "is_extract": True, "is_primary": False},
    {"column_name": "LOCALITYROAD", "column_type": "nvarchar", "column_type_length": "50", "column_position": 8,
     "is_extract": True, "is_primary": False},
    {"column_name": "FINISHDATE", "column_type": "datetime2", "column_type_length": "", "column_position": 15,
     "is_extract": True, "is_primary": False},
    {"column_name": "UNITLOSS", "column_type": "numeric", "column_type_length": "38, 8", "column_position": 34,
     "is_extract": True, "is_primary": False},
    {"column_name": "UPDATETIME", "column_type": "datetime2", "column_type_length": "", "column_position": 43,
     "is_extract": True, "is_primary": False},
    {"column_name": "EDITUSERIP", "column_type": "nvarchar", "column_type_length": "50", "column_position": 44,
     "is_extract": True, "is_primary": False},
    {"column_name": "ID", "column_type": "integer", "column_type_length": "", "column_position": 47, "is_extract": True,
     "is_primary": False},
    {"column_name": "period", "column_type": "tsrange", "column_type_length": "", "column_position": 55,
     "is_extract": False, "is_primary": False, "is_system_col": True}]


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

    def create_table_info(self, increment_table=False, db_type="mssql"):
        if db_type == "mssql":
            create_connection = mssql_connection_factory.create()
        elif db_type == "mysql":
            create_connection = mysql_connection_factory.create()
        elif db_type == "postgresql":
            create_connection = pg_connection_factory.create()

        if increment_table is True:
            table_info_data = db_table_info_increment_factory
        else:
            table_info_data = db_table_info_factory
        table_info_data.kwargs["connection_id"] = create_connection.id
        create_table_info = table_info_data.create()

        result_data = {"id": create_table_info.id,
                       "connection_id": create_table_info.connection_id,
                       "table_name": create_table_info.table_name,
                       "table_extract_col": create_table_info.table_extract_col,
                       "is_entity": create_table_info.is_entity,
                       "latest_extract_date": create_table_info.latest_extract_date
                       }
        return result_data

    def create_table_detail(self,
                            db_type="mssql",
                            column_info_list=None,
                            has_geo=True,
                            geo_name="shape",
                            increment_table=False):

        if column_info_list is None:
            column_info_list = db_table_detail_dict

            if has_geo is True:
                if geo_name == "shape":
                    column_info_list.append(
                        {"column_name": "SHAPE", "column_type": "geometry", "column_type_length": "",
                         "column_position": 54,
                         "is_extract": True, "is_primary": False})
                elif geo_name == "geometry":
                    column_info_list.append(
                        {"column_name": "geometry", "column_type": "geometry", "column_type_length": "",
                         "column_position": 54,
                         "is_extract": True, "is_primary": False})

        create_table_info = self.create_table_info(increment_table=increment_table,
                                                   db_type=db_type)

        result = []

        for i in column_info_list:
            column_new_data = ModelFactory(model=models.TableDetail, **i)
            column_new_data.kwargs["table_info_id"] = create_table_info["id"]
            column_new_data.create()
            column_new_data_dict = column_new_data.kwargs
            result.append(column_new_data_dict)

        return result

    def close_extract_on_column(self, column_info):
        column_info.is_extract = False
        db.session.add(column_info)
        db.session.commit()
        # column_info_dict = column_info.__dict__
        # column_info_dict.pop("_sa_instance_state")
        # column_info_dict.pop("created_at")
        # column_info_dict.pop("updated_at")
        # column_info_dict["is_extract"] = False

        # models.TableDetail.upsert(input_data=column_info_dict)

    def get_pickle_data(self, table_info):
        """
        pickle里的数据结构和上面的数据结构保持统一
        :return:
        """
        insert_data_df = pd.read_pickle(PICKLE_PATH)

        table_schema = models.TableDetail.get_table_detail(table_info=table_info,
                                                           is_entity=True)
        table_schema_column_type = {}
        for col in table_schema:
            column_type = None
            for c_type_key, v in settings.SWITCH_DIFF_DB_COLUMN_TYPE_ACCORDING_PG.items():
                if col.column_type in v:
                    column_type = c_type_key
                    break

            if column_type is None:
                column_type = col.column_type

            table_schema_column_type[col.column_name] = column_type

        need_change_column_type = {}
        drop_column = []
        for c_name, c_type in insert_data_df.dtypes.items():

            if insert_data_df[c_name].isnull().all():
                drop_column.append(c_name)
                continue

            if c_name == "geometry":
                continue

            actual_type = settings.SWITCH_DIFF_DB_COLUMN_TYPE_ACCORDING_PD[table_schema_column_type[c_name]]
            if c_type != actual_type[0]:
                insert_data_df[c_name] = insert_data_df[c_name].fillna(actual_type[1])
                need_change_column_type[c_name] = actual_type[0]

        # 删除整列为nan的列
        insert_data_df = insert_data_df.drop(columns=drop_column)
        # 列格式与实际格式不符，进行转换, errors="ignore"
        insert_data_df = insert_data_df.astype(need_change_column_type)
        # 所有列命小写
        insert_data_df.columns = insert_data_df.columns.str.lower()

        return insert_data_df

    def insert_data_to_stag(self, table_info):
        insert_data_df = self.get_pickle_data(table_info=table_info)
        insert_data_df.to_postgis(
            con=db.engine,
            name=str(table_info.table_name).lower(),
            if_exists="append",
            schema=settings.ODS_STAG_SCHEMA_NAME
        )
        return insert_data_df.columns, len(insert_data_df.index)

    def insert_mock_data_to_source_db(self, table_info):
        from l_search.models.extract_table_models import DBSession

        insert_data_df = pd.read_pickle(PICKLE_PATH)
        insert_data_df = insert_data_df.drop(columns=["geometry"])

        connection = DBSession(connection_info=table_info.connection)

        insert_data_df.to_sql(
            con=connection.engine,
            name=str(table_info.table_name).lower(),
            if_exists="replace",
            index=False
        )
        return len(insert_data_df.index)
