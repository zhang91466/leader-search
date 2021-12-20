# -*- coding: UTF-8 -*-
"""
@time:12/16/2021
@author:
@file:extract_data
"""

from l_search import models
from l_search.models.base import db
from l_search.models.extract_table_models import TableOperate, DONOT_CREATE_COLUMN
from l_search.handlers.source_meta_operate.handle.meta_handle import MetaDetector
from l_search import settings
from l_search.utils.logger import Logger
from l_search.utils import json_converter
import pandas as pd
import simplejson as json

logger = Logger()


class ExtractData:
    domain = ""
    db_object_type = ""
    db_name = ""

    @classmethod
    def get_table_name_in_db(cls, table_name, extract_data_info=None):
        if extract_data_info is None:
            extract_data_info = models.ExtractDataInfo.get_by_table_name(domain=cls.domain,
                                                                         db_object_type=cls.db_object_type,
                                                                         db_name=cls.db_name,
                                                                         table_name=table_name)
        return "%(extract_data_info_id)d_%(table_name)s" % {"extract_data_info_id": extract_data_info.id,
                                                            "table_name": table_name}

    @classmethod
    def update_extract_data_info(cls, table_name, table_schema=None, extract_data_info=None):

        if table_schema is None:
            table_schema = models.DBMetadata.get_table_info(domain=cls.domain,
                                                            type=models.DBObjectType[cls.db_object_type].value,
                                                            default_db=cls.db_name,
                                                            table_name=table_name,
                                                            is_extract=True)

        if extract_data_info is None:
            extract_data_info = models.ExtractDataInfo.get_by_table_name(domain=cls.domain,
                                                                         db_object_type=cls.db_object_type,
                                                                         db_name=cls.db_name,
                                                                         table_name=table_name)

        primary_col_type_is_int = False
        extract_column_name = None
        primary_column_name = None
        for column_info in table_schema:
            if column_info.is_primary == 1:
                primary_column_name = column_info.column_name

                if column_info.column_type not in settings.STRING_COLUMN_TYPE:
                    primary_col_type_is_int = True

            if column_info.is_extract_filter == 1:
                extract_column_name = column_info.column_name

        if primary_col_type_is_int:
            latest_table_primary_id_stat = """max(%s) as latest_table_primary_id""" % primary_column_name
        else:
            latest_table_primary_id_stat = """ 0 as latest_table_primary_id"""

        if extract_column_name is None:
            latest_extract_date_stat = """ '' as latest_extract_date"""
        else:
            latest_extract_date_stat = """ max(%s) as latest_extract_date""" % extract_column_name

        get_latest_date_sql = """
                            select 
                            %(latest_table_primary_id)s
                            ,%(latest_extract_date)s
                            from %(table_name)s limit 1""" % {"latest_table_primary_id": latest_table_primary_id_stat,
                                                              "latest_extract_date": latest_extract_date_stat,
                                                              "table_name": cls.get_table_name_in_db(
                                                                  table_name=table_name,
                                                                  extract_data_info=extract_data_info)}

        extract_data = db.session.execute(get_latest_date_sql)

        if extract_data.rowcount > 0:
            extract_data = extract_data.fetchone()
            aa = extract_data["latest_table_primary_id"]
            extract_data_info.table_primary_id = primary_column_name
            extract_data_info.table_primary_id_is_int = primary_col_type_is_int
            extract_data_info.table_extract_col = extract_column_name
            extract_data_info.latest_table_primary_id = extract_data["latest_table_primary_id"]
            if extract_data["latest_extract_date"] == "":
                latest_extract_date = None
            else:
                latest_extract_date = extract_data["latest_extract_date"]
            extract_data_info.latest_extract_date = latest_extract_date
            models.ExtractDataInfo.upsert(table_data=extract_data_info)

    @classmethod
    def get_extract_sql(cls, table_name, where_stat=None, table_schema=None):
        if table_schema is None:
            table_schema = models.DBMetadata.get_table_info(domain=cls.domain,
                                                            type=models.DBObjectType[cls.db_object_type].value,
                                                            default_db=cls.db_name,
                                                            table_name=table_name,
                                                            is_extract=True)

        column_name_in_order_list = [column_info.column_name for column_info in table_schema if
                                     column_info.column_name not in DONOT_CREATE_COLUMN]

        if where_stat:
            where_stat = "where " + where_stat.split("select")[0]
        else:
            where_stat = ""

        data_extract_sql = """
        select
        %(col)s
        from %(table_name)s
        %(where_stat)s
        """ % {
            "col": ",".join(column_name_in_order_list),
            "table_name": table_name,
            "where_stat": where_stat
        }
        return data_extract_sql, column_name_in_order_list

    @classmethod
    def extract_data(cls, data_extract_sql):
        meta_detector = MetaDetector(domain=cls.domain,
                                     type=models.DBObjectType[cls.db_object_type].value)

        return meta_detector.execute_select_sql(sql_text=data_extract_sql)

    @classmethod
    def insert(cls, table_name, table_schema=None, extract_data_info=None):

        data_extract_sql, column_name_in_order_list = cls.get_extract_sql(table_name=table_name,
                                                                          table_schema=table_schema)

        extract_data = cls.extract_data(data_extract_sql=data_extract_sql)

        insert_row_count = TableOperate.insert(table_name=cls.get_table_name_in_db(table_name=table_name,
                                                                                   extract_data_info=extract_data_info),
                                               columns_in_order=column_name_in_order_list,
                                               values_in_order=extract_data)

        cls.update_extract_data_info(table_name=table_name,
                                     table_schema=table_schema,
                                     extract_data_info=extract_data_info)

        return insert_row_count

    @classmethod
    def upsert(cls, table_name):
        """
        先查看主表信息该schema是否被实体化,
        如果实体化,然后查看主键id是否是int
        如果是int则删除抽取出来的数据小于记录的最大值
        如果是string则删除抽取出来所有相关的id
        最后插入数据
        :param table_name:
        :return:
        """
        extract_data_info = models.ExtractDataInfo.get_by_table_name(domain=cls.domain,
                                                                     db_object_type=cls.db_object_type,
                                                                     db_name=cls.db_name,
                                                                     table_name=table_name)
        where_stat = None
        if extract_data_info.is_entity:
            if extract_data_info.table_extract_col is not None:
                where_stat = """ %s > '%s'""" % (extract_data_info.table_extract_col,
                                                 extract_data_info.latest_extract_date)

            data_extract_sql, column_name_in_order_list = cls.get_extract_sql(table_name=table_name,
                                                                              where_stat=where_stat)

            extract_data = cls.extract_data(data_extract_sql=data_extract_sql)

            extract_data_df = pd.DataFrame(extract_data)

            if extract_data_info.table_primary_id_is_int:
                extract_data_df_less_max_p_id_df = extract_data_df[
                    extract_data_df[extract_data_info.table_primary_id].astype(int) < int(
                        extract_data_info.latest_table_primary_id)]
                extract_data_df_less_max_p_id_list = extract_data_df_less_max_p_id_df[
                    extract_data_info.table_primary_id].astype(str).to_list()
                delete_value = ",".join(extract_data_df_less_max_p_id_list)
            else:
                extract_data_df_less_max_p_id_list = extract_data_df[extract_data_info.table_primary_id].astype(
                    str).to_list()
                delete_value = "'" + "','".join(extract_data_df_less_max_p_id_list) + "'"

            # 删除已经存在的id
            delete_where_stat = " %(column_name)s in (%(delete_value)s)" % {
                "column_name": extract_data_info.table_primary_id,
                "delete_value": delete_value}
            TableOperate.delete(table_name=cls.get_table_name_in_db(table_name=table_name,
                                                                    extract_data_info=extract_data_info),
                                where_stat=delete_where_stat)

            # 插入数据
            insert_row_count = TableOperate.insert(table_name=cls.get_table_name_in_db(table_name=table_name,
                                                                                       extract_data_info=extract_data_info),
                                                   columns_in_order=column_name_in_order_list,
                                                   values_in_order=extract_data)

            cls.update_extract_data_info(table_name=table_name,
                                         extract_data_info=extract_data_info)

            return {"status": "success",
                    "info": "抽取%d条数据" % insert_row_count}
        else:
            return {"status": "failed",
                    "info": "实体表不存在，请创建实体表"}

    @classmethod
    def drop(cls, table_name, extract_data_info=None):
        if extract_data_info is None:
            extract_data_info = models.ExtractDataInfo.get_by_table_name(domain=cls.domain,
                                                                         db_object_type=cls.db_object_type,
                                                                         db_name=cls.db_name,
                                                                         table_name=table_name)
        table_name_in_db = cls.get_table_name_in_db(table_name=table_name,
                                                    extract_data_info=extract_data_info)
        try:
            TableOperate.drop_table(table_name=table_name_in_db)
            extract_data_info.is_entity = False
            extract_data_info.latest_table_primary_id = None
            extract_data_info.latest_extract_date = None
            models.ExtractDataInfo.upsert(table_data=extract_data_info)
        except Exception as e:
            return e


    @classmethod
    def init(cls, table_name, need_drop=False):
        """
        初始化表
        若表不存在,则创建,并插入数据
        若表存在,看是否要删除,若删除则创建,不删除则清空表数据,并插入数据
        :param table_name:
        :param need_drop:
        :return:
        """

        extract_data_info = models.ExtractDataInfo.get_by_table_name(domain=cls.domain,
                                                                     db_object_type=cls.db_object_type,
                                                                     db_name=cls.db_name,
                                                                     table_name=table_name)

        table_schema = models.DBMetadata.get_table_info(domain=cls.domain,
                                                        type=models.DBObjectType[cls.db_object_type].value,
                                                        default_db=cls.db_name,
                                                        table_name=table_name,
                                                        is_extract=True)
        need_create = True
        # 清空表数据
        if extract_data_info and extract_data_info.is_entity is True:
            table_name_in_db = cls.get_table_name_in_db(table_name=table_name,
                                                        extract_data_info=extract_data_info)

            # 是否重置
            if need_drop:
                cls.drop(table_name=table_name, extract_data_info=extract_data_info)
            else:
                TableOperate.truncate(table_name=table_name_in_db)
                need_create = False

        # 创建表
        if need_create is True:
            primary_column_name = None
            extract_column_name = None
            for col in table_schema:

                if col.is_primary == 1:
                    primary_column_name = col.column_name

                if col.is_extract_filter == 1:
                    extract_column_name = col.column_name

            if extract_data_info is None:
                extract_data_info = models.ExtractDataInfo.create(domain=cls.domain,
                                                                  db_object_type=cls.db_object_type,
                                                                  db_name=cls.db_name,
                                                                  table_name=table_name,
                                                                  table_primary_id=primary_column_name,
                                                                  table_extract_col=extract_column_name,
                                                                  is_entity=True)
            else:
                extract_data_info.is_entity = True
                models.ExtractDataInfo.upsert(table_data=extract_data_info)

            table_name_in_db = cls.get_table_name_in_db(table_name=table_name,
                                                        extract_data_info=extract_data_info)
            TableOperate.create(table_name=table_name_in_db,
                                meta_data=table_schema)

        insert_row_count = cls.insert(table_name=table_name, table_schema=table_schema)
        return {"status": "success",
                "insert": insert_row_count}

    @classmethod
    def get(cls, table_name, column_list=None, where_stat=None):
        extract_data_info = models.ExtractDataInfo.get_by_table_name(domain=cls.domain,
                                                                     db_object_type=cls.db_object_type,
                                                                     db_name=cls.db_name,
                                                                     table_name=table_name)

        if extract_data_info.is_entity:

            if column_list:
                table_schema = models.DBMetadata.get_table_info(domain=cls.domain,
                                                                type=models.DBObjectType[cls.db_object_type].value,
                                                                default_db=cls.db_name,
                                                                table_name=table_name,
                                                                is_extract=True)

                column_name_in_order_list = [column_info.column_name for column_info in table_schema if
                                             column_info.column_name not in DONOT_CREATE_COLUMN]

                need_remove_col = []

                for index in range(len(column_list)):
                    if column_list[index] not in column_name_in_order_list:
                        need_remove_col.append(column_list[index])
                column_list = list(set(column_list) - set(need_remove_col))

            select_data = TableOperate.select(table_name=cls.get_table_name_in_db(table_name=table_name,
                                                                                  extract_data_info=extract_data_info),
                                              column_list=column_list,
                                              where_stat=where_stat)
            select_data_df = pd.DataFrame(select_data)
            all_columns = list(select_data_df)  # Creates list of all column headers
            select_data_df[all_columns] = select_data_df[all_columns].astype(str)
            return {"status": "success",
                    "data": select_data_df.T.to_dict()}

        else:
            return {"status": "failed",
                    "info": "实体表不存在，请创建实体表"}
