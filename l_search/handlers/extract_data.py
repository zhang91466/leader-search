# -*- coding: UTF-8 -*-
"""
@time:12/16/2021
@author:
@file:extract_data
"""
from l_search.handlers.meta_operation import Meta
from l_search import models
from l_search.models.extract_table_models import TableOperate


class ExtractData:
    domain = ""
    db_object_type = ""
    db_name = ""

    @classmethod
    def init(cls, table_name, need_drop=False):

        table_extract_main = models.ExtractDataInfo.get_by_table_name(domain=cls.domain,
                                                                      db_object_type=cls.db_object_type,
                                                                      db_name=cls.db_name,
                                                                      table_name=table_name)

        table_schema = models.DBMetadata.get_table_info(domain=cls.domain,
                                                        type=models.DBObjectType[cls.db_object_type].value,
                                                        default_db=cls.db_name,
                                                        table_name=table_name,
                                                        is_extract=None)

        # 创建表
        if table_extract_main is None or table_extract_main.is_entity is None:

            primary_column_name = None
            extract_column_name = None
            for col in table_schema:

                if col.is_primary == 1:
                    primary_column_name = col.column_name

                if col.is_extract_filter == 1:
                    extract_column_name = col.column_name

            table_extract_main = models.ExtractDataInfo.create(domain=cls.domain,
                                                               db_object_type=cls.db_object_type,
                                                               db_name=cls.db_name,
                                                               table_name=table_name,
                                                               table_primary_id=primary_column_name,
                                                               table_extract_col=extract_column_name,
                                                               is_entity=True)

            table_name = "%(extart_data_info_id)d_%(table_name)s" % {"extart_data_info_id": table_extract_main.id,
                                                                     "table_name": table_name}
            TableOperate.create(table_name=table_name,
                                meta_data=table_schema)
        else:

            table_name = "%(extart_data_info_id)d_%(table_name)s" % {"extart_data_info_id": table_extract_main,
                                                                     "table_name": table_name}

            # 是否重置
            if need_drop:
                cls.drop(table_name=table_name)
                models.ExtractDataInfo.upsert(table_data=table_extract_main, is_entity=None)
            else:
                TableOperate.truncate(table_name=table_name)

        TableOperate.insert(table_name=table_name, meta_data=table_schema)

    @classmethod
    def upsert(cls, table_name):
        pass

    @classmethod
    def get(cls, table_name):
        pass

    @classmethod
    def delete(cls, table_name):
        pass

    @classmethod
    def drop(cls, table_name):
        pass
