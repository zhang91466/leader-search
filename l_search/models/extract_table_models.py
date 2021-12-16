# -*- coding: UTF-8 -*-
"""
@time:12/16/2021
@author:
@file:extract_table_models
"""
from l_search.models.base import db

COLUMN_TYPE_MAPPING = {"postgresql": {"varchar": "varchar",
                                      "integer": "int",
                                      "numeric": "numeric",
                                      "text": "text",
                                      "timestamp": "timestamp"
                                      }}

DONOT_CREATE_COLUMN = ["geom"]


class TableOperate:

    @classmethod
    def create(cls, table_name, meta_data):

        create_stat = """create table if not exists %(table_name)s (
        
        """
        column_stat = """`%(column_name)s` %(column_type)s%(column_length)s,
        """
        close_stat = """
        
        ) ENGINE=INNODB DEFAULT CHARSET=utf8"""

        meta_column = ""
        for col in meta_data:

            if col.column_name not in DONOT_CREATE_COLUMN:
                if col.column_type_length:
                    column_length = "(%s)" % col.column_type_length
                else:
                    column_length = ""

                meta_column = meta_column + column_stat % {"column_name": col.column_name,
                                                           "column_type": COLUMN_TYPE_MAPPING[col.type][
                                                               col.column_type],
                                                           "column_length": column_length}

        create_table_sql = create_stat % {"table_name": table_name} + meta_column.strip()[:-1] + close_stat

        db.session.execute(create_table_sql)
        db.session.commit()
        return table_name

    @classmethod
    def alter_table(cls):
        pass

    @classmethod
    def truncate(cls, table_name):
        truncate_stat = """truncate table %s""" % table_name
        db.session.execute(truncate_stat)
        db.session.commit()

    @classmethod
    def insert(cls, table_name, columns_in_order, values_in_order):
        """INSERT INTO full_text_index (id, extract_data_info_id, block_name, block_key, row_content) VALUES (:id, :extract_data_info_id, :block_name, :block_key, :row_content)"""
        insert_stat = """insert into %(table_name)s (%(columns)s) values (%(values)s)"""


    @classmethod
    def upsert(cls):
        pass

    @classmethod
    def delete(cls):
        pass

    @classmethod
    def drop_table(cls):
        pass
