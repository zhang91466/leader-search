# -*- coding: UTF-8 -*-
"""
@time:12/16/2021
@author:
@file:extract_table_models
"""
from l_search.models.base import db
from l_search.utils.logger import Logger

logger = Logger()

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
        logger.debug("table %s start create" % table_name)
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
        logger.debug("table %s start truncate" % table_name)
        truncate_stat = """truncate table %s""" % table_name
        db.session.execute(truncate_stat)
        db.session.commit()

    @classmethod
    def insert(cls, table_name, columns_in_order, values_in_order):
        logger.debug("Table %s start insert" % table_name)
        # """INSERT INTO full_text_index (id, extract_data_info_id, block_name, block_key, row_content) VALUES (:id, :extract_data_info_id, :block_name, :block_key, :row_content)"""

        insert_stat = """insert into %(table_name)s (%(columns)s) values (%(values)s)""" % {
            "table_name": table_name,
            "columns": "`" + "`,`".join(columns_in_order) + "`",
            "values": ":" + ", :".join(columns_in_order)
        }
        execute_result = db.session.execute(insert_stat, values_in_order)
        db.session.commit()
        row_count = execute_result.rowcount
        logger.info("Table %s insert count %d" % (table_name, row_count))
        return row_count

    @classmethod
    def delete(cls, table_name, where_stat=None):
        logger.debug("table %s start delete" % table_name)

        if where_stat:
            where_stat = " where " + where_stat
        else:
            where_stat = ""

        delete_stat = "delete from %(table_name)s %(where_stat)s" % {"table_name": table_name,
                                                                     "where_stat": where_stat}
        db.session.execute(delete_stat)
        db.session.commit()

    @classmethod
    def drop_table(cls, table_name):
        logger.debug("table %s start drop" % table_name)
        drop_stat = "drop table if exists %s" % table_name
        db.session.execute(drop_stat)
        db.session.commit()

    @classmethod
    def select(cls, table_name, column_list=None, where_stat=None):
        logger.debug("table %s start select" % table_name)

        if column_list:
            column_list_str = "`" + "`,`".join(column_list) + "`"
        else:
            column_list_str = " * "

        if where_stat:
            where_stat_str = " where " + where_stat.split("select")[0]
        else:
            where_stat_str = ""

        select_stat = "select %(column_list)s from %(table_name)s %(where_stat)s" % {"column_list": column_list_str,
                                                                                    "table_name": table_name,
                                                                                    "where_stat": where_stat_str}
        execute_data = db.session.execute(select_stat)
        return [dict(row) for row in execute_data]
