# -*- coding: UTF-8 -*-
"""
@time:12/16/2021
@author:
@file:extract_table_models
"""
from l_search import models
from l_search.models.base import db
from l_search.utils.logger import Logger
from l_search import settings

DONOT_CREATE_COLUMN = []

logger = Logger()


class TableOperate:

    @staticmethod
    def get_real_table_name(table_name, is_stag=False):
        if is_stag:
            table_name = "%s.%s" % (settings.ODS_STAG_SCHEMA_NAME, str(table_name).lower())
        else:
            table_name = "%s.%s" % (settings.ODS_SCHEMA_NAME, str(table_name).lower())
        return table_name

    @classmethod
    def create_table(cls, table_info, is_stag=False, is_commit=True):
        """
        创建表
        :param table_info: object TableInfo query result
        :return: table_name
        """

        table_name = cls.get_real_table_name(table_name=table_info.table_name, is_stag=is_stag)
        logger.debug("table %s start create" % table_name)

        table_detail = models.TableDetail.get_table_detail(table_info=table_info,
                                                           is_extract=True,
                                                           is_system_col=True)

        create_stat = """create table if not exists %(table_name)s (
        
        """
        column_stat = """%(column_name)s %(column_type)s%(column_length)s,
        """
        close_stat = """
        
        )"""

        create_table_column_info = ""
        for col in table_detail:

            if col.is_extract or col.is_system_col:

                if col.column_type_length:
                    column_length = "(%s)" % col.column_type_length
                else:
                    column_length = ""

                column_type = "text"
                for pg_col_type_key, pg_col_type_value in settings.SWITCH_DIFF_DB_COLUMN_TYPE_ACCORDING_PG.items():
                    if col.column_type in pg_col_type_value:
                        column_type = pg_col_type_key
                        break

                column_name = str(col.column_name).lower()
                geo_column_name = None
                if column_type == "geometry":
                    if is_stag is True:
                        column_name = settings.GEO_COLUMN_NAME_STAG
                    column_type = "geometry(geometry, %s)" % settings.GEO_CRS_CODE

                create_table_column_info = create_table_column_info + column_stat % {
                    "column_name": column_name,
                    "column_type": column_type,
                    "column_length": column_length}

                # 为了记录列是否已经存在,在每次抽取前都应去查看下,是否有新的需要抽取的列但是没有被实体化的
                if col.is_entity is False and is_stag is False:
                    col.is_entity = True
                    db.session.add(col)

        if len(create_table_column_info) > 0:
            create_table_sql = create_stat % {"table_name": table_name} + create_table_column_info.strip()[:-1] + close_stat

            db.session.execute(create_table_sql)

            table_info.is_entity = True

            if is_commit:
                db.session.commit()
            else:
                db.session.flush()

            return table_name

    @classmethod
    def alter_table(cls, table_info):
        check_new_columns = models.TableDetail.get_table_detail(table_info=table_info,
                                                                is_entity=False,
                                                                is_extract=True)
        check_disable_columns = models.TableDetail.get_table_detail(table_info=table_info,
                                                                    is_entity=True,
                                                                    is_extract=False,
                                                                    is_system_col=False)
        if len(check_new_columns) > 0 or len(check_disable_columns) > 0:
            # 创建stag表
            # 迁移数据
            # 删除并重建ods表
            # 迁移数据
            # 完成表更新
            real_table_name = cls.get_real_table_name(table_name=table_info.table_name, is_stag=False)
            stag_table_name = cls.get_real_table_name(table_name=table_info.table_name, is_stag=True)
            get_exists_column_name = models.TableDetail.get_table_detail(table_info=table_info,
                                                                         is_entity=True)

            get_exists_column_name_string = ",".join([str(x.column_name).lower() for x in get_exists_column_name])
            create_stag_stmt = """create table %s as select %s from %s""" % (stag_table_name,
                                                                             get_exists_column_name_string,
                                                                             real_table_name
                                                                             )
            db.session.execute(create_stag_stmt)

            cls.drop_table(table_info=table_info, is_stag=False, is_commit=False)
            real_table_name = cls.create_table(table_info=table_info, is_stag=False, is_commit=False)

            cls.insert_table_to_table(source_table_name=stag_table_name,
                                      target_table_name=real_table_name,
                                      source_table_columns_str=get_exists_column_name_string,
                                      target_table_columns_str=get_exists_column_name_string,
                                      is_commit=False)
            cls.drop_table(table_info=table_info, is_stag=True, is_commit=True)

            return real_table_name

    @classmethod
    def truncate(cls, table_info, is_stag=False, is_commit=True):
        """
        重置表数据
        :param table_info: object TableInfo query result
        :return:
        """
        table_name = cls.get_real_table_name(table_name=table_info.table_name, is_stag=is_stag)
        logger.debug("table %s start truncate" % table_name)
        truncate_stat = """truncate table %s""" % table_name
        db.session.execute(truncate_stat)
        if is_commit:
            db.session.commit()
        else:
            db.session.flush()

    @classmethod
    def drop_table(cls, table_info, is_stag=False, is_commit=True):
        """
        删除表
        :param table_info: object TableInfo query result
        :return:
        """
        table_name = cls.get_real_table_name(table_name=table_info.table_name, is_stag=is_stag)
        logger.debug("table %s start drop" % table_name)
        drop_stat = "drop table if exists %s" % table_name
        db.session.execute(drop_stat)
        if is_stag is False:
            models.TableDetail.update_entity(table_info=table_info,
                                             is_entity=False,
                                             is_commit=is_commit)

        table_info.is_entity = False

        if is_commit:
            db.session.commit()
        else:
            db.session.flush()

    @classmethod
    def insert_value_to_table(cls, table_name, columns_in_order, values_in_order):
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
    def insert_table_to_table(cls,
                              target_table_name,
                              source_table_name,
                              target_table_columns_str,
                              source_table_columns_str,
                              is_commit=True):
        insert_stat = """insert into %(target)s(%(target_columns)s) select %(source_columns)s from %(source)s""" % (
            {"target": target_table_name,
             "source": source_table_name,
             "target_columns": target_table_columns_str,
             "source_columns": source_table_columns_str})
        execute_result = db.session.execute(insert_stat)
        if is_commit:
            db.session.commit()
        else:
            db.session.flush()
        return execute_result.rowcount

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
