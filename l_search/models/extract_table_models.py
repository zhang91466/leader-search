# -*- coding: UTF-8 -*-
"""
@time:12/16/2021
@author:
@file:extract_table_models
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from werkzeug.exceptions import BadRequest
from sql_metadata import Parser

from l_search import models
from l_search import settings
from l_search.models.base import db
from l_search.utils.logger import Logger

DONOT_CREATE_COLUMN = []

logger = Logger()


class DBSession:
    def __init__(self, connection_info):
        self.connect_info = connection_info

        if self.connect_info.db_type in settings.SOURCE_DB_CONNECTION_URL:
            get_part_of_connect_string = settings.SOURCE_DB_CONNECTION_URL[self.connect_info.db_type]
        else:
            raise "%s not support" % self.connect_info.print_name()

        engine_connect_string = '%s://%s:%s@%s:%s/%s%s' % (get_part_of_connect_string["connect_prefix"],
                                                           self.connect_info.account,
                                                           self.connect_info.pwd,
                                                           self.connect_info.host,
                                                           self.connect_info.port,
                                                           self.connect_info.default_db,
                                                           get_part_of_connect_string["remark"])

        self.engine = create_engine(engine_connect_string)
        self.session = Session(self.engine, future=True)


class TableOperate:

    @staticmethod
    def db_commit(is_commit=True):
        if is_commit:
            db.session.commit()
        else:
            db.session.flush()

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

        table_name = cls.get_real_table_name(table_name=table_info.entity_table_name(), is_stag=is_stag)
        logger.info("table %s start create" % table_name)

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
            create_table_sql = create_stat % {"table_name": table_name} + create_table_column_info.strip()[
                                                                          :-1] + close_stat
            logger.info("table %s start sql: %s" % (table_name, create_table_sql))
            db.session.execute(create_table_sql)

            table_info.is_entity = True

            if is_commit:
                db.session.commit()
            else:
                db.session.flush()

            return table_name

    @classmethod
    def alter_table(cls, table_info):
        logger.info("table %s start alter" % table_info.entity_table_name())

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
            real_table_name = cls.get_real_table_name(table_name=table_info.entity_table_name(), is_stag=False)
            stag_table_name = cls.get_real_table_name(table_name=table_info.entity_table_name(), is_stag=True)
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
        table_name = cls.get_real_table_name(table_name=table_info.entity_table_name(), is_stag=is_stag)
        logger.info("table %s start truncate" % table_name)
        truncate_stat = """truncate table %s""" % table_name
        logger.info("table %s truncate sql: %s" % (table_name, truncate_stat))
        db.session.execute(truncate_stat)

        cls.db_commit(is_commit=is_commit)

    @classmethod
    def drop_table(cls, table_info, is_stag=False, is_commit=True):
        """
        删除表
        :param table_info: object TableInfo query result
        :return:
        """
        table_name = cls.get_real_table_name(table_name=table_info.entity_table_name(), is_stag=is_stag)
        logger.info("table %s start drop" % table_name)
        drop_stat = "drop table if exists %s" % table_name
        logger.info("table %s drop sql: %s" % (table_name, drop_stat))
        db.session.execute(drop_stat)
        if is_stag is False:
            models.TableDetail.update_entity(table_info=table_info,
                                             is_entity=False,
                                             is_commit=is_commit)

        table_info.is_entity = False

        cls.db_commit(is_commit=is_commit)

    @classmethod
    def insert_value_to_table(cls, table_name, columns_in_order, values_in_order, is_commit=True):
        logger.info("Table %s start insert with value" % table_name)
        # """INSERT INTO full_text_index (id, extract_data_info_id, block_name, block_key, row_content) VALUES (:id, :extract_data_info_id, :block_name, :block_key, :row_content)"""

        insert_stat = """insert into %(table_name)s (%(columns)s) values (%(values)s)""" % {
            "table_name": table_name,
            "columns": "`" + "`,`".join(columns_in_order) + "`",
            "values": ":" + ", :".join(columns_in_order)
        }
        execute_result = db.session.execute(insert_stat, values_in_order)

        cls.db_commit(is_commit=is_commit)

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
        logger.info("table %s start insert" % target_table_name)
        insert_stat = """insert into %(target)s(%(target_columns)s) select %(source_columns)s from %(source)s""" % (
            {"target": target_table_name,
             "source": source_table_name,
             "target_columns": target_table_columns_str,
             "source_columns": source_table_columns_str})
        execute_result = db.session.execute(insert_stat)

        logger.info("table %s insert sql: %s" % (target_table_name, insert_stat))

        cls.db_commit(is_commit=is_commit)
        return execute_result.rowcount

    @classmethod
    def _primary_column_combine_for_sql(cls, table_info):
        primary_row = models.TableDetail.get_table_detail(table_info=table_info,
                                                          table_primary=True)

        if len(primary_row) == 1:
            return str(primary_row[0].column_name).lower()
        else:
            primary_name_list = []
            for r in primary_row:
                sql_cast_str = "%s::varchar" % str(r.column_name).lower()
                primary_name_list.append(sql_cast_str)

            return "concat(%s)" % "%(alias)s" + ",%(alias)s".join(primary_name_list)

    @classmethod
    def update_tsrange(cls, table_info, upper_datetime, is_commit=True):
        table_name = table_info.entity_table_name()
        primary_col_name = cls._primary_column_combine_for_sql(table_info=table_info)

        logger.info("table %s start update tsrange" % table_name)
        update_stmt = """
        UPDATE %(ods_table_name)s m set %(period)s = tsrange(lower(m.%(period)s)::timestamp, '%(upper_datetime)s'::timestamp, '[]') 
        from %(stag_table_name)s stag
        where %(m_primary_col_name)s = %(stag_primary_col_name)s""" % {
            "ods_table_name": cls.get_real_table_name(table_name=table_name, is_stag=False),
            "stag_table_name": cls.get_real_table_name(table_name=table_name, is_stag=True),
            "period": settings.PERIOD_COLUMN_NAME,
            "upper_datetime": upper_datetime,
            "m_primary_col_name": primary_col_name % {"alias": "m."},
            "stag_primary_col_name": primary_col_name % {"alias": "stag."}
        }

        logger.info("table %s update tsrange sql %s" % (table_name, update_stmt))
        db.session.execute(update_stmt)

        cls.db_commit(is_commit=is_commit)

    @classmethod
    def update_tsrange_for_delete_data(cls, table_info, upper_datetime, is_commit=True):
        table_name = table_info.entity_table_name()
        primary_col_name = cls._primary_column_combine_for_sql(table_info=table_info)

        logger.info("table %s start update tsrange for delete data " % table_name)
        update_stmt = """
            UPDATE %(ods_table_name)s set %(period)s = tsrange(lower(%(period)s)::timestamp, '%(upper_datetime)s'::timestamp, '[]') 
            where %(primary_col_name)s in (
                select %(m_primary_col_name)s 
                from %(ods_table_name)s m
                left join %(stag_table_name)s stag on %(m_primary_col_name)s = %(stag_primary_col_name)s
                where %(stag_primary_col_name)s is null
            )""" % {
            "ods_table_name": cls.get_real_table_name(table_name=table_name, is_stag=False),
            "stag_table_name": cls.get_real_table_name(table_name=table_name, is_stag=True),
            "period": settings.PERIOD_COLUMN_NAME,
            "upper_datetime": upper_datetime,
            "primary_col_name": primary_col_name % {"alias": ""},
            "m_primary_col_name": primary_col_name % {"alias": "m."},
            "stag_primary_col_name": primary_col_name % {"alias": "stag."}
        }

        logger.info("table %s update tsrange for delete data sql %s" % (table_name, update_stmt))
        execute_data = db.session.execute(update_stmt)

        cls.db_commit(is_commit=is_commit)
        return execute_data

    @classmethod
    def delete(cls, table_info, where_stat=None, is_stag=False, is_commit=True):
        table_name = cls.get_real_table_name(table_name=table_info.entity_table_name(), is_stag=is_stag),
        logger.info("table %s start delete" % table_name)

        if where_stat:
            where_stat = " where " + where_stat
        else:
            where_stat = ""

        delete_stat = "delete from %(table_name)s %(where_stat)s" % {"table_name": table_name,
                                                                     "where_stat": where_stat}
        logger.info("table %s delete sql %s" % (table_name, delete_stat))
        db.session.execute(delete_stat)
        cls.db_commit(is_commit=is_commit)

    @classmethod
    def select(cls, sql, connection_id=None):

        logger.info("Start select by sql")

        sql = str(sql).lower()
        black_key_words = ["insert", "update", "delete", "drop", "alter", "create", "truncate"]

        # 避免sql注入导致数据的变更
        for black_key in black_key_words:
            sql_split = str(sql).split(black_key)
            if len(sql_split) > 1:
                error_message = "Sql contains invalid characters: %s" % black_key
                logger.warn(error_message)
                raise BadRequest(error_message)

        # 去除回车用空格替换
        sql_list = [str(l).strip() for l in str(sql).splitlines()]
        sql = " ".join(sql_list)

        table_name_list = Parser(sql).tables

        get_tables = models.TableInfo.get_tables(table_name_alias=table_name_list)

        if connection_id is not None and len(connection_id) > 0:
            get_tables = [x for x in get_tables if x.connection_id in connection_id]

        if len(table_name_list) == len(get_tables):
            for i, table_info in enumerate(get_tables):

                table_name = str(table_info.table_name_alias).lower()
                table_alias = ""
                # 为了避免用表命直接做别名，故需要在此处判断是否有表命后带点的情况 （select aa.b,aa.c from aa）
                table_name_behind_with_point = "%s." % table_name
                if table_name_behind_with_point in sql:
                    table_alias = "T%d" % i
                    sql = sql.replace(table_name_behind_with_point, "%s." % table_alias)

                sql = sql.replace(table_name, "ods.%s %s" % (str(table_info.entity_table_name()).lower(), table_alias))

        else:
            table_meta = [x.table_name for x in get_tables]
            error_message = "In the sql table(%s) of Connection %s are not found in metadata" % (
                ",".join(list(set(table_name_list) - set(table_meta))),
                str(connection_id))
            logger.warn(error_message)
            raise BadRequest(error_message)

        try:
            logger.info("Execute select by sql: %s" % sql)
            execute_data = db.session.execute(sql)
            return [dict(row) for row in execute_data]
        except Exception as e:
            raise BadRequest("""Sql execute error. Execute sql:
            %s
            Error info:
            %s
            """ % (sql, e))

    @classmethod
    def get_max_update_ts(cls, table_info, update_ts_col, is_stag=True):

        table_name = cls.get_real_table_name(table_name=table_info.entity_table_name(), is_stag=is_stag)

        max_stmt = "select max(%(update_ts_col)s) as max_update_ts from %(table_name)s" % {
            "update_ts_col": update_ts_col,
            "table_name": table_name}

        logger.info("Get %s max update ts sql %s" % (table_name, max_stmt))
        execute_data = db.session.execute(max_stmt).first()

        return execute_data.max_update_ts

    @classmethod
    def row_count(cls, table_info):
        table_name = cls.get_real_table_name(table_name=table_info.entity_table_name(), is_stag=False)

        count_stmt = "select count(*) as row_cnt from %s" % table_name

        logger.info("Get %s row count sql %s" % (table_name, count_stmt))
        execute_data = db.session.execute(count_stmt).first()
        return execute_data.row_cnt
