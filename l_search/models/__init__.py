# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""

import enum
import hashlib
from .base import db, Column, InsertObject, TimestampMixin
from .mysql_full_text_search import FullText, FullTextSearch, FullTextMode
from sqlalchemy import or_, and_, func

from l_search.utils.logger import Logger

logger = Logger()


def convert_to_dict(sqlalchemy_data):
    """
    将sqlalchemy格式转换喂字典
    :param data: sqlalchemy all返回的数据
    :return: 转换为字典
    """
    result = []
    if not isinstance(sqlalchemy_data, list):
        sqlalchemy_data = [sqlalchemy_data]

    for row in sqlalchemy_data:
        row_dict = row.__dict__
        row_dict.pop("_sa_instance_state")
        result.append(row_dict)
    return result


class DBObjectType(enum.Enum):
    mysql = "mysql"
    postgresql = "postgresql"
    greenplum = "greenplum"
    mariadb = "mariadb"
    mssql = "mssql"


class DBConnect(db.Model, InsertObject, TimestampMixin):
    # 表的名字:数据库连接信息表
    __tablename__ = "db_connect_info"

    # 表的结构:
    id = Column(db.Integer, primary_key=True, autoincrement=True)
    domain = Column(db.String(255))
    db_type = Column(db.String(255))
    default_db = Column(db.String(255))
    db_schema = Column(db.String(255), nullable=True)
    host = Column(db.String(255))
    port = Column(db.Integer)
    account = Column(db.String(255))
    pwd = Column(db.String(255))

    def print_name(self):
        return "%s %s Database (%s) under %s" % (self.db_type,
                                                 self.host,
                                                 self.default_db,
                                                 self.domain)

    @classmethod
    def upsert(cls, input_data):

        if isinstance(input_data, dict):
            input_data = [input_data]

        for d in input_data:
            if "id" in d and d["id"] is None:
                d.pop("id")

        upsert_result = cls.upsert_base(input_data=input_data,
                                        col_not_in=[cls.id.key, cls.created_at.key],
                                        update_index=[cls.id])
        return upsert_result

    @classmethod
    def get_by_domain(cls,
                      domain=None,
                      db_type=None,
                      default_db=None,
                      db_schema=None,
                      connection_id=None,
                      is_all=True):

        get_query = cls.query

        if domain:
            get_query = get_query.filter(cls.domain == domain)

        if db_type:
            get_query = get_query.filter(cls.db_type == db_type)

        if default_db:
            get_query = get_query.filter(cls.default_db == default_db)

        if db_schema:
            get_query = get_query.filter(cls.db_schema == db_schema)

        if connection_id:
            get_query = get_query.filter(cls.id == connection_id)

        if is_all:
            return get_query.all()
        else:
            return get_query.first()


def table_info_primary_id_value(context):
    return hashlib.md5(str(
        str(context.get_current_parameters()["connection_id"]) + context.get_current_parameters()["table_name"]).encode(
        'utf-8')).hexdigest()


class TableInfo(db.Model, InsertObject, TimestampMixin):
    __tablename__ = "table_info"

    id = Column(db.String(50), primary_key=True, default=table_info_primary_id_value)
    connection_id = Column(db.Integer, db.ForeignKey("db_connect_info.id"))
    connection = db.relationship(DBConnect, backref="table_info_db_connect")
    table_name = Column(db.String(500))
    table_extract_col = Column(db.String(150), nullable=True)
    need_extract = Column(db.Boolean, default=False)
    latest_extract_date = Column(db.DateTime(), nullable=True)
    has_geo_col = Column(db.Boolean, default=False)

    __table_args__ = (
    db.Index("table_info_connection_id_table_name_index", "connection_id", "table_name", unique=True),)

    @classmethod
    def get_tables(cls,
                   connection_id=None,
                   connection_info=None,
                   table_id=None,
                   table_name=None,
                   need_extract=None,
                   has_geo_col=None):
        """
        元数据表信息提取sql生成
        :param connection_info: object DBConnect
        :param need_extract: 获取需抽取的
        :param connection_id:  DBConnect id
        :param table_name: 筛选表 单个或多个(a|b|c)
        :return:sql query
        """

        get_tables_query = cls.query

        if connection_id:
            get_tables_query = get_tables_query.filter(cls.connection_id == connection_id)
        elif connection_info:
            get_tables_query = get_tables_query.filter(cls.connection == connection_info)
            connection_id = connection_info.id

        if table_id:
            get_tables_query = get_tables_query.filter(cls.id == table_id)
        elif table_name:
            logger.debug("查询链接id(%d)下的表信息:%s" % (connection_id, table_name))

            if "|" in table_name:
                table_name_list = table_name.split("|")
            else:
                table_name_list = [table_name]

            table_name_list = [str(x).lower() for x in table_name_list]

            get_tables_query = get_tables_query.filter(func.lower(cls.table_name).in_(table_name_list))

        else:
            if connection_id:
                logger.debug("查询链接id(%d)下的所有表" % (connection_id))
            else:
                logger.debug("查询所记录的所有表")

        if need_extract:
            get_tables_query = get_tables_query.filter(cls.need_extract == need_extract)

        if has_geo_col:
            get_tables_query = get_tables_query.filter(cls.has_geo_col == has_geo_col)

        return get_tables_query.all()

    @classmethod
    def upsert(cls,
               input_data
               ):
        """

        :param input_data:
        [{connection_id:1,
        table_name:xxx,
        table_extract_col:xxx
        }]
        :return:
        """
        if isinstance(input_data, dict):
            input_data = [input_data]

        for d in input_data:
            if "id" in d and d["id"] is None:
                d.pop("id")

        execute_result = cls.upsert_base(input_data=input_data,
                                         col_not_in=[cls.id.key, cls.created_at.key],
                                         update_index=[cls.connection_id, cls.table_name])
        return execute_result


def table_detail_primary_id_value(context):
    return hashlib.md5(str(
        str(context.get_current_parameters()["table_info_id"]) + context.get_current_parameters()["column_name"]).encode(
        'utf-8')).hexdigest()


class TableDetail(db.Model, InsertObject, TimestampMixin):
    # 表的名字:元数据信息表
    __tablename__ = "table_detail"
    id = Column(db.String(50), primary_key=True, default=table_detail_primary_id_value)
    table_info_id = Column(db.String(50), db.ForeignKey("table_info.id"))
    table_info = db.relationship(TableInfo, backref="table_detail_table_info")
    column_name = Column(db.String(255))
    column_type = Column(db.String(255))
    column_type_length = Column(db.String(255))
    column_comment = Column(db.String(255), nullable=True)
    column_position = Column(db.Integer)
    is_extract = Column(db.Boolean, default=True)
    is_primary = Column(db.Boolean, default=False)
    is_entity = Column(db.Boolean, default=False)

    __table_args__ = (
        db.Index("table_detail_table_info_id_column_name_index", "table_info_id", "column_name", unique=True),)

    @classmethod
    def get_table_detail(cls,
                         table_info_id=None,
                         table_info=None,
                         column_name=None,
                         is_extract=None,
                         table_primary=None,
                         is_entity=None
                         ):
        meta_query = cls.query

        if table_info_id:
            meta_query = meta_query.filter(cls.table_info_id == table_info_id)

        if table_info:
            meta_query = meta_query.filter(cls.table_info == table_info)

        if column_name:
            meta_query = meta_query.filter(cls.column_name == column_name)

        if is_extract:
            meta_query = meta_query.filter(cls.is_extract == is_extract).order_by(cls.column_position)

        if table_primary:
            meta_query = meta_query.filter(cls.is_primary == table_primary).order_by(cls.is_primary)

        if is_entity:
            meta_query = meta_query.filter(cls.is_entity == is_entity)

        return meta_query.all()

    @classmethod
    def upsert(cls, input_data):

        if isinstance(input_data, dict):
            input_data = [input_data]

        for d in input_data:
            if "id" in d and d["id"] is None:
                d.pop("id")

        execute_result = cls.upsert_base(input_data=input_data,
                                         col_not_in=[cls.id.key, cls.created_at.key],
                                         update_index=[cls.table_info_id, cls.column_name])
        return execute_result


class FullTextIndex(db.Model, InsertObject, FullText):
    __tablename__ = "full_text_index"
    __fulltext_columns__ = ("row_content",)

    id = Column(db.String(300), primary_key=True)
    extract_data_info_id = Column(db.Integer)
    block_name = Column(db.String(500))
    block_key = Column(db.String(500))
    row_content = Column(db.Text())

    @classmethod
    def search_index(cls,
                     domain,
                     search_text,
                     page=None,
                     page_size=20,
                     db_object_type=None,
                     db_name=None,
                     block_name=None,
                     block_key=None):

        search_query = cls.query.filter(
            and_(
                cls.extract_data_info.has(TableInfo.domain == domain),
                FullTextSearch("*%s*" % search_text, cls, FullTextMode.BOOLEAN))
        )

        if db_object_type and db_object_type != "":
            search_query = search_query.filter(
                cls.extract_data_info.has(TableInfo.db_object_type == db_object_type))

        if db_name and db_name != "":
            search_query = search_query.filter(
                cls.extract_data_info.has(TableInfo.db_name == db_name))

        if block_name:
            search_query = search_query.filter(cls.block_name == block_name)

        if block_key:
            search_query = search_query.filter(cls.block_key == block_key)

        if page:
            get_search_query = search_query.paginate(page=page, per_page=page_size).items
        else:
            get_search_query = search_query.all()

        return get_search_query

    @classmethod
    def search_index_group(cls,
                           domain,
                           search_text,
                           page=None,
                           page_size=20,
                           db_object_type=None,
                           db_name=None, ):
        search_group_query = db.session.query(cls.block_key,
                                              cls.block_name,
                                              func.count().label("hits_num")).filter(
            and_(cls.extract_data_info.has(TableInfo.domain == domain),
                 FullTextSearch("*%s*" % search_text, cls, FullTextMode.BOOLEAN)))

        if db_object_type and db_object_type != "":
            search_group_query = search_group_query.filter(
                cls.extract_data_info.has(TableInfo.db_object_type == db_object_type))

        if db_name and db_name != "":
            search_group_query = search_group_query.filter(
                cls.extract_data_info.has(TableInfo.db_name == db_name))

        search_group_query = search_group_query.group_by(cls.block_name, cls.block_key)

        if page:
            get_search_group_query = search_group_query.paginate(page=page, per_page=page_size).items
        else:
            get_search_group_query = search_group_query.all()

        return get_search_group_query

    @classmethod
    def delete_data(cls, extract_data_info_id=None, id_list=None):

        if extract_data_info_id:
            cls.query.filter(cls.extract_data_info_id == extract_data_info_id).delete()
            db.session.commit()
        elif id_list:
            cls.query.filter(cls.id.in_(id_list)).delete()
            db.session.commit()
