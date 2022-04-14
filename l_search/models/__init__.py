# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:simonzhang
@file:__init__.py
"""
import enum
import hashlib
from croniter import croniter
from .base import db, Column, InsertObject, TimestampMixin
from sqlalchemy import or_, and_, func
from l_search import settings
from l_search.utils import get_now, datetime
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

        if domain is not None:
            get_query = get_query.filter(cls.domain == domain)

        if db_type is not None:
            get_query = get_query.filter(cls.db_type == db_type)

        if default_db is not None:
            get_query = get_query.filter(cls.default_db == default_db)

        if db_schema is not None:
            get_query = get_query.filter(cls.db_schema == db_schema)

        if connection_id is not None:
            get_query = get_query.filter(cls.id == connection_id)

        if is_all:
            return get_query.all()
        else:
            return get_query.first()


def table_info_primary_id_value(context):
    return hashlib.md5(str(
        str(context.get_current_parameters()["connection_id"]) + context.get_current_parameters()["table_name"]).encode(
        "utf-8")).hexdigest()


def table_info_table_name_alias(context):
    return context.get_current_parameters()["table_name"]


class TableInfo(db.Model, InsertObject, TimestampMixin):
    __tablename__ = "table_info"

    id = Column(db.String(50), primary_key=True, default=table_info_primary_id_value)
    connection_id = Column(db.Integer, db.ForeignKey("db_connect_info.id"))
    connection = db.relationship(DBConnect, backref="table_info_db_connect")
    table_name = Column(db.String(500))
    table_name_alias = Column(db.String(500), default=table_info_table_name_alias)
    is_entity = Column(db.Boolean, default=False)
    table_extract_col = Column(db.String(150), nullable=True)
    latest_extract_date = Column(db.DateTime(), nullable=True)
    has_been_dropped = Column(db.Boolean, default=False)
    dropped_time = Column(db.DateTime(), nullable=True)
    crontab_str = Column(db.String(50), nullable=True)
    crontab_last_ts = Column(db.DateTime(), nullable=True)

    __table_args__ = (
        db.Index("table_info_connection_id_table_name_index", "connection_id", "table_name", unique=True),)

    def entity_table_name(self):
        return "c%d__%s" % (self.connection_id, str(self.table_name_alias).lower())

    @classmethod
    def get_tables(cls,
                   connection_id=None,
                   connection_info=None,
                   table_id=None,
                   table_name=None,
                   table_name_alias=None,
                   is_entity=None,
                   source_table_exists=True,
                   need_crontab=False):
        """
        元数据表信息提取sql生成
        :param connection_info: object DBConnect
        :param is_entity: 已经实体化
        :param connection_id:  DBConnect id
        :param table_name: 筛选表 单个或多个(a|b|c)
        :param source_table_exists: 目标表需存在
        :return:sql query
        """

        def explain_table_name(import_table_name):
            if isinstance(import_table_name, str):
                if "|" in import_table_name:
                    table_name_list = import_table_name.split("|")
                else:
                    table_name_list = [import_table_name]
            else:
                table_name_list = import_table_name

            table_name_list = [str(x).lower() for x in table_name_list]
            return table_name_list

        get_tables_query = cls.query

        if connection_id:
            if isinstance(connection_id, str):
                connection_id = [connection_id]

            get_tables_query = get_tables_query.filter(cls.connection_id.in_(connection_id))
        elif connection_info:
            get_tables_query = get_tables_query.filter(cls.connection == connection_info)
            connection_id = connection_info.id

        if table_id is not None:
            if isinstance(table_id, str):
                table_id = [table_id]
            get_tables_query = get_tables_query.filter(cls.id.in_(table_id))
        elif table_name is not None:
            table_name_list = explain_table_name(import_table_name=table_name)
            get_tables_query = get_tables_query.filter(func.lower(cls.table_name).in_(table_name_list))
        elif table_name_alias is not None:
            table_name_list = explain_table_name(import_table_name=table_name_alias)
            get_tables_query = get_tables_query.filter(func.lower(cls.table_name_alias).in_(table_name_list))
        else:
            if connection_id is not None:
                logger.debug("查询链接id(%s)下的所有表" % (str(connection_id)))
            else:
                logger.debug("查询所记录的所有表")

        if is_entity is not None:
            get_tables_query = get_tables_query.filter(cls.is_entity == is_entity)

        if source_table_exists:
            get_tables_query = get_tables_query.filter(cls.has_been_dropped == False)

        if need_crontab:
            get_tables_query = get_tables_query.filter(cls.crontab_str != None)

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

            if "crontab_str" in d and d["crontab_str"] is not None:
                if croniter.is_valid(d["crontab_str"]) is False:
                    d.pop("crontab_str")
                else:
                    crontab_iter = croniter(d["crontab_str"], get_now())
                    d["crontab_last_ts"] = crontab_iter.get_next(datetime.datetime)

        execute_result = cls.upsert_base(input_data=input_data,
                                         col_not_in=[cls.id.key, cls.created_at.key],
                                         update_index=[cls.connection_id, cls.table_name])
        return execute_result

    @classmethod
    def delete_table_info(cls, table_info):
        table_info.has_been_dropped = True
        table_info.dropped_time = get_now()
        db.session.add(table_info)

        TableDetail.query.filter(TableDetail.table_info == table_info).update({TableDetail.is_extract: False})

        db.session.commit()


def table_detail_primary_id_value(context):
    return hashlib.md5(str(
        str(context.get_current_parameters()["table_info_id"]) + context.get_current_parameters()[
            "column_name"]).encode(
        "utf-8")).hexdigest()


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
    is_system_col = Column(db.Boolean, default=False)

    __table_args__ = (
        db.Index("table_detail_table_info_id_column_name_index", "table_info_id", "column_name", unique=True),)

    @classmethod
    def get_table_detail(cls,
                         table_info_id=None,
                         table_info=None,
                         column_name=None,
                         is_extract=None,
                         table_primary=None,
                         is_entity=None,
                         is_system_col=None,
                         select_geo=None
                         ):
        meta_query = cls.query

        if table_info_id is not None:
            meta_query = meta_query.filter(cls.table_info_id == table_info_id)

        if table_info is not None:
            meta_query = meta_query.filter(cls.table_info == table_info)

        if column_name is not None:
            meta_query = meta_query.filter(cls.column_name == column_name)

        if is_extract is not None:
            if is_system_col is True:
                meta_query = meta_query.filter(or_(cls.is_extract == is_extract,
                                                   cls.is_system_col == True))
            elif is_system_col is False:
                meta_query = meta_query.filter(and_(cls.is_extract == is_extract,
                                                    cls.is_system_col == False))
            else:
                meta_query = meta_query.filter(cls.is_extract == is_extract)

        if table_primary is not None:
            meta_query = meta_query.filter(cls.is_primary == table_primary)

        if is_entity is not None:
            meta_query = meta_query.filter(cls.is_entity == is_entity)

        if select_geo is True:
            meta_query = meta_query.filter(cls.column_type == "geometry")

        return meta_query.order_by(cls.column_position).all()

    @classmethod
    def upsert(cls, input_data):

        if isinstance(input_data, dict):
            input_data = [input_data]

        for d in input_data:
            if "id" in d and d["id"] is None:
                d.pop("id")

        period_column = {"table_info_id": input_data[0]["table_info_id"],
                         "column_name": settings.PERIOD_COLUMN_NAME,
                         "column_type": "tsrange",
                         "column_type_length": "",
                         "column_comment": None,
                         "is_extract": False,
                         "is_primary": False,
                         "is_system_col": True}
        check_period_column_exist = cls.get_table_detail(table_info_id=period_column["table_info_id"],
                                                         column_name=period_column["column_name"])

        if len(check_period_column_exist) == 0:
            # 添加时态列, 一般是表第一次创建的时候 需要添加这个列
            max_index = len(input_data) + 1
            period_column["column_position"] = max_index
            input_data.append(period_column)

        execute_result = cls.upsert_base(input_data=input_data,
                                         col_not_in=[cls.id.key, cls.created_at.key],
                                         update_index=[cls.table_info_id, cls.column_name],
                                         is_commit=True)

        return execute_result

    @classmethod
    def update_entity(cls,
                      is_entity,
                      table_info=None,
                      table_info_id=None,
                      is_extract=True,
                      is_system_col=None,
                      is_commit=True):

        update_query = cls.query

        if table_info_id is not None:
            update_query = update_query.filter(cls.table_info_id == table_info_id)

        if table_info:
            update_query = update_query.filter(cls.table_info == table_info)

        if is_extract is not None:
            update_query = update_query.filter(cls.is_extract == is_extract)

            if is_system_col is not None:
                update_query = update_query.filter(cls.is_system_col == is_system_col)

        update_query.update({cls.is_entity: is_entity})
        if is_commit:
            db.session.commit()
        else:
            db.session.flush()

    @classmethod
    def get_columns(cls,
                    table_info_id=None,
                    table_info=None,
                    is_extract=None,
                    is_entity=None,
                    return_type="str"):
        query_result = cls.get_table_detail(table_info_id=table_info_id,
                                            table_info=table_info,
                                            is_extract=is_extract,
                                            is_entity=is_entity)
        column_name_list = [str(x.column_name).lower() for x in query_result]

        if return_type == "str":
            return ",".join(column_name_list)
        else:
            return column_name_list
