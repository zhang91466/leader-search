# -*- coding: UTF-8 -*-
"""
@time:2021/11/24
@author:zhangwei
@file:__init__.py
"""
from .base import db, Column, InsertObject, TimestampMixin
import enum
from .mysql_full_text_search import FullText, FullTextSearch, FullTextMode
from sqlalchemy import or_, and_, func
from sqlalchemy.dialects.postgresql import insert
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

    @classmethod
    def add_row(cls, infos):
        """
        新增连接字符串
        :param infos: {"domain":"", "type":"", "host":"", "port":"", "account":"", "pwd":"", "default_db":""}
        """
        basic_key = ["domain", "db_type", "host", "port", "account", "pwd", "default_db", "db_schema"]
        create_result = None
        failed_info = ""

        if infos and isinstance(infos, dict):
            if len(basic_key) == len(infos.keys()):
                is_key_complete = True
                for bkey in basic_key:
                    if bkey not in infos.keys():
                        failed_info = "数据库连接信息表无法插入新增数据,因key与预设不同"
                        logger.warn(failed_info)
                        is_key_complete = False
                        break

                connection_info = cls.get_by_domain(domain=infos["domain"],
                                                    db_type=infos["db_type"],
                                                    default_db=infos["default_db"],
                                                    db_schema=infos["db_schema"])

                if is_key_complete and len(connection_info) == 0:
                    create_result = cls.create(**infos)

            else:
                failed_info = "数据库连接信息表无法插入新增数据,因缺失key"
                logger.warn(failed_info)

        if create_result is None:
            return None, "failed %s" % failed_info
        else:
            return create_result.id, ""
    @classmethod
    def upsert(cls, input_data):
        insert_stmt = insert(cls.__table__).values(input_data)

        set_dict = {}

        for c in insert_stmt.excluded:
            set_dict[c.name] = c

        set_dict.pop("updated_at")
        set_dict.pop("created_at")
        set_dict.pop("id")

        # pg 特定写法
        upsert_stmt = insert_stmt.on_conflict_do_update(index_elements=["id"],
                                                        set_=set_dict)

        execute_result = db.session.execute(upsert_stmt)
        db.session.commit()

        return execute_result

    @classmethod
    def modify(cls,
               connection_id,
               host=None,
               port=None,
               account=None,
               pwd=None,
               default_db=None,
               db_schema=None):

        update_column = {}

        exists_data = cls.get_by_domain(connection_id=connection_id, is_all=False)

        if host and host != exists_data.host:
            update_column["host"] = host

        if port and port != exists_data.port:
            update_column["port"] = port

        if account and account != exists_data.account:
            update_column["account"] = account

        if pwd and pwd != exists_data.pwd:
            update_column["pwd"] = pwd

        if default_db and default_db != exists_data.default_db:
            update_column["default_db"] = default_db

        if db_schema and db_schema != exists_data.db_schema:
            update_column["db_schema"] = db_schema

        db.session.query(cls).filter(cls.id == connection_id).update(update_column)
        db.session.commit()

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


class TableInfo(db.Model, InsertObject, TimestampMixin):
    __tablename__ = "table_info"

    id = Column(db.Integer, primary_key=True, autoincrement=True)
    connection_id = Column(db.Integer, db.ForeignKey("db_connect_info.id"))
    connection = db.relationship(DBConnect, backref="table_info_db_connect")
    table_name = Column(db.String(500))
    table_primary_col = Column(db.String(150), nullable=True)
    table_primary_col_is_int = Column(db.Boolean, default=True)
    table_extract_col = Column(db.String(150), nullable=True)
    need_extract = Column(db.Boolean, default=False)
    latest_table_primary_id = Column(db.String(150), nullable=True)
    latest_extract_date = Column(db.DateTime(), nullable=True)

    @classmethod
    def get_tables(cls,
                   connection_id=None,
                   connection_info=None,
                   table_name=None,
                   need_extract=None):
        """
        元数据表信息提取sql生成
        :param connection_info: object DBConnect
        :param need_extract: 获取需抽取的
        :param connection_id:  DBConnect id
        :param table_name: 筛选表 单个或多个(a|b|c)
        :return:sql query
        """

        meta_query = db.session.query(cls.connection_info.domain,
                                      cls.connection_info.db_type,
                                      cls.connection_info.default_db,
                                      cls.table_name
                                      )
        if connection_id:
            meta_query = meta_query.filter(cls.connection_id == connection_id)
        elif connection_info:
            meta_query = meta_query.filter(cls.connection == connection_info)

        if table_name:
            logger.debug("查询链接id(%d)下的表信息:%s" % (connection_id, table_name))

            if "|" in table_name:
                table_name_list = table_name.split("|")
            else:
                table_name_list = [table_name]

            table_name_list = [str(x).lower() for x in table_name_list]

            meta_query = meta_query.filter(func.lower(cls.table_name).in_(table_name_list))

        else:
            logger.debug("查询链接id(%d)下的所有表" % (connection_id))

        if need_extract:
            meta_query = meta_query.filter(cls.need_extract == need_extract)

        meta_query = meta_query.group_by(cls.connection,
                                         cls.table_name)
        return meta_query.all()

    @classmethod
    def upsert(cls,
               input_data
               ):
        """

        :param input_data:
        [{connection_id:1,
        table_name:xxx,
        table_primary_col:xxx,
        table_primary_col_is_int:xxx,
        table_extract_col:xxx
        }]
        :return:
        """

        insert_stmt = insert(cls.__table__).values(input_data)
        update_dict = {x.name: x for x in insert_stmt.inserted}
        # pg 特定写法
        upsert_stmt = insert_stmt.on_conflict_do_update(index_elements=["connection_id", "table_name"],
                                                        set_=update_dict)

        execute_result = db.session.execute(upsert_stmt)
        db.session.commit()
        return execute_result

    @classmethod
    def delete_table(cls, connection_info, table_name):
        cls.query.filter(and_(cls.connection == connection_info,
                              func.lower(cls.table_name) == str(table_name).lower()
                              )).delete(synchronize_session="fetch")

        db.session.commit()


class TableDetail(db.Model, InsertObject, TimestampMixin):
    # 表的名字:元数据信息表
    __tablename__ = "table_detail"
    id = Column(db.Integer, primary_key=True, autoincrement=True)
    table_info_id = Column(db.Integer, db.ForeignKey("table_info.id"))
    table_info = db.relationship(TableInfo, backref="table_detail_table_info")
    column_name = Column(db.String(255))
    column_type = Column(db.String(255))
    column_type_length = Column(db.String(255))
    column_comment = Column(db.String(255), nullable=True)
    column_position = Column(db.Integer)
    is_extract = Column(db.Boolean, default=True)
    is_primary = Column(db.Boolean, default=False)

    @classmethod
    def get_table_info(cls,
                       table_info_id=None,
                       table_info=None,
                       column_name=None,
                       is_extract=None,
                       table_primary=None
                       ):
        if table_info_id:
            meta_query = cls.query.filter(cls.table_info_id == table_info_id)

        if table_info:
            meta_query = cls.query.filter(cls.table_info == table_info)

        if column_name:
            meta_query = meta_query.filter(cls.column_name == column_name)

        if is_extract:
            meta_query = meta_query.filter(cls.is_extract == is_extract).order_by(cls.column_position)

        if table_primary:
            meta_query = meta_query.filter(cls.is_primary == table_primary).order_by(cls.is_primary)

        return meta_query.all()

    @classmethod
    def update_filter(cls, connection_info, table_name, column_name, filter_value):
        db.session.query(cls).filter(and_(cls.connection == connection_info,
                                          cls.table_info == table_name,
                                          cls.column_name == column_name,
                                          cls.is_extract_filter == True
                                          )).update({"filter_default": filter_value})
        db.session.commit()

    @classmethod
    def modify(cls, column_id, input_data):

        update_column = {}

        try:
            if "column_comment" in input_data:
                update_column["column_comment"] = input_data["column_comment"]

            if "is_extract" in input_data and isinstance(input_data["is_extract"], bool):
                update_column["is_extract"] = input_data["is_extract"]

            if "is_primary" in input_data and isinstance(input_data["is_primary"], bool):
                update_column["is_primary"] = input_data["is_primary"]

            db.session.query(cls).filter(cls.id == column_id).update(update_column)
            db.session.commit()
        except Exception as e:
            return e


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
