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
from l_search.utils.logger import Logger

logger = Logger()


def convert_to_dict(sqlalchemy_data):
    """
    将sqlalchemy格式转换喂字典
    :param data: sqlalchemy all返回的数据
    :return: 转换为字典
    """
    result = []
    for row in sqlalchemy_data:
        row_dict = row.__dict__
        row_dict.pop("_sa_instance_state")
        result.append(row_dict)
    return result


class DBConnect(db.Model, InsertObject, TimestampMixin):
    # 表的名字:数据库连接信息表
    __tablename__ = "db_connect_info"

    # 表的结构:
    id = Column(db.Integer, primary_key=True, autoincrement=True)
    domain = Column(db.String(255))
    type = Column(db.String(255))
    default_db = Column(db.String(255))
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
        basic_key = ["domain", "type", "host", "port", "account", "pwd", "default_db"]
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
                                                    type=infos["type"])

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
    def modify(cls, new_data, connection_id):

        update_column = {}

        exists_data = cls.get_by_domain(connection_id=connection_id, is_all=False)

        if "host" in new_data and new_data["host"] != exists_data.host:
            update_column["host"] = new_data["host"]

        if "port" in new_data and new_data["port"] != exists_data.port:
            update_column["port"] = new_data["port"]

        if "account" in new_data and new_data["account"] != exists_data.account:
            update_column["account"] = new_data["account"]

        if "pwd" in new_data and new_data["pwd"] != exists_data.pwd:
            update_column["pwd"] = new_data["pwd"]

        if "default_db" in new_data and new_data["default_db"] != exists_data.default_db:
            update_column["default_db"] = new_data["default_db"]

        db.session.query(cls).filter(cls.id == connection_id).update(update_column)
        db.session.commit()

    @classmethod
    def get_by_domain(cls, domain=None, type=None, connection_id=None, is_all=True):

        get_query = cls.query

        if domain:
            get_query = get_query.filter(cls.domain == domain)

        if type:
            get_query = get_query.filter(cls.type == type)

        if connection_id:
            get_query = get_query.filter(cls.id == connection_id)

        if is_all:
            return get_query.all()
        else:
            return get_query.first()


class DBMetadata(db.Model, InsertObject, TimestampMixin):
    # 表的名字:元数据信息表
    __tablename__ = "db_metadata_info"
    id = Column(db.Integer, primary_key=True, autoincrement=True)
    domain = Column(db.String(255))
    type = Column(db.String(255))
    default_db = Column(db.String(255))
    table_name = Column(db.String(255))
    column_name = Column(db.String(255))
    column_type = Column(db.String(255))
    column_type_length = Column(db.String(255))
    column_comment = Column(db.String(255), nullable=True)
    column_position = Column(db.Integer)
    is_extract = Column(db.Integer, default=1)
    is_primary = Column(db.Integer, default=0)
    is_extract_filter = Column(db.Integer, default=0)
    filter_default = Column(db.String(255), nullable=True)

    @classmethod
    def get_tables(cls, domain, type, **kwargs):
        """
        元数据表信息提取sql生成
        :param domain:
        :param type:
        :param db: 筛选db
        :param table: 筛选表 单个或多个(a|b|c)
        :return:sql query
        """

        meta_query = db.session.query(cls.domain,
                                      cls.type,
                                      cls.default_db,
                                      cls.table_name
                                      ).filter(and_(cls.domain == domain,
                                                    cls.type == type
                                                    ))

        if "default_db" in kwargs and "table_name_list" in kwargs and kwargs["db_name"] is not None and kwargs[
            "table_name_list"] is not None:
            logger.info("按数据库的特定表进行元数据提取 业务域:%s 物理库:%s db:%s table:%s" %
                        (domain, type, kwargs["db_name"], kwargs["table_name"]))

            meta_query = meta_query.filter(and_(cls.default_db == kwargs["default_db"],
                                                cls.table_name.in_(kwargs["table_name_list"])
                                                ))
        elif "default_db" in kwargs and "table_name" in kwargs and kwargs["default_db"] is not None and kwargs[
            "table_name"] is not None:

            meta_query = meta_query.filter(and_(cls.default_db == kwargs["default_db"],
                                                func.lower(cls.table_name) == str((kwargs["table_name"]).lower())))

        elif "default_db" in kwargs and kwargs["default_db"] is not None:
            logger.info("按数据库的特定库进行元数据提取 业务域:%s 物理库:%s db:%s" %
                        (domain, type, kwargs["db_name"]))
            meta_query = meta_query.filter(cls.default_db == kwargs["default_db"])

        if "is_extract" in kwargs and kwargs["is_extract"] is not None:
            meta_query = meta_query.filter(cls.is_extract == 1)

        meta_query = meta_query.group_by(cls.domain,
                                         cls.type,
                                         cls.default_db,
                                         cls.table_name)
        return meta_query.all()

    @classmethod
    def get_table_info(cls, domain, type, default_db, table_name, **kwargs):
        meta_query = cls.query.filter(and_(cls.domain == domain,
                                           cls.type == type,
                                           cls.default_db == default_db,
                                           cls.table_name == table_name
                                           ))

        if "is_extract" in kwargs and kwargs["is_extract"] is not None:
            meta_query = meta_query.filter(cls.is_extract == kwargs["is_extract"]).order_by(cls.column_position)

        if "is_extract_filter" in kwargs and kwargs["is_extract_filter"] is not None:
            meta_query = meta_query.filter(cls.is_extract_filter == kwargs["is_extract_filter"])

        if "table_primary" in kwargs and kwargs["table_primary"] is not None:
            meta_query = meta_query.filter(cls.is_primary > kwargs["table_primary"]).order_by(cls.is_primary)

        return meta_query.all()

    @classmethod
    def update_filter(cls, domain, type, default_db, table_name, column_name, filter_value):
        db.session.query(cls).filter(and_(cls.domain == domain,
                                          cls.type == type,
                                          cls.default_db == default_db,
                                          cls.table_name == table_name,
                                          cls.column_name == column_name,
                                          cls.is_extract_filter == 1
                                          )).update({"filter_default": filter_value})
        db.session.commit()

    @classmethod
    def modify(cls, column_id, input_data):

        update_column = {}

        try:
            if "column_comment" in input_data:
                update_column["column_comment"] = input_data["column_comment"]

            if "is_extract" in input_data and input_data["is_extract"] in [0, 1]:
                update_column["is_extract"] = input_data["is_extract"]

            if "is_primary" in input_data and input_data["is_primary"] in [0, 1]:
                update_column["is_primary"] = input_data["is_primary"]

            if "is_extract_filter" in input_data and input_data["is_extract_filter"] in [0, 1]:
                update_column["is_extract_filter"] = input_data["is_extract_filter"]

            if "filter_default" in input_data:
                update_column["filter_default"] = input_data["filter_default"]

            db.session.query(cls).filter(cls.id == column_id).update(update_column)
            db.session.commit()
        except Exception as e:
            return e

    @classmethod
    def delete_table_info(cls, domain, type, default_db, table_name):
        cls.query.filter(and_(cls.domain == domain,
                              cls.type == type,
                              cls.default_db == default_db,
                              func.lower(cls.table_name) == str(table_name).lower()
                              )).delete(synchronize_session="fetch")

        db.session.commit()


class DBObjectType(enum.Enum):
    mysql = "mysql"
    postgres = "postgresql"


class ExtractDataInfo(db.Model, InsertObject, TimestampMixin):
    __tablename__ = "extract_data_info"

    id = Column(db.Integer, primary_key=True, autoincrement=True)
    domain = Column(db.String(150))
    db_object_type = Column(db.String(150))
    db_name = Column(db.String(500))
    table_name = Column(db.String(500))
    table_primary_id = Column(db.String(150), nullable=True)
    table_extract_col = Column(db.String(150), nullable=True)
    latest_table_primary_id = Column(db.String(150), nullable=True)
    latest_extract_date = Column(db.DateTime(True), nullable=True)

    @classmethod
    def get_by_table_name(cls, domain, db_object_type, db_name, table_name):
        select_query = cls.query.filter(and_(cls.domain == domain,
                                             cls.db_object_type == db_object_type,
                                             cls.db_name == db_name,
                                             cls.table_name == table_name))
        return select_query.first()

    @classmethod
    def upsert(cls, table_data=None, **kwargs):

        if table_data is None:
            table_data = cls.get_by_table_name(domain=kwargs["domain"],
                                               db_object_type=kwargs["db_object_type"],
                                               db_name=kwargs["db_name"],
                                               table_name=kwargs["table_name"])
            if table_data:
                if "table_primary_id" in kwargs and table_data.table_primary_id != kwargs["table_primary_id"]:
                    table_data.table_primary_id = kwargs["table_primary_id"]

                if "table_extract_col" in kwargs and table_data.table_extract_col != kwargs["table_extract_col"]:
                    table_data.table_extract_col = kwargs["table_extract_col"]

                if "latest_table_primary_id" in kwargs and table_data.latest_table_primary_id != kwargs[
                    "latest_table_primary_id"]:
                    table_data.latest_table_primary_id = kwargs["latest_table_primary_id"]

                if "latest_extract_date" in kwargs and table_data.latest_extract_date != kwargs["latest_extract_date"]:
                    table_data.latest_extract_date = kwargs["latest_extract_date"]
            else:
                return cls.create(**kwargs)

        db.session.add(table_data)
        db.session.commit()
        return table_data


class FullTextIndex(db.Model, InsertObject, FullText):
    __tablename__ = "full_text_index"
    __fulltext_columns__ = ("row_content",)

    id = Column(db.String(300), primary_key=True)
    extract_data_info_id = Column(db.Integer, db.ForeignKey("extract_data_info.id"))
    extract_data_info = db.relationship(ExtractDataInfo, backref="fulltextindex_extractdatainfo")
    block_name = Column(db.String(500))
    block_key = Column(db.String(500))
    row_content = Column(db.Text())

    @classmethod
    def search_index(cls,
                     domain,
                     db_object_type,
                     search_text,
                     block_name=None,
                     block_key=None):

        search_query = cls.query.filter(
            and_(
                cls.extract_data_info.has(ExtractDataInfo.domain == domain),
                cls.extract_data_info.has(ExtractDataInfo.db_object_type == db_object_type),
                FullTextSearch(search_text, cls, FullTextMode.DEFAULT))
        )

        if block_name:
            search_query = search_query.filter(cls.block_name == block_name)

        if block_key:
            search_query = search_query.filter(cls.block_key == block_key)

        return search_query.all()

    @classmethod
    def delete_data(cls, extract_data_info_id=None, id_list=None):

        if extract_data_info_id:
            cls.query.filter(cls.extract_data_info_id == extract_data_info_id).delete()
            db.session.commit()
        elif id_list:
            cls.query.filter(cls.id.in_(id_list)).delete()
            db.session.commit()
