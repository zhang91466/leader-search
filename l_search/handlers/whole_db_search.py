# -*- coding: UTF-8 -*-
"""
@time:12/13/2021
@author:
@file:whole_db_search
"""
from l_search.utils.logger import Logger
from l_search import models
from DWMM.operate.metadata_info import MetadataOperate
from DWMM.source_meta_operate.handle.meta_handle import MetaDetector
import hashlib

logger = Logger()


class WholeDbSearch:
    domain = ""
    db_object_type = ""
    db_name = ""

    @classmethod
    def create_extract_table_sql_to_full_index(cls,
                                               table_name,
                                               primary_column_name=None,
                                               extract_column_name=None,
                                               where_stat=""):
        """
        为了将数据导入全局检索表中，根据元数据管理系统中已经记录的表结构，组织数据抽取sql
        :param table_name: 抽取表表名
        :param primary_column_name: 抽取表主键
        :param extract_column_name: 抽取表中的时间字段，通过该字段进行增量的数据抽取
        :param where_stat: 抽取where条件
        :return: dict {select:select column sql, from: from table sql}
        """
        operate = MetadataOperate(subject_domain=cls.domain, object_type=models.DBObjectType[cls.db_object_type].value)

        table_schema = operate.get_table_info(db_name=cls.db_name,
                                              table_name=table_name,
                                              is_extract=None
                                              )
        select_case_str = ""
        concat_str = ""
        for col in table_schema:

            if col["is_primary"] == 1:
                primary_column_name = col["column_name"]
                continue

            if col["is_extract_filter"] == 1:
                extract_column_name = col["column_name"]

            if col["column_type"].lower() in ["varchar", "string", "text", "char"] \
                    and "geo" not in col["column_name"] \
                    and col["is_extract"] == 1:
                select_case_str = select_case_str + """
                ,case when %(column_name)s is null then '*' else concat(%(column_name)s, '*') end as %(column_name)s """ % {
                    "column_name": col["column_name"]}

                concat_str = concat_str + "%s, " % col["column_name"]

        concat_str = concat_str.strip()[:-1]

        if primary_column_name and extract_column_name and len(concat_str) > 1:
            logger.debug("(%s.%s.%s) 表主键和抽取列都明确，生成抽取sql" % (cls.domain, cls.db_name, table_name))

            sql_select = """
            select 
            concat('%(id_tag)s','-',%(table_primary_id)s) as id
            ,concat(%(row_content)s) as row_content

            """ % {
                "id_tag": hashlib.md5(
                    str("%s-%s-%s-%s" % (cls.domain, cls.db_object_type, cls.db_name, table_name)).encode(
                        "utf-8")).hexdigest(),
                "table_primary_id": primary_column_name,
                "row_content": concat_str
            }

            sql_from = """
            from (
                select
                %(table_primary_id)s
                %(case_col)s
                from %(table_name)s
                %(where_stat)s
            ) t1
            """ % {"table_primary_id": primary_column_name,
                   "case_col": select_case_str,
                   "table_name": table_name,
                   "where_stat": where_stat}

            return {"select": sql_select,
                    "from": sql_from}

        else:
            if primary_column_name is None:
                logger.info("(%s.%s.%s) 表主键不明确，无法生成抽取sql" % (cls.domain, cls.db_name, table_name))

            if extract_column_name is None:
                logger.info("(%s.%s.%s) 表抽取列不明确，无法生成抽取sql" % (cls.domain, cls.db_name, table_name))

            return None

    @classmethod
    def get_latest_data_tag_sql(cls, table_name, primary_column_name, extract_column_name):
        sql_stat = """
                    select 
                    max(%(table_primary_id)s) as latest_table_primary_id
                    ,max(%(table_extract_col)s) as latest_extract_date
                    from %(table_name)s)""" % {"table_primary_id": primary_column_name,
                                               "table_extract_col": extract_column_name,
                                               "table_name": table_name}
        return sql_stat

    @classmethod
    def extract_data(cls,
                     execute_sql,
                     extract_data_info_id,
                     block_name="",
                     block_key=""):
        """
        将数据导入全局检索表中
        :param execute_sql: 抽取的sql
        :param block_name: 该数据对应的业务域
        :param block_key: 该数据在业务域中的关键词
        :param extract_data_info_id: ExtractDataInfo的id
        :return: 抽取的数据
        """

        meta_detector = MetaDetector(subject_domain=cls.domain,
                                     object_type=models.DBObjectType[cls.db_object_type].value)

        sql_text = """
        %(select)s
        ,%(extract_data_info_id)s as extract_data_info_id
        ,'%(block_name)s' as block_name
        ,'%(block_key)s' as block_key
        %(from)s
        """ % {"select": execute_sql["select"],
               "extract_data_info_id": extract_data_info_id,
               "block_name": block_name,
               "block_key": block_key,
               "from": execute_sql["from"]}

        logger.debug("抽取sql: %s" % sql_text)

        execute_data = meta_detector.execute_select_sql(sql_text=sql_text)

        return execute_data

    @classmethod
    def store_data(cls,
                   table_name,
                   block_name="",
                   block_key="",
                   is_full=0):
        """
        如果是全量
            删除该表的所有信息
            抽取数据--全量
            存储数据
        如果是增量
            按上次抽取的时间往后进行数据抽取
            抽取出来的数据
                若主键id是数字
                    则把小于已经记录的最大id的id记录下来
                    删除记录下来的id
                若主键id是字符串
                    则把这次抽取出来的数据id都记录下来
                    删除记录下来的id
        存储数据
        :param table_name: 抽取表表名
        :param block_name: 该数据对应的业务域
        :param block_key: 该数据在业务域中的关键词
        :param is_full: 是否全量抽取数据
        """
        extract_data_info = models.ExtractDataInfo.get_by_table_name(domain=cls.domain,
                                                                     db_object_type=cls.db_object_type,
                                                                     db_name=cls.db_name,
                                                                     table_name=table_name)

        # 全量
        if is_full == 1:
            extract_sql = cls.create_extract_table_sql_to_full_index(table_name=table_name)
            extract_data = cls.extract_data(execute_sql=extract_sql,
                                            extract_data_info_id=extract_data_info.id,
                                            block_name=block_name,
                                            block_key=block_key)
            models.FullTextIndex.delete_data(extract_data_info_id=extract_data_info.id)
        # 增量
        else:
            where_stat = """
                        where %(latest_extract_date)s > %(latest_extract_date_value)s
                         """ % {"latest_extract_date": extract_data_info.table_extract_col,
                                "latest_extract_date_value": extract_data_info.latest_extract_date}
            extract_sql = cls.create_extract_table_sql_to_full_index(table_name=table_name,
                                                                     where_stat=where_stat)
            extract_data = cls.extract_data(execute_sql=extract_sql,
                                            extract_data_info_id=extract_data_info.id,
                                            block_name=block_name,
                                            block_key=block_key)
