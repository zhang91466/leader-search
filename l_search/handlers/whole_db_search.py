# -*- coding: UTF-8 -*-
"""
@time:12/13/2021
@author:
@file:whole_db_search
"""
from l_search.utils.logger import Logger
from l_search import models
from l_search.handlers.source_meta_operate.handle.meta_handle import MetaDetector
import hashlib
import pandas as pd

logger = Logger()


class WholeDbSearch:
    domain = ""
    db_object_type = ""
    db_name = ""

    @classmethod
    def get_full_text_index_id_prefix(cls, table_name):
        return hashlib.md5(
            str("%s-%s-%s-%s" % (cls.domain, cls.db_object_type, cls.db_name, table_name)).encode("utf-8")).hexdigest()

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
        :return: dict {select:select column sql, from: from table sql, latest_date: 获取最终抽取时间与最大主键id}
        """
        string_column_type = ["varchar", "string", "text", "char"]

        table_schema = models.DBMetadata.get_table_info(domain=cls.domain,
                                                        type=models.DBObjectType[cls.db_object_type].value,
                                                        default_db=cls.db_name,
                                                        table_name=table_name,
                                                        is_extract=None
                                                        )

        select_case_str = ""
        concat_str = ""
        primary_col_type_is_int = True
        for col in table_schema:

            if col.is_primary == 1:
                primary_column_name = col.column_name

                if col.column_type in string_column_type:
                    primary_col_type_is_int = False
                continue

            if col.is_extract_filter == 1:
                extract_column_name = col.column_name

            if col.column_type.lower() in string_column_type \
                    and "geo" not in col.column_name \
                    and col.is_extract == 1:
                select_case_str = select_case_str + """
                ,case when %(column_name)s is null then '*' else concat(%(column_name)s, '*') end as %(column_name)s """ % {
                    "column_name": col.column_name}

                concat_str = concat_str + "%s, " % col.column_name

        concat_str = concat_str.strip()[:-1]

        if primary_column_name and extract_column_name and len(concat_str) > 1:
            logger.debug("(%s.%s.%s) 表主键和抽取列都明确，生成抽取sql" % (cls.domain, cls.db_name, table_name))

            sql_select = """
            select 
            concat('%(id_tag)s','-',%(table_primary_id)s) as id
            ,concat(%(row_content)s) as row_content

            """ % {
                "id_tag": cls.get_full_text_index_id_prefix(table_name=table_name),
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

            if primary_col_type_is_int:
                get_latest_date_sql = """
                                select 
                                max(%(table_primary_id)s) as latest_table_primary_id
                                ,max(%(table_extract_col)s) as latest_extract_date
                                from %(table_name)s""" % {"table_primary_id": primary_column_name,
                                                          "table_extract_col": extract_column_name,
                                                          "table_name": table_name}
            else:
                get_latest_date_sql = """
                                select 
                                0 as latest_table_primary_id
                                ,max(%(table_extract_col)s) as latest_extract_date
                                from %(table_name)s""" % {"table_extract_col": extract_column_name,
                                                          "table_name": table_name}

            return {"select": sql_select,
                    "from": sql_from,
                    "latest_date": get_latest_date_sql,
                    "primary_col_type_is_int": primary_col_type_is_int,
                    "table_primary_id": primary_column_name,
                    "table_extract_col": extract_column_name
                    }

        else:
            if primary_column_name is None:
                failed_info = "(%s.%s.%s) 表主键不明确，无法生成抽取sql" % (cls.domain, cls.db_name, table_name)
                logger.info(failed_info)

            if extract_column_name is None:
                failed_info = "(%s.%s.%s) 表抽取列不明确，无法生成抽取sql" % (cls.domain, cls.db_name, table_name)
                logger.info(failed_info)

            return failed_info

    @classmethod
    def extract_data(cls,
                     execute_sql,
                     extract_data_info_id,
                     block_name="",
                     block_key=""):
        """
        将数据从目标库中导出
        :param execute_sql: 来自create_extract_table_sql_to_full_index的返回值
        :param block_name: 该数据对应的业务域
        :param block_key: 该数据在业务域中的关键词
        :param extract_data_info_id: ExtractDataInfo的id
        :return: 抽取的数据
        """

        meta_detector = MetaDetector(domain=cls.domain,
                                     type=models.DBObjectType[cls.db_object_type].value)

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

        need_insert_data = meta_detector.execute_select_sql(sql_text=sql_text)

        get_last_date = meta_detector.execute_select_sql(sql_text=execute_sql["latest_date"])

        return need_insert_data, get_last_date[0]

    @classmethod
    def store_data(cls, insert_data, get_last_date, extract_data_info):
        """
        将数据导入全局表中
        :param insert_data:
        :param get_last_date:
        :param extract_data_info:
        :return:
        """
        insert_success_row_count = models.FullTextIndex.bulk_insert(input_data=insert_data)

        extract_data_info.latest_table_primary_id = get_last_date["latest_table_primary_id"]
        extract_data_info.latest_extract_date = get_last_date["latest_extract_date"]
        models.ExtractDataInfo.upsert(table_data=extract_data_info)
        return insert_success_row_count

    @classmethod
    def extract_and_store(cls,
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

        where_stat = ""
        # 增量抽取
        if is_full == 0 and extract_data_info is not None:
            where_stat = """
                        where %(latest_extract_date)s > '%(latest_extract_date_value)s'
                         """ % {"latest_extract_date": extract_data_info.table_extract_col,
                                "latest_extract_date_value": extract_data_info.latest_extract_date}

        extract_sql = cls.create_extract_table_sql_to_full_index(table_name=table_name,
                                                                 where_stat=where_stat)

        # 如果是string说明生成sql的时候存在问题故返回错误信息
        if isinstance(extract_sql, str):
            return extract_sql

        if extract_data_info is None:
            extract_data_info = models.ExtractDataInfo.create(domain=cls.domain,
                                                              db_object_type=cls.db_object_type,
                                                              db_name=cls.db_name,
                                                              table_name=table_name,
                                                              table_primary_id=extract_sql["table_primary_id"],
                                                              table_extract_col=extract_sql["table_extract_col"]
                                                              )

        extract_data, get_last_date_data = cls.extract_data(execute_sql=extract_sql,
                                                            extract_data_info_id=extract_data_info.id,
                                                            block_name=block_name,
                                                            block_key=block_key)

        # 处理已经存在的数据
        # 全量
        if is_full == 1:
            models.FullTextIndex.delete_data(extract_data_info_id=extract_data_info.id)
        # 增量
        else:
            # merge过程

            # 把抽取出来的数据转为df
            extract_data_df = pd.DataFrame(extract_data)
            extract_data_df["id_prefix"] = cls.get_full_text_index_id_prefix(table_name=table_name)

            extract_data_df[["id_prefix", "full_text_index_id"]] = extract_data_df["id"].str.split('-', 1, expand=True)

            if extract_sql["primary_col_type_is_int"]:
                extract_data_df_less_max_p_id_df = extract_data_df[
                    extract_data_df["full_text_index_id"].astype(int) < int(extract_data_info.latest_table_primary_id)]
                extract_data_df_less_max_p_id_list = extract_data_df_less_max_p_id_df["id"].to_list()
            else:
                extract_data_df_less_max_p_id_list = extract_data_df["id"].to_list()

            # 删除已经确定的id
            models.FullTextIndex.delete_data(id_list=extract_data_df_less_max_p_id_list)

        # 插入数据
        insert_row_count = cls.store_data(insert_data=extract_data,
                                          get_last_date=get_last_date_data,
                                          extract_data_info=extract_data_info)
        return insert_row_count

    @classmethod
    def search(cls, search_text, block_name=None, block_key=None):
        search_data = models.FullTextIndex.search_index(domain=cls.domain,
                                                        db_object_type=cls.db_object_type,
                                                        search_text=search_text,
                                                        block_name=block_name,
                                                        block_key=block_key)
        search_data_dict_list = []
        search_text_list = str(search_text).replace("+", "").replace("-", "").split(" ")
        for row in search_data:
            row_dict = {"db_name": row.extract_data_info.db_name,
                        "table_name": row.extract_data_info.table_name,
                        "row_id": str(row.id).split("-")[1],
                        "block_name": row.block_name,
                        "block_key": row.block_key}

            hits = []
            for hit in str(row.row_content).split("*"):
                if any(s in hit for s in search_text_list):
                    hits.append(hit)
            row_dict["hits"] = hits
            search_data_dict_list.append(row_dict)

        return search_data_dict_list
