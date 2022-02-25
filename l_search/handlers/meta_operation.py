# -*- coding: UTF-8 -*-
"""
@time:2021/11/30
@author:zhangwei
@file:meta_operation
"""
from l_search.utils.logger import Logger
from l_search import models
from l_search import settings

logger = Logger()


class Meta:

    @classmethod
    def _del_none_from_dict(cls, input_data):
        for k in input_data.copy():
            if input_data[k] is None:
                del(input_data[k])

        return input_data

    @classmethod
    def upsert_connection_info(cls, input_data):

        if not isinstance(input_data, list):
            input_data = [input_data]

        need_upsert_data = []
        for d in input_data:
            need_upsert_data.append(cls._del_none_from_dict(input_data=d))

        upsert_data = models.DBConnect.upsert(input_data=input_data)

        return upsert_data

    @classmethod
    def get_connection_info(cls, connection_id=None, domain=None, default_db=None, db_type=None):

        def change_key_name(data):
            data["db_type"] = models.DBObjectType(data["db_type"]).name
            return data

        if db_type:
            db_type = models.DBObjectType[db_type].value

        connection_info = models.DBConnect.get_by_domain(domain=domain,
                                                         db_type=db_type,
                                                         default_db=default_db,
                                                         connection_id=connection_id)
        connection_info = models.convert_to_dict(connection_info)
        connection_info = [change_key_name(c_data) for c_data in connection_info]

        return connection_info

    @classmethod
    def get_table_meta(cls, connection_id, table_name=None):
        """
        获取多个或单个表的列信息
        :param connection_id:
        :param table_name: 筛选表 单个或多个(a|b|c)
        :return:
        """

        get_info = []

        table_name_list = models.TableInfo.get_tables(connection_id=connection_id,
                                                      table_name=table_name)

        for table_info in table_name_list:
            table_detail = models.TableDetail.get_table_info(table_info=table_info)

            get_info.append({"table_name": table_detail.table_name,
                             "data": table_detail})

        return get_info

    @classmethod
    def modify_column_info(cls, connection_id, table_name, input_data):

        if isinstance(input_data, dict):
            input_data = [input_data]

        tables_info_list = cls.get_table_meta(connection_id=connection_id,
                                              table_name=table_name)
        change = 0
        failed_info = ""

        for row in input_data:
            if any(d["data"].get('id', row["id"]) == row["id"] for d in tables_info_list):
                modify_result = models.TableDetail.modify(column_id=row["id"], input_data=row)
                change += 1

                if modify_result is not None:
                    failed_info = modify_result
        return {"change_success": change,
                "change_info": failed_info}

    @classmethod
    def add_table_info(cls, connection_info, input_meta):
        """
        一个表的列信息
        :param input_meta:{columns:[{
                            "column_name":"",
                            "column_type":"",
                            "column_type_length":"",
                            "column_comment":"",
                            "column_position":"",
                            }],
                            table_name:xxx
                            table_primary_id:xx
                            table_primary_id_is_int:Ture,
                            table_extract_col:xxx
        """
        table_detail_insert_data = input_meta.pop("columns")

        input_meta["connection_id"] = connection_info.id
        table_info = models.TableInfo.upsert(input_data=input_meta)

        required_keys = ["column_name",
                         "column_type",
                         "column_type_length",
                         "column_comment",
                         "column_position"]

        input_meta_keys = table_detail_insert_data[0].keys()

        check_keys = list(set(required_keys) - set(input_meta_keys))

        insert_row_count = 0
        # 确保key都存在
        if len(check_keys) == 0:

            get_table_info = models.TableDetail.get_table_info(table_info=table_info)
            check = False

            if get_table_info:

                get_table_info_list = [u.__dict__ for u in get_table_info]
                table_detail_insert_data_check = table_detail_insert_data.copy()

                for i in range(len(get_table_info_list)):
                    if get_table_info_list[i]["column_name"] == table_detail_insert_data_check[i]["column_name"]:
                        if get_table_info_list[i]["column_position"] == table_detail_insert_data_check[i]["column_position"]:
                            table_detail_insert_data.remove(table_detail_insert_data_check[i])
                        else:
                            failed_info = "同步后（%s）的列信息顺序与已记录的表信息条数，无法更新" % input_meta["table_name"]
                            logger.debug(failed_info)
                            return failed_info

            if len(table_detail_insert_data) > 0:
                insert_row_count = models.TableDetail.bulk_insert(input_data=table_detail_insert_data)

        return insert_row_count
