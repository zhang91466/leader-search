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
    def upsert_connection_info(cls, input_data):

        connection_id = None

        if "connection_id" in input_data:
            connection_id = input_data["connection_id"]

        if connection_id:

            for k in input_data.copy():
                if input_data[k] is None:
                    input_data.pop(k)

            models.DBConnect.modify(connection_id=connection_id, **input_data)
            add_result_info = ""
        else:

            infos = {"domain": input_data["domain"],
                     "type": models.DBObjectType[input_data["db_object_type"]].value,
                     "host": input_data["host"],
                     "port": input_data["port"],
                     "account": input_data["account"],
                     "pwd": input_data["pwd"],
                     "default_db": input_data["default_db"]}
            connection_id, add_result_info = models.DBConnect.add_row(infos=infos)

        return {"connection_id": connection_id,
                "add_result_info": add_result_info}

    @classmethod
    def get_connection_info(cls, connection_id=None, domain=None, db_name=None, db_type=None):

        def change_key_name(data):
            data["connection_id"] = data.pop("id")
            data["db_type"] = models.DBObjectType(data["db_object_type"]).name
            return data

        if db_type:
            db_type = models.DBObjectType[db_type].value

        connection_info = models.DBConnect.get_by_domain(domain=domain,
                                                         db_type=db_type,
                                                         default_db=db_name,
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
            if any(d["data"].get('id', row["id"]) == 'red' for d in tables_info_list):
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
        :param input_meta:{table_name:[{"db_name":"",
                            "table_name":"",
                            "column_name":"",
                            "column_type":"",
                            "column_type_length":"",
                            "column_comment":"",
                            "column_position":"",
                            }],
                            table_primary_id:xx
                            table_primary_id_is_int:Ture,
                            table_extract_col:xxx
        """
        table_info = models.TableInfo.upsert(connection_info=connection_info,
                                             table_name=input_meta["table_name"],
                                             table_primary_id=input_meta["table_primary_id"],
                                             table_primary_id_is_int=input_meta["table_primary_id_is_int"],
                                             table_extract_col=input_meta["table_extract_col"])

        required_keys = ["column_name",
                         "column_type",
                         "column_type_length",
                         "column_comment",
                         "column_position"]

        input_meta_keys = input_meta["data"][0].keys()

        check_keys = list(set(required_keys) - set(input_meta_keys))

        update_num = 0
        # 确保key都存在
        if len(check_keys) == 0:

            get_table_info = models.TableDetail.get_table_info(table_info=table_info)
            check = False
            if len(get_table_info) > len(input_meta):
                failed_info = "同步表信息（%s）数据库中记录的列多于更新的列，无法更新" % input_meta[0]["table_name"]
                logger.info(failed_info)
                return failed_info
            elif len(get_table_info) < len(input_meta) and len(get_table_info) != 0:
                input_meta = input_meta[len(get_table_info) - 1:]
                check = True
            elif len(get_table_info) == len(input_meta):
                input_meta = []

            for items in input_meta:

                get_column_info = None
                if check:
                    get_column_info = models.TableDetail.get_table_info(domain=cls.domain,
                                                                        type=cls.db_object_type,
                                                                        default_db=items["default_db"],
                                                                        table_name=items["table_name"],
                                                                        column_name=items["column_name"])

                if get_column_info is None:
                    models.TableDetail.create(**items)
                    update_num = update_num + 1

        return str(update_num)
