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
    domain = ""
    db_object_type = ""
    db_name = ""

    @classmethod
    def add_connection_info(cls, input_data):

        connection_id = None

        if "connection_id" in input_data:
            connection_id = input_data["connection_id"]

        if connection_id:

            for k in input_data.copy():
                if input_data[k] is None:
                    input_data.pop(k)

            models.DBConnect.modify(new_data=input_data, connection_id=connection_id)
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
    def get_connection_info(cls, connection_id=None, domain=None, db_name=None, db_object_type=None):

        def change_key_name(data):
            data["connection_id"] = data.pop("id")
            data["db_object_type"] = data.pop("type")

            data["db_object_type"] = models.DBObjectType(data["db_object_type"]).name
            return data

        if db_object_type:
            db_object_type = models.DBObjectType[db_object_type].value

        if domain is None:
            domain = cls.domain

        if db_object_type is None:
            db_object_type = cls.db_object_type

        if db_name is None:
            db_name = cls.db_name

        connection_info = models.DBConnect.get_by_domain(domain=domain,
                                                         type=db_object_type,
                                                         default_db=db_name,
                                                         connection_id=connection_id)
        connection_info = models.convert_to_dict(connection_info)
        if len(connection_info) == 1:
            connection_info = change_key_name(connection_info[0])
        else:
            connection_info = [change_key_name(c_data) for c_data in connection_info]

        return connection_info

    @classmethod
    def get_table_meta(cls, default_db, table_name=None):

        def get_with_table_name(t_name):
            data = models.DBMetadata.get_table_info(domain=cls.domain,
                                                    type=models.DBObjectType[cls.db_object_type].value,
                                                    default_db=default_db,
                                                    table_name=t_name,
                                                    is_extract=None)

            return {"table_name": t_name,
                    "data": models.convert_to_dict(data)}

        if table_name:
            get_info = get_with_table_name(t_name=table_name)

        else:
            tables_in_db = models.DBMetadata.get_tables(domain=cls.domain,
                                                        type=models.DBObjectType[cls.db_object_type].value,
                                                        db_name=default_db)

            get_info = []

            for table in tables_in_db:
                get_info.append(get_with_table_name(t_name=table.table_name))

        return get_info

    @classmethod
    def modify_column_info(cls, default_db, table_name, input_data):

        if isinstance(input_data, dict):
            input_data = [input_data]

        table_info = models.DBMetadata.get_table_info(domain=cls.domain,
                                                      type=models.DBObjectType[cls.db_object_type].value,
                                                      default_db=default_db,
                                                      table_name=table_name,
                                                      is_extract=None)

        column_id_list = [col.id for col in table_info]

        change = 0
        failed_info = ""
        for row in input_data:
            if row["id"] in column_id_list:
                modify_result = models.DBMetadata.modify(column_id=row["id"], input_data=row)
                change += 1

                if modify_result is not None:
                    failed_info = modify_result
        return {"change_success": change,
                "change_info": failed_info}

    @classmethod
    def add_table_info(cls, input_meta):
        """
        一个表的列信息
        :param input_meta:[{"db_name":"",
                            "table_name":"",
                            "column_name":"",
                            "column_type":"",
                            "column_type_length":"",
                            "column_comment":"",
                            "column_position":"",
                            }]
        """
        required_keys = ["default_db",
                         "table_name",
                         "column_name",
                         "column_type",
                         "column_type_length",
                         "column_comment",
                         "column_position"]

        input_meta_keys = input_meta[0].keys()

        check_keys = list(set(required_keys) - set(input_meta_keys))

        update_num = 0
        # 确保key都存在
        if len(check_keys) == 0:

            get_table_info = models.DBMetadata.get_table_info(domain=cls.domain,
                                                              type=cls.db_object_type,
                                                              default_db=input_meta[0]["default_db"],
                                                              table_name=input_meta[0]["table_name"])
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

                update_dict = {
                    "domain": cls.domain,
                    "type": cls.db_object_type,
                }

                if "is_extract" not in items:
                    update_dict["is_extract"] = 0

                if "is_primary" not in items:
                    update_dict["is_primary"] = 0

                if "is_extract_filter" not in items:
                    update_dict["is_extract_filter"] = 0

                if "filter_default" not in items:
                    update_dict["filter_default"] = ""

                items.update(update_dict)

                get_column_info = None
                if check:
                    get_column_info = models.DBMetadata.get_table_info(domain=cls.domain,
                                                                       type=cls.db_object_type,
                                                                       default_db=items["default_db"],
                                                                       table_name=items["table_name"],
                                                                       column_name=items["column_name"])

                if get_column_info is None:
                    models.DBMetadata.create(**items)
                    update_num = update_num + 1

        return str(update_num)
