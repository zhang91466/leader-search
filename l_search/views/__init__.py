# -*- coding: UTF-8 -*-
"""
@time:2021/11/29
@author:zhangwei
@file:__init__.py
"""
from flask_restx import Api
from l_search import __title__, __version__
from .search import (api_search,
                     QueryIndex,
                     GroupIndex)
from .mirror_data import (api_mirror,
                          ExtractToFullTextIndexTable,
                          ExtractToEntityInit,
                          ExtractToEntityUpsert,
                          ExtractToEntityDataGet,
                          ExtractToEntityDrop
                          )
from .data_meta import (api_meta,
                        ConnectionInfo,
                        ConnectionInfoCreate,
                        SyncMeta,
                        MetaInfo)
from .tasks_info import (api_task,
                         TaskStatus)

api = Api(title=__title__,
          version=__version__, )

api.add_namespace(api_search, path="/search")
api.add_namespace(api_mirror, path="/mirror")
api.add_namespace(api_meta, path="/meta")
api.add_namespace(api_task, path="/task")

api_task.add_resource(TaskStatus, "/<task_id>", endpoint="task_status")

api_search.add_resource(QueryIndex,
                        "/<domain>",
                        "/<domain>/<db_object_type>",
                        "/<domain>/<db_object_type>/<db_name>",
                        endpoint="query_index")

api_search.add_resource(GroupIndex,
                        "/preview/<domain>",
                        "/preview/<domain>/<db_object_type>",
                        "/preview/<domain>/<db_object_type>/<db_name>",
                        endpoint="group_index")

api_mirror.add_resource(ExtractToFullTextIndexTable,
                        "/data_to_full_index/<domain>/<db_object_type>/<db_name>/<table_name>",
                        endpoint="extract_to_full_text_index_table")

api_mirror.add_resource(ExtractToEntityInit,
                        "/data_to_table/<domain>/<db_object_type>/<db_name>/<table_name>/init",
                        endpoint="extract_to_entity_init")

api_mirror.add_resource(ExtractToEntityUpsert,
                        "/data_to_table/<domain>/<db_object_type>/<db_name>/<table_name>/upsert",
                        endpoint="extract_to_entity_upsert")

api_mirror.add_resource(ExtractToEntityDataGet,
                        "/data_to_table/<domain>/<db_object_type>/<db_name>/<table_name>/get",
                        endpoint="extract_to_entity_get")

api_mirror.add_resource(ExtractToEntityDrop,
                        "/data_to_table/<domain>/<db_object_type>/<db_name>/<table_name>",
                        endpoint="extract_to_entity_drop")

api_meta.add_resource(ConnectionInfo,
                      "/connections",
                      "/connections/<domain>",
                      "/connection/<domain>/<db_object_type>",
                      "/connection/<domain>/<db_object_type>/<db_name>",
                      "/connection/<int:connection_id>",
                      endpoint="connection_info")

api_meta.add_resource(ConnectionInfoCreate,
                      "/connection/create",
                      endpoint="connection_info_create")

api_meta.add_resource(SyncMeta, "/sync", endpoint="sync_meta")

api_meta.add_resource(MetaInfo,
                      "/table/<domain>/<db_object_type>/<db_name>",
                      endpoint="meta_info_get_whole_db_tables",
                      methods=["GET"])

api_meta.add_resource(MetaInfo,
                      "/table/<domain>/<db_object_type>/<db_name>/<table_name>",
                      endpoint="meta_info")
