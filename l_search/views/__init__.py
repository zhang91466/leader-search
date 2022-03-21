# -*- coding: UTF-8 -*-
"""
@time:2021/11/29
@author:zhangwei
@file:__init__.py
"""
from flask_restx import Api
from l_search import __title__, __version__
from .mirror_data import (api_mirror,
                          ExtractAndLoad
                          )
from .data_meta import (api_meta,
                        ConnectionInfo,
                        ConnectionInfoUpsert,
                        SyncMeta,
                        TableInfo,
                        TableDetail)
from .tasks_info import (api_task,
                         TaskStatus)

api = Api(title=__title__,
          version=__version__, )

api.add_namespace(api_mirror, path="/entity")
api.add_namespace(api_meta, path="/meta")
api.add_namespace(api_task, path="/task")

api_task.add_resource(TaskStatus, "/<task_id>", endpoint="task_status")


api_mirror.add_resource(ExtractAndLoad,
                        "/etl/<int:connection_id>/<table_info_id>",
                        endpoint="extract_and_load")

api_meta.add_resource(ConnectionInfo,
                      "/connections",
                      "/connections/<domain>",
                      "/connection/<domain>/<db_type>",
                      "/connection/<domain>/<db_type>/<db_name>",
                      "/connection/<int:connection_id>",
                      endpoint="connection_info")

api_meta.add_resource(ConnectionInfoUpsert,
                      "/connection/upsert",
                      endpoint="connection_info_upsert")

api_meta.add_resource(SyncMeta, "/sync", endpoint="sync_meta")

api_meta.add_resource(TableInfo,
                      "/<int:connection_id>/tables/info",
                      "/<int:connection_id>/<table_name>/info",
                      endpoint="get_tables",
                      methods=["GET"])

api_meta.add_resource(TableInfo,
                      "/table/info/upsert",
                      endpoint="table_info_upsert",
                      methods=["POST"])

api_meta.add_resource(TableDetail,
                      "/<table_info_id>/detail",
                      endpoint="get_table_detail",
                      methods=["GET"])

api_meta.add_resource(TableDetail,
                      "/table/detail/upsert",
                      endpoint="table_detail_upsert",
                      methods=["POST"])
