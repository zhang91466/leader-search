# -*- coding: UTF-8 -*-
"""
@time:2021/11/29
@author:zhangwei
@file:__init__.py
"""
from flask_restx import Api
from l_search import __title__, __version__
from .search import api_search, QueryIndex
from .mirror_data import (api_mirror,
                          ExtractToFullTextIndexTable,
                          )
from .data_meta import (api_meta,
                        ConnectionInfo,
                        ConnectionInfoCreate,
                        SyncMeta,
                        MetaInfo)

api = Api(title=__title__,
          version=__version__, )

api.add_namespace(api_search, path="/search")
api.add_namespace(api_mirror, path="/mirror")
api.add_namespace(api_meta, path="/meta")

api_search.add_resource(QueryIndex, "/index", endpoint="query_index")

api_mirror.add_resource(ExtractToFullTextIndexTable, "/data_to_full_index", endpoint="mirror_data")

api_meta.add_resource(ConnectionInfo,
                      "/connections",
                      "/connections/<domain>",
                      "/connection/<domain>/<db_object_type>",
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

# Todo
# 要设计一个通用的表结构
# 要用一个强规则来应对业务库的更新
#
# 要有一个采集策略的逻辑
# 要有一个定时采集的摸快
# 要开发喂数据进来的接口
